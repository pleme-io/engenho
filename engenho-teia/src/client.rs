//! `TeiaClient` — wraps `async_nats::Client` with engenho's typed
//! subject construction + JSON payload encoding.
//!
//! F1 ships connect + publish + subscribe + request-reply. F2-F5
//! layer on this with concrete primitives (RaftTransport,
//! WatchPub/Sub, ContentStore, AttestationPub).

use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, info};

use crate::config::TeiaConfig;
use crate::error::TeiaError;
use crate::subject::ClusterScope;

/// The teia client. Owns an `async_nats::Client` + the
/// [`ClusterScope`] subject builder.
#[derive(Clone)]
pub struct TeiaClient {
    nats: async_nats::Client,
    scope: ClusterScope,
}

impl std::fmt::Debug for TeiaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TeiaClient")
            .field("scope", &self.scope)
            .field("nats_state", &"<connected>")
            .finish()
    }
}

impl TeiaClient {
    /// Connect to NATS using `config`. Validates the config first;
    /// returns a typed error on connection failure.
    ///
    /// # Errors
    ///
    /// - [`TeiaError::InvalidConfig`] if config validation fails
    /// - [`TeiaError::ConnectFailed`] if all server URLs fail to connect
    pub async fn connect(config: TeiaConfig) -> Result<Self, TeiaError> {
        config.validate()?;
        debug!(
            servers = ?config.servers,
            cluster = %config.cluster,
            "teia connecting to NATS"
        );

        let mut builder = async_nats::ConnectOptions::new()
            .connection_timeout(config.server_timeout)
            .require_tls(false);
        if let Some(name) = config.client_name {
            builder = builder.name(name);
        }
        if let Some(creds_path) = &config.credentials_path {
            builder = builder
                .credentials_file(creds_path)
                .await
                .map_err(|e| TeiaError::InvalidConfig(format!("credentials_file: {e}")))?;
        }

        let nats = tokio::time::timeout(
            config.connect_timeout,
            builder.connect(config.servers.join(",")),
        )
        .await
        .map_err(|_| {
            TeiaError::ConnectFailed(format!(
                "timed out after {:?}",
                config.connect_timeout
            ))
        })?
        .map_err(|e| TeiaError::ConnectFailed(e.to_string()))?;

        info!(cluster = %config.cluster, "teia connected");

        Ok(Self {
            nats,
            scope: ClusterScope::new(config.cluster),
        })
    }

    /// Reference to the typed subject builder.
    #[must_use]
    pub fn scope(&self) -> &ClusterScope {
        &self.scope
    }

    /// Underlying NATS client for advanced use cases (JetStream,
    /// Object Store, KV) at F3-F5.
    #[must_use]
    pub fn nats(&self) -> &async_nats::Client {
        &self.nats
    }

    /// Publish a JSON-encoded payload to `subject`.
    ///
    /// # Errors
    ///
    /// - [`TeiaError::Encode`] if serde_json fails
    /// - [`TeiaError::PublishFailed`] if NATS publish fails
    pub async fn publish_json<T: Serialize>(
        &self,
        subject: String,
        payload: &T,
    ) -> Result<(), TeiaError> {
        let bytes = serde_json::to_vec(payload)
            .map_err(|e| TeiaError::Encode(e.to_string()))?;
        self.nats
            .publish(subject.clone(), bytes.into())
            .await
            .map_err(|e| TeiaError::PublishFailed {
                subject,
                detail: e.to_string(),
            })?;
        Ok(())
    }

    /// Subscribe to a subject pattern (supports `*` and `>` wildcards).
    /// Returns the typed [`Subscription`].
    ///
    /// # Errors
    ///
    /// [`TeiaError::SubscribeFailed`] if NATS subscribe fails.
    pub async fn subscribe(&self, subject: String) -> Result<Subscription, TeiaError> {
        let sub = self
            .nats
            .subscribe(subject.clone())
            .await
            .map_err(|e| TeiaError::SubscribeFailed {
                subject: subject.clone(),
                detail: e.to_string(),
            })?;
        Ok(Subscription { inner: sub })
    }

    /// Request-reply: publish to `subject` + await response within
    /// `timeout`. Used by Raft RPC at F2.
    ///
    /// # Errors
    ///
    /// - [`TeiaError::Encode`] / [`TeiaError::Decode`] for payload errors
    /// - [`TeiaError::RequestFailed`] on NATS error or timeout
    pub async fn request_json<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        subject: String,
        payload: &Req,
        timeout: Duration,
    ) -> Result<Resp, TeiaError> {
        let bytes = serde_json::to_vec(payload)
            .map_err(|e| TeiaError::Encode(e.to_string()))?;
        let reply = tokio::time::timeout(
            timeout,
            self.nats.request(subject.clone(), bytes.into()),
        )
        .await
        .map_err(|_| TeiaError::RequestFailed {
            subject: subject.clone(),
            detail: format!("timed out after {timeout:?}"),
        })?
        .map_err(|e| TeiaError::RequestFailed {
            subject: subject.clone(),
            detail: e.to_string(),
        })?;
        serde_json::from_slice::<Resp>(&reply.payload).map_err(|e| TeiaError::Decode(e.to_string()))
    }

    /// Graceful drain — flush pending publishes + close connection.
    /// Mirrors [`async_nats::Client::drain`].
    pub async fn drain(&self) -> Result<(), TeiaError> {
        self.nats
            .drain()
            .await
            .map_err(|e| TeiaError::ConnectFailed(format!("drain: {e}")))
    }
}

/// Typed wrapper over [`async_nats::Subscriber`].
pub struct Subscription {
    inner: async_nats::Subscriber,
}

impl Subscription {
    /// Pull the next message. Returns `None` if the subscription
    /// has been unsubscribed.
    pub async fn next_message(&mut self) -> Option<async_nats::Message> {
        use futures::StreamExt;
        self.inner.next().await
    }

    /// Decode the next message's payload as JSON.
    pub async fn next_json<T: DeserializeOwned>(&mut self) -> Option<Result<T, TeiaError>> {
        let msg = self.next_message().await?;
        Some(
            serde_json::from_slice::<T>(&msg.payload)
                .map_err(|e| TeiaError::Decode(e.to_string())),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Connection failure with bad URL returns a typed error
    /// quickly (validation passes, connect fails on bind).
    #[tokio::test]
    async fn connect_to_bogus_server_returns_typed_error() {
        let cfg = TeiaConfig::default()
            .with_servers(vec!["nats://127.0.0.1:1".to_string()])
            .with_cluster("teia-test");
        // Set tight timeouts so the test runs fast.
        let cfg = TeiaConfig {
            connect_timeout: Duration::from_millis(200),
            server_timeout: Duration::from_millis(100),
            ..cfg
        };
        let res = TeiaClient::connect(cfg).await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(err, TeiaError::ConnectFailed(_)),
            "expected ConnectFailed, got: {err:?}"
        );
        assert_eq!(err.kind(), "connect_failed");
    }

    #[tokio::test]
    async fn connect_with_invalid_config_returns_invalid_config_error() {
        let cfg = TeiaConfig::default().with_servers(vec![]);
        let res = TeiaClient::connect(cfg).await;
        let err = res.unwrap_err();
        assert_eq!(err.kind(), "invalid_config");
    }
}
