//! `ReqwestKubeClient` — the canonical [`KubeClient`] impl over reqwest.
//!
//! Concrete async ops mapping to the apiserver's REST endpoints:
//! GET/POST/PUT/PATCH/DELETE. Watch lives in [`crate::watcher`] because
//! the trait returns a streaming object.
//!
//! Error mapping: every reqwest::Error → [`KubeError::Network`];
//! every apiserver 4xx/5xx → [`KubeError::ApiStatus`] with the typed
//! `ApiStatusKind` from the status code (+ Status body's reason
//! when available).

use async_trait::async_trait;
use engenho_types::api::{item_path, list_path};
use engenho_types::client::{DeleteOptions, KubeClient, List, ListOptions};
use engenho_types::error::{ApiStatusKind, KubeError};
use engenho_types::kind::KubeResource;
use engenho_types::patch::Patch;
use engenho_types::watch::Watcher;

use crate::connection::Connection;
use crate::watcher::ReqwestWatcher;

/// reqwest-backed [`KubeClient`].
#[derive(Clone)]
pub struct ReqwestKubeClient {
    conn: Connection,
}

impl ReqwestKubeClient {
    /// Construct a new client over `conn`.
    #[must_use]
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }

    /// Underlying [`Connection`] (shared with watchers + future
    /// engenho-controller-runtime).
    #[must_use]
    pub fn connection(&self) -> Connection {
        self.conn.clone()
    }

    fn build_url<R: KubeResource>(&self, namespace: Option<&str>, name: Option<&str>) -> String {
        let path = match name {
            Some(n) => item_path(R::GVR, R::SCOPE, namespace, n),
            None => list_path(R::GVR, R::SCOPE, namespace),
        };
        format!("{}{path}", self.conn.server())
    }

    async fn parse_response<T: serde::de::DeserializeOwned>(
        resp: reqwest::Response,
    ) -> Result<T, KubeError> {
        let status = resp.status();
        if status.is_success() {
            return resp
                .json::<T>()
                .await
                .map_err(|e| KubeError::Decode(format!("response body: {e}")));
        }
        let code = status.as_u16();
        let text = resp.text().await.unwrap_or_default();
        // Try to parse upstream `Status` body for a typed reason field.
        let kind = if let Ok(s) = serde_json::from_str::<serde_json::Value>(&text) {
            s.get("reason")
                .and_then(|r| r.as_str())
                .map(reason_to_kind)
                .unwrap_or_else(|| ApiStatusKind::from_code(code))
        } else {
            ApiStatusKind::from_code(code)
        };
        if matches!(kind, ApiStatusKind::NotFound) {
            return Err(KubeError::NotFound(text));
        }
        Err(KubeError::ApiStatus {
            code,
            kind,
            message: text,
        })
    }
}

fn reason_to_kind(reason: &str) -> ApiStatusKind {
    match reason {
        "Unauthorized" => ApiStatusKind::Unauthorized,
        "Forbidden" => ApiStatusKind::Forbidden,
        "NotFound" => ApiStatusKind::NotFound,
        "Conflict" => ApiStatusKind::Conflict,
        "AlreadyExists" => ApiStatusKind::AlreadyExists,
        "Gone" => ApiStatusKind::Gone,
        "Invalid" => ApiStatusKind::Invalid,
        "TooManyRequests" => ApiStatusKind::TooManyRequests,
        "InternalError" => ApiStatusKind::InternalError,
        "ServiceUnavailable" => ApiStatusKind::ServiceUnavailable,
        "Timeout" => ApiStatusKind::Timeout,
        _ => ApiStatusKind::Other,
    }
}

#[async_trait]
impl KubeClient for ReqwestKubeClient {
    async fn get<R: KubeResource + Send + Sync + 'static>(
        &self,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<R, KubeError> {
        let url = self.build_url::<R>(namespace, Some(name));
        let rb = self.conn.auth_header(self.conn.http().get(&url))?;
        let resp = rb
            .send()
            .await
            .map_err(|e| KubeError::Network(format!("GET {url}: {e}")))?;
        Self::parse_response::<R>(resp).await
    }

    async fn list<R: KubeResource + Send + Sync + 'static>(
        &self,
        namespace: Option<&str>,
        opts: &ListOptions,
    ) -> Result<List<R>, KubeError> {
        let base = self.build_url::<R>(namespace, None);
        // Typed query composition via url::Url — no `format!()` of
        // query-string syntax (★★ TYPED EMISSION). Append order +
        // form-urlencoding match the prior hand-rolled string.
        let url = crate::url_builder::KubeUrlBuilder::new(&base)?
            .pair_if_nonempty("labelSelector", &opts.label_selector)
            .pair_if_nonempty("fieldSelector", &opts.field_selector)
            .opt_int("limit", opts.limit)
            .opt_pair("continue", opts.continue_token.as_deref())
            .pair_if_nonempty("resourceVersion", &opts.resource_version)
            .finish();
        let rb = self.conn.auth_header(self.conn.http().get(&url))?;
        let resp = rb
            .send()
            .await
            .map_err(|e| KubeError::Network(format!("GET {url}: {e}")))?;

        // The k8s list response shape is `{ items: [...], metadata: { resourceVersion, continue } }`.
        // Decode into a thin local wrapper so we can re-pack into engenho's typed `List<R>`.
        #[derive(serde::Deserialize)]
        #[serde(bound(deserialize = "T: serde::de::DeserializeOwned"))]
        struct RawList<T> {
            #[serde(default = "Vec::new")]
            items: Vec<T>,
            #[serde(default)]
            metadata: RawListMeta,
        }
        #[derive(serde::Deserialize, Default)]
        struct RawListMeta {
            #[serde(default, rename = "resourceVersion")]
            resource_version: String,
            #[serde(default, rename = "continue")]
            continue_token: Option<String>,
        }
        let raw: RawList<R> = Self::parse_response(resp).await?;
        Ok(List {
            items: raw.items,
            resource_version: raw.metadata.resource_version,
            continue_token: raw.metadata.continue_token.filter(|s| !s.is_empty()),
        })
    }

    async fn create<R: KubeResource + Send + Sync + 'static>(
        &self,
        resource: &R,
    ) -> Result<R, KubeError> {
        let url = self.build_url::<R>(resource.namespace().as_deref(), None);
        let body = serde_json::to_vec(resource)
            .map_err(|e| KubeError::Encode(format!("create body: {e}")))?;
        let rb = self.conn.auth_header(
            self.conn
                .http()
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body),
        )?;
        let resp = rb
            .send()
            .await
            .map_err(|e| KubeError::Network(format!("POST {url}: {e}")))?;
        Self::parse_response(resp).await
    }

    async fn replace<R: KubeResource + Send + Sync + 'static>(
        &self,
        resource: &R,
    ) -> Result<R, KubeError> {
        let url = self.build_url::<R>(resource.namespace().as_deref(), Some(&resource.name()));
        let body = serde_json::to_vec(resource)
            .map_err(|e| KubeError::Encode(format!("replace body: {e}")))?;
        let rb = self.conn.auth_header(
            self.conn
                .http()
                .put(&url)
                .header("Content-Type", "application/json")
                .body(body),
        )?;
        let resp = rb
            .send()
            .await
            .map_err(|e| KubeError::Network(format!("PUT {url}: {e}")))?;
        Self::parse_response(resp).await
    }

    async fn patch<R: KubeResource + Send + Sync + 'static>(
        &self,
        namespace: Option<&str>,
        name: &str,
        patch: &Patch,
    ) -> Result<R, KubeError> {
        let base = self.build_url::<R>(namespace, Some(name));
        let url = if let Patch::Apply {
            field_manager,
            force,
            ..
        } = patch
        {
            // Typed query composition (★★ TYPED EMISSION).
            let mut b =
                crate::url_builder::KubeUrlBuilder::new(&base)?.pair("fieldManager", field_manager);
            if *force {
                b = b.flag("force", "true");
            }
            b.finish()
        } else {
            base
        };
        let body = patch
            .body_bytes()
            .map_err(|e| KubeError::Encode(format!("patch body: {e}")))?;
        let rb = self.conn.auth_header(
            self.conn
                .http()
                .patch(&url)
                .header("Content-Type", patch.content_type())
                .body(body),
        )?;
        let resp = rb
            .send()
            .await
            .map_err(|e| KubeError::Network(format!("PATCH {url}: {e}")))?;
        Self::parse_response(resp).await
    }

    async fn delete<R: KubeResource + Send + Sync + 'static>(
        &self,
        namespace: Option<&str>,
        name: &str,
        opts: &DeleteOptions,
    ) -> Result<(), KubeError> {
        let url = self.build_url::<R>(namespace, Some(name));
        let body =
            serde_json::to_vec(opts).map_err(|e| KubeError::Encode(format!("delete opts: {e}")))?;
        let rb = self.conn.auth_header(
            self.conn
                .http()
                .delete(&url)
                .header("Content-Type", "application/json")
                .body(body),
        )?;
        let resp = rb
            .send()
            .await
            .map_err(|e| KubeError::Network(format!("DELETE {url}: {e}")))?;
        if resp.status().is_success() {
            return Ok(());
        }
        let code = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        let kind = ApiStatusKind::from_code(code);
        if matches!(kind, ApiStatusKind::NotFound) {
            return Err(KubeError::NotFound(text));
        }
        Err(KubeError::ApiStatus {
            code,
            kind,
            message: text,
        })
    }

    fn watcher<R: KubeResource + Send + Sync + 'static>(&self) -> Box<dyn Watcher<R>> {
        Box::new(ReqwestWatcher::<R>::new(self.conn.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_types::auth::KubeAuth;

    // Smoke: client builds + serializes typed errors without I/O.
    #[test]
    fn client_builds_against_anonymous() {
        let conn = Connection::new("https://api.example.com", KubeAuth::Anonymous, None).unwrap();
        let _c = ReqwestKubeClient::new(conn);
    }

    // Query-string urlencoding is now owned by KubeUrlBuilder; see
    // `url_builder::tests::list_pairs_match_hand_rolled_encoding`.

    #[test]
    fn reason_strings_map_to_typed_kinds() {
        assert_eq!(reason_to_kind("Unauthorized"), ApiStatusKind::Unauthorized);
        assert_eq!(reason_to_kind("Forbidden"), ApiStatusKind::Forbidden);
        assert_eq!(
            reason_to_kind("AlreadyExists"),
            ApiStatusKind::AlreadyExists
        );
        assert_eq!(reason_to_kind("Conflict"), ApiStatusKind::Conflict);
        assert_eq!(reason_to_kind("Gone"), ApiStatusKind::Gone);
        assert_eq!(reason_to_kind("Invalid"), ApiStatusKind::Invalid);
        assert_eq!(
            reason_to_kind("TooManyRequests"),
            ApiStatusKind::TooManyRequests
        );
        assert_eq!(
            reason_to_kind("InternalError"),
            ApiStatusKind::InternalError
        );
        assert_eq!(reason_to_kind("Garbage"), ApiStatusKind::Other);
    }
}
