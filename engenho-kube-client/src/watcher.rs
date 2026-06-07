//! `ReqwestWatcher` — streaming watch implementation.
//!
//! Opens `GET …?watch=true&resourceVersion=N` against the apiserver
//! and parses the chunked NDJSON response into [`WatchEvent<R>`]. Each
//! line is a JSON-encoded `metav1.WatchEvent` which we decode via
//! [`RawWatchEvent::into_typed`].

use std::marker::PhantomData;
use std::pin::Pin;

use async_trait::async_trait;
use bytes::BytesMut;
use engenho_types::api::list_path;
use engenho_types::error::KubeError;
use engenho_types::kind::KubeResource;
use engenho_types::watch::{RawWatchEvent, WatchEvent, Watcher};
use futures_core::Stream;
use futures_util::StreamExt;

use crate::connection::Connection;

/// reqwest-backed [`Watcher`] for resource type `R`.
pub struct ReqwestWatcher<R> {
    conn: Connection,
    _kind: PhantomData<R>,
}

impl<R: KubeResource> ReqwestWatcher<R> {
    /// Build a new watcher bound to `conn`. Cheap — no I/O until
    /// `watch()` is called.
    #[must_use]
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            _kind: PhantomData,
        }
    }
}

#[async_trait]
impl<R: KubeResource + Send + Sync + 'static> Watcher<R> for ReqwestWatcher<R> {
    async fn watch(
        &self,
        namespace: Option<&str>,
        resource_version: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<WatchEvent<R>, KubeError>> + Send>>, KubeError>
    {
        let base = format!(
            "{}{}",
            self.conn.server(),
            list_path(R::GVR, R::SCOPE, namespace)
        );
        // Typed query composition via url::Url — no `format!()` of the
        // `?watch=true&resourceVersion=…` query string (★★ TYPED
        // EMISSION). Byte-identical to the prior hand-rolled form.
        let url = crate::url_builder::KubeUrlBuilder::new(&base)?
            .flag("watch", "true")
            .pair_if_nonempty("resourceVersion", resource_version)
            .finish();
        let rb = self.conn.auth_header(self.conn.http().get(&url))?;
        let resp = rb
            .send()
            .await
            .map_err(|e| KubeError::Network(format!("GET {url}: {e}")))?;
        if !resp.status().is_success() {
            let code = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(KubeError::ApiStatus {
                code,
                kind: engenho_types::error::ApiStatusKind::from_code(code),
                message: body,
            });
        }
        let mut byte_stream = resp.bytes_stream();
        let stream = async_stream::stream! {
            let mut buf = BytesMut::with_capacity(8 * 1024);
            while let Some(chunk) = byte_stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        buf.extend_from_slice(&bytes);
                        loop {
                            let Some(nl) = memchr_newline(&buf) else { break };
                            let line = buf.split_to(nl + 1);
                            let trimmed = line.iter().take(nl).copied().collect::<Vec<u8>>();
                            if trimmed.is_empty() { continue }
                            match serde_json::from_slice::<RawWatchEvent>(&trimmed) {
                                Ok(raw)  => yield raw.into_typed::<R>(),
                                Err(e)   => yield Err(KubeError::Decode(format!("watch line: {e}"))),
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(KubeError::Network(format!("chunk: {e}")));
                        break;
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }
}

fn memchr_newline(buf: &[u8]) -> Option<usize> {
    buf.iter().position(|&b| b == b'\n')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newline_index_finds_first() {
        assert_eq!(memchr_newline(b"abc\ndef\n"), Some(3));
        assert_eq!(memchr_newline(b"\nabc"), Some(0));
        assert_eq!(memchr_newline(b"no newline"), None);
    }
}
