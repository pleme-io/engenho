//! In-memory mock [`KubeClient`] for tests. No HTTP, deterministic.
//!
//! Behavior mirrors the real apiserver for the typical CRUD path:
//! GET returns NotFound when absent, CREATE assigns a synthetic
//! resourceVersion, PATCH is a JSON merge against the stored body,
//! WATCH streams events as objects are mutated.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use engenho_types::client::{DeleteOptions, KubeClient, List, ListOptions};
use engenho_types::error::{ApiStatusKind, KubeError};
use engenho_types::kind::KubeResource;
use engenho_types::patch::{JsonPatchOp, Patch};
use engenho_types::watch::{WatchEvent, Watcher};
use futures_core::Stream;

/// In-memory keyed by `(group, version, kind, namespace, name)`.
/// `metadata.resourceVersion` is auto-assigned as a monotonic counter.
#[derive(Default, Clone)]
pub struct MockKubeClient {
    inner: Arc<Mutex<MockState>>,
}

#[derive(Default)]
struct MockState {
    /// Map of (kind-fqn, namespace, name) → JSON body of the resource.
    objects: BTreeMap<(String, String, String), serde_json::Value>,
    /// Monotonic resourceVersion counter assigned on create/update.
    counter: u64,
}

impl MockKubeClient {
    /// Empty mock — no resources.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Direct insertion (for setting up test fixtures without going
    /// through `create`).
    pub fn insert<R: KubeResource>(&self, r: R) {
        let mut s = self.inner.lock().unwrap();
        s.counter += 1;
        let kind = kind_fqn::<R>();
        let ns = r.namespace().map(|c| c.into_owned()).unwrap_or_default();
        let name = r.name().into_owned();
        let mut body = serde_json::to_value(&r).unwrap();
        if let Some(obj) = body.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            obj.insert(
                "resourceVersion".into(),
                serde_json::Value::String(s.counter.to_string()),
            );
        }
        s.objects.insert((kind, ns, name), body);
    }
}

fn kind_fqn<R: KubeResource>() -> String {
    format!("{}/{}/{}", R::GVK.group, R::GVK.version, R::GVK.kind)
}

#[async_trait]
impl KubeClient for MockKubeClient {
    async fn get<R: KubeResource + Send + Sync + 'static>(
        &self,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<R, KubeError> {
        let s = self.inner.lock().unwrap();
        let key = (
            kind_fqn::<R>(),
            namespace.unwrap_or("").to_string(),
            name.to_string(),
        );
        let body = s
            .objects
            .get(&key)
            .ok_or_else(|| KubeError::NotFound(format!("mock: {key:?}")))?;
        serde_json::from_value(body.clone())
            .map_err(|e| KubeError::Decode(format!("mock decode: {e}")))
    }

    async fn list<R: KubeResource + Send + Sync + 'static>(
        &self,
        namespace: Option<&str>,
        _opts: &ListOptions,
    ) -> Result<List<R>, KubeError> {
        let s = self.inner.lock().unwrap();
        let kind = kind_fqn::<R>();
        let mut items: Vec<R> = Vec::new();
        for ((k, ns, _name), body) in &s.objects {
            if k == &kind && namespace.map_or(true, |n| n == ns) {
                let r: R = serde_json::from_value(body.clone())
                    .map_err(|e| KubeError::Decode(format!("mock decode: {e}")))?;
                items.push(r);
            }
        }
        Ok(List {
            items,
            resource_version: s.counter.to_string(),
            continue_token: None,
        })
    }

    async fn create<R: KubeResource + Send + Sync + 'static>(
        &self,
        resource: &R,
    ) -> Result<R, KubeError> {
        let mut s = self.inner.lock().unwrap();
        s.counter += 1;
        let kind = kind_fqn::<R>();
        let ns = resource
            .namespace()
            .map(|c| c.into_owned())
            .unwrap_or_default();
        let name = resource.name().into_owned();
        let key = (kind, ns, name);
        if s.objects.contains_key(&key) {
            return Err(KubeError::ApiStatus {
                code: 409,
                kind: ApiStatusKind::AlreadyExists,
                message: format!("mock: already exists {key:?}"),
            });
        }
        let mut body = serde_json::to_value(resource)
            .map_err(|e| KubeError::Encode(format!("mock create: {e}")))?;
        if let Some(obj) = body.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            obj.insert(
                "resourceVersion".into(),
                serde_json::Value::String(s.counter.to_string()),
            );
        }
        s.objects.insert(key, body.clone());
        serde_json::from_value(body).map_err(|e| KubeError::Decode(format!("mock decode: {e}")))
    }

    async fn replace<R: KubeResource + Send + Sync + 'static>(
        &self,
        resource: &R,
    ) -> Result<R, KubeError> {
        let mut s = self.inner.lock().unwrap();
        s.counter += 1;
        let kind = kind_fqn::<R>();
        let ns = resource
            .namespace()
            .map(|c| c.into_owned())
            .unwrap_or_default();
        let name = resource.name().into_owned();
        let key = (kind, ns, name);
        if !s.objects.contains_key(&key) {
            return Err(KubeError::NotFound(format!("mock: {key:?}")));
        }
        let mut body = serde_json::to_value(resource)
            .map_err(|e| KubeError::Encode(format!("mock replace: {e}")))?;
        if let Some(obj) = body.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            obj.insert(
                "resourceVersion".into(),
                serde_json::Value::String(s.counter.to_string()),
            );
        }
        s.objects.insert(key, body.clone());
        serde_json::from_value(body).map_err(|e| KubeError::Decode(format!("mock decode: {e}")))
    }

    async fn patch<R: KubeResource + Send + Sync + 'static>(
        &self,
        namespace: Option<&str>,
        name: &str,
        patch: &Patch,
    ) -> Result<R, KubeError> {
        let mut s = self.inner.lock().unwrap();
        let kind = kind_fqn::<R>();
        let key = (kind, namespace.unwrap_or("").to_string(), name.to_string());
        let body = s
            .objects
            .get(&key)
            .ok_or_else(|| KubeError::NotFound(format!("mock: {key:?}")))?
            .clone();
        // Honestly dispatch on the patch algorithm. The previous mock FAKED
        // every algorithm (strategic collapsed to RFC7386, json-patch ops
        // SILENTLY DROPPED — a wrong Ok). Now:
        //   * Merge / Strategic / Apply → RFC7396 merge (the test double has
        //     no OpenAPI schema env, so strategic falls back to merge — but
        //     this is an explicit, documented fallback, not a silent lie).
        //   * Json → the RFC6902 ops are actually applied (never dropped). A
        //     `test` failure / bad pointer surfaces as a typed `KubeError`,
        //     never a silent wrong Ok.
        let merged = match patch {
            Patch::Merge(p) | Patch::Strategic(p) => merge_json(body, p.clone()),
            Patch::Apply { body: p, .. } => merge_json(body, p.clone()),
            Patch::Json(ops) => apply_json_patch_ops(body, ops)
                .map_err(|e| KubeError::Decode(format!("mock json-patch: {e}")))?,
        };
        s.counter += 1;
        let mut new_body = merged;
        if let Some(obj) = new_body.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            obj.insert(
                "resourceVersion".into(),
                serde_json::Value::String(s.counter.to_string()),
            );
        }
        s.objects.insert(key, new_body.clone());
        serde_json::from_value(new_body).map_err(|e| KubeError::Decode(format!("mock decode: {e}")))
    }

    async fn delete<R: KubeResource + Send + Sync + 'static>(
        &self,
        namespace: Option<&str>,
        name: &str,
        _opts: &DeleteOptions,
    ) -> Result<(), KubeError> {
        let mut s = self.inner.lock().unwrap();
        let kind = kind_fqn::<R>();
        let key = (kind, namespace.unwrap_or("").to_string(), name.to_string());
        if s.objects.remove(&key).is_none() {
            return Err(KubeError::NotFound(format!("mock: {key:?}")));
        }
        Ok(())
    }

    fn watcher<R: KubeResource + Send + Sync + 'static>(&self) -> Box<dyn Watcher<R>> {
        Box::new(MockWatcher::<R>::default())
    }
}

fn merge_json(mut base: serde_json::Value, patch: serde_json::Value) -> serde_json::Value {
    match (&mut base, patch) {
        (serde_json::Value::Object(b), serde_json::Value::Object(p)) => {
            for (k, v) in p {
                let entry = b.entry(k).or_insert(serde_json::Value::Null);
                let merged = merge_json(entry.clone(), v);
                *entry = merged;
            }
            base
        }
        (_, p) => p,
    }
}

/// Apply an RFC 6902 op list to `base` atomically (test-double impl). Ops are
/// folded over a clone; the clone is returned only if EVERY op succeeds — a
/// `test` mismatch or a bad JSON-Pointer rejects the whole document, NEVER a
/// silent drop. (The canonical interpreter is
/// `engenho_store::patch_apply::apply`; this small fold mirrors its json-patch
/// arm so the client mock is honest without the store dependency. It is a
/// candidate to re-home onto the canonical impl once that is promoted to
/// engenho-types.)
fn apply_json_patch_ops(
    base: serde_json::Value,
    ops: &[JsonPatchOp],
) -> Result<serde_json::Value, String> {
    let mut doc = base;
    for op in ops {
        match op {
            JsonPatchOp::Add { path, value } => mock_ptr_add(&mut doc, path, value.clone())?,
            JsonPatchOp::Remove { path } => {
                mock_ptr_remove(&mut doc, path)?;
            }
            JsonPatchOp::Replace { path, value } => {
                doc.pointer(path)
                    .ok_or_else(|| format!("replace: missing pointer {path}"))?;
                mock_ptr_remove(&mut doc, path)?;
                mock_ptr_add(&mut doc, path, value.clone())?;
            }
            JsonPatchOp::Copy { from, path } => {
                let v = doc
                    .pointer(from)
                    .ok_or_else(|| format!("copy: missing from {from}"))?
                    .clone();
                mock_ptr_add(&mut doc, path, v)?;
            }
            JsonPatchOp::Move { from, path } => {
                let v = mock_ptr_remove(&mut doc, from)?;
                mock_ptr_add(&mut doc, path, v)?;
            }
            JsonPatchOp::Test { path, value } => {
                let actual = doc
                    .pointer(path)
                    .ok_or_else(|| format!("test: missing pointer {path}"))?;
                if actual != value {
                    return Err(format!("test failed at {path}"));
                }
            }
        }
    }
    Ok(doc)
}

fn mock_ptr_tokens(path: &str) -> Vec<String> {
    if path.is_empty() {
        return Vec::new();
    }
    path.split('/')
        .skip(1)
        .map(|t| t.replace("~1", "/").replace("~0", "~"))
        .collect()
}

fn mock_ptr_add(
    doc: &mut serde_json::Value,
    path: &str,
    value: serde_json::Value,
) -> Result<(), String> {
    let tokens = mock_ptr_tokens(path);
    let Some((last, parents)) = tokens.split_last() else {
        *doc = value;
        return Ok(());
    };
    let mut cur = doc;
    for t in parents {
        cur = match cur {
            serde_json::Value::Object(m) => {
                m.get_mut(t).ok_or_else(|| format!("add: missing {path}"))?
            }
            serde_json::Value::Array(a) => {
                let i: usize = t.parse().map_err(|_| format!("add: bad index {path}"))?;
                a.get_mut(i).ok_or_else(|| format!("add: missing {path}"))?
            }
            _ => return Err(format!("add: not a container {path}")),
        };
    }
    match cur {
        serde_json::Value::Object(m) => {
            m.insert(last.clone(), value);
            Ok(())
        }
        serde_json::Value::Array(a) => {
            if last == "-" {
                a.push(value);
            } else {
                let i: usize = last.parse().map_err(|_| format!("add: bad index {path}"))?;
                if i > a.len() {
                    return Err(format!("add: index out of range {path}"));
                }
                a.insert(i, value);
            }
            Ok(())
        }
        _ => Err(format!("add: not a container {path}")),
    }
}

fn mock_ptr_remove(doc: &mut serde_json::Value, path: &str) -> Result<serde_json::Value, String> {
    let tokens = mock_ptr_tokens(path);
    let Some((last, parents)) = tokens.split_last() else {
        return Ok(std::mem::replace(doc, serde_json::Value::Null));
    };
    let mut cur = doc;
    for t in parents {
        cur = match cur {
            serde_json::Value::Object(m) => m
                .get_mut(t)
                .ok_or_else(|| format!("remove: missing {path}"))?,
            serde_json::Value::Array(a) => {
                let i: usize = t.parse().map_err(|_| format!("remove: bad index {path}"))?;
                a.get_mut(i)
                    .ok_or_else(|| format!("remove: missing {path}"))?
            }
            _ => return Err(format!("remove: not a container {path}")),
        };
    }
    match cur {
        serde_json::Value::Object(m) => m
            .remove(last)
            .ok_or_else(|| format!("remove: missing {path}")),
        serde_json::Value::Array(a) => {
            let i: usize = last
                .parse()
                .map_err(|_| format!("remove: bad index {path}"))?;
            if i >= a.len() {
                return Err(format!("remove: index out of range {path}"));
            }
            Ok(a.remove(i))
        }
        _ => Err(format!("remove: not a container {path}")),
    }
}

/// Mock watcher — yields an empty stream. Sufficient for the trait
/// dyn-compat check; future enhancement: hook to `MockKubeClient`
/// mutations to emit synthetic events.
pub struct MockWatcher<R> {
    _kind: std::marker::PhantomData<R>,
}

impl<R> Default for MockWatcher<R> {
    fn default() -> Self {
        Self {
            _kind: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<R: KubeResource + Send + Sync + 'static> Watcher<R> for MockWatcher<R> {
    async fn watch(
        &self,
        _namespace: Option<&str>,
        _resource_version: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<WatchEvent<R>, KubeError>> + Send>>, KubeError>
    {
        Ok(Box::pin(futures_util::stream::empty()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engenho_types::kind::{GroupVersionKind, GroupVersionResource, Scope};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct ToyPod {
        metadata: engenho_types::meta::ObjectMeta,
    }

    impl KubeResource for ToyPod {
        const GVK: GroupVersionKind = GroupVersionKind {
            group: "",
            version: "v1",
            kind: "ToyPod",
        };
        const GVR: GroupVersionResource = GroupVersionResource {
            group: "",
            version: "v1",
            resource: "toypods",
        };
        const SCOPE: Scope = Scope::Namespaced;
        fn name(&self) -> std::borrow::Cow<'_, str> {
            self.metadata.name.as_str().into()
        }
        fn namespace(&self) -> Option<std::borrow::Cow<'_, str>> {
            self.metadata.namespace.as_deref().map(|s| s.into())
        }
        fn resource_version(&self) -> Option<std::borrow::Cow<'_, str>> {
            if self.metadata.resource_version.is_empty() {
                None
            } else {
                Some(self.metadata.resource_version.as_str().into())
            }
        }
    }

    fn pod(name: &str, ns: &str) -> ToyPod {
        ToyPod {
            metadata: engenho_types::meta::ObjectMeta {
                name: name.into(),
                namespace: Some(ns.into()),
                ..Default::default()
            },
        }
    }

    #[tokio::test]
    async fn create_then_get_roundtrip() {
        let c = MockKubeClient::new();
        let p = pod("p1", "default");
        let created = c.create(&p).await.unwrap();
        assert_eq!(created.metadata.name, "p1");
        assert_eq!(created.metadata.resource_version, "1");
        let fetched: ToyPod = c.get(Some("default"), "p1").await.unwrap();
        assert_eq!(fetched.metadata.name, "p1");
    }

    #[tokio::test]
    async fn get_missing_is_notfound() {
        let c = MockKubeClient::new();
        let r: Result<ToyPod, _> = c.get(Some("default"), "missing").await;
        assert!(matches!(r, Err(KubeError::NotFound(_))));
    }

    #[tokio::test]
    async fn create_existing_is_alreadyexists() {
        let c = MockKubeClient::new();
        let p = pod("p1", "default");
        c.create(&p).await.unwrap();
        let r = c.create(&p).await;
        assert!(matches!(
            r,
            Err(KubeError::ApiStatus {
                kind: ApiStatusKind::AlreadyExists,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn list_filters_by_namespace() {
        let c = MockKubeClient::new();
        c.create(&pod("p1", "ns1")).await.unwrap();
        c.create(&pod("p2", "ns2")).await.unwrap();
        let l: List<ToyPod> = c.list(Some("ns1"), &ListOptions::default()).await.unwrap();
        assert_eq!(l.items.len(), 1);
        assert_eq!(l.items[0].metadata.name, "p1");
    }

    #[tokio::test]
    async fn list_all_namespaces() {
        let c = MockKubeClient::new();
        c.create(&pod("p1", "ns1")).await.unwrap();
        c.create(&pod("p2", "ns2")).await.unwrap();
        let l: List<ToyPod> = c.list(None, &ListOptions::default()).await.unwrap();
        assert_eq!(l.items.len(), 2);
    }

    #[tokio::test]
    async fn patch_merge_bumps_resource_version() {
        let c = MockKubeClient::new();
        c.create(&pod("p1", "default")).await.unwrap();
        let patch = Patch::Merge(serde_json::json!({
            "metadata": {"labels": {"x": "y"}}
        }));
        let patched: ToyPod = c.patch(Some("default"), "p1", &patch).await.unwrap();
        assert_eq!(patched.metadata.labels.get("x"), Some(&"y".to_string()));
        assert_eq!(patched.metadata.resource_version, "2"); // bumped from "1"
    }

    #[tokio::test]
    async fn delete_then_get_is_notfound() {
        let c = MockKubeClient::new();
        c.create(&pod("p1", "default")).await.unwrap();
        c.delete::<ToyPod>(Some("default"), "p1", &DeleteOptions::default())
            .await
            .unwrap();
        let r: Result<ToyPod, _> = c.get(Some("default"), "p1").await;
        assert!(matches!(r, Err(KubeError::NotFound(_))));
    }

    #[tokio::test]
    async fn delete_missing_is_notfound() {
        let c = MockKubeClient::new();
        let r = c
            .delete::<ToyPod>(Some("default"), "p1", &DeleteOptions::default())
            .await;
        assert!(matches!(r, Err(KubeError::NotFound(_))));
    }

    #[tokio::test]
    async fn replace_missing_is_notfound() {
        let c = MockKubeClient::new();
        let r = c.replace(&pod("p1", "default")).await;
        assert!(matches!(r, Err(KubeError::NotFound(_))));
    }
}
