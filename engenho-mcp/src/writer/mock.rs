//! In-memory `ClusterWriter` mock — fixtures store the writes
//! observed so tests can assert what would have happened against
//! a real cluster.
//!
//! Production code never touches this module.

use async_trait::async_trait;
use std::sync::Mutex;

use crate::resource_kind::ResourceKind;
use crate::writer::{Authority, ClusterWriter, WriterError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedApply {
    pub cluster: String,
    pub kind: ResourceKind,
    pub namespace: String,
    pub name: String,
    pub field_manager: String,
    pub force: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedDelete {
    pub cluster: String,
    pub kind: ResourceKind,
    pub namespace: String,
    pub name: String,
}

#[derive(Default)]
pub struct MockClusterWriter {
    applies: Mutex<Vec<ObservedApply>>,
    deletes: Mutex<Vec<ObservedDelete>>,
}

impl MockClusterWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn applies(&self) -> Vec<ObservedApply> {
        self.applies.lock().unwrap().clone()
    }

    pub fn deletes(&self) -> Vec<ObservedDelete> {
        self.deletes.lock().unwrap().clone()
    }
}

#[async_trait]
impl ClusterWriter for MockClusterWriter {
    async fn apply_resource(
        &self,
        cluster: &str,
        kind: ResourceKind,
        namespace: &str,
        name: &str,
        _body: serde_json::Value,
        field_manager: &str,
        force: bool,
        authority: &Authority,
    ) -> Result<serde_json::Value, WriterError> {
        if !authority.can_write() {
            return Err(WriterError::AuthorityRequired);
        }
        self.applies.lock().unwrap().push(ObservedApply {
            cluster: cluster.to_string(),
            kind,
            namespace: namespace.to_string(),
            name: name.to_string(),
            field_manager: field_manager.to_string(),
            force,
        });
        Ok(serde_json::json!({"kind": "Stub", "metadata": {"name": name}}))
    }

    async fn delete_resource(
        &self,
        cluster: &str,
        kind: ResourceKind,
        namespace: &str,
        name: &str,
        authority: &Authority,
    ) -> Result<(), WriterError> {
        if !authority.can_write() {
            return Err(WriterError::AuthorityRequired);
        }
        self.deletes.lock().unwrap().push(ObservedDelete {
            cluster: cluster.to_string(),
            kind,
            namespace: namespace.to_string(),
            name: name.to_string(),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_writer_rejects_placeholder_authority() {
        let writer = MockClusterWriter::new();
        let err = writer
            .apply_resource(
                "demo",
                ResourceKind::Pod,
                "default",
                "x",
                serde_json::json!({}),
                "mcp",
                false,
                &Authority::Placeholder,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "authority_required");
        assert!(
            writer.applies().is_empty(),
            "rejected calls must not record"
        );
    }

    /// The "if Authority::SaguaoPassport were valid" path — we
    /// simulate it by using a struct-shaped test stub. Until the
    /// real variant lands, this test is a placeholder of its own,
    /// asserting only the rejection branch.
    ///
    /// When `Authority::SaguaoPassport { … }` lands:
    ///   1. Add the variant
    ///   2. Update `Authority::can_write` to match
    ///   3. Add the success-path test that asserts MockClusterWriter
    ///      records the observed apply.
    #[test]
    fn p2_authority_variant_lands_here() {
        // Placeholder for the test that will be filled in at P2.
        // For now this is a documentation breadcrumb — search for
        // this exact string when wiring saguão.
    }
}
