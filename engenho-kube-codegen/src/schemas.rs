use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::catalog::KIND_CATALOG;
use crate::emit_typed::SchemaView;
use crate::openapi::OpenApiDoc;

pub fn schema_path_for_group(schema_dir: &Path, group: &str) -> Result<PathBuf> {
    let fname = match group {
        "" => "api__v1_openapi.json",
        "apps" => "apis__apps__v1_openapi.json",
        "rbac.authorization.k8s.io" => "apis__rbac.authorization.k8s.io__v1_openapi.json",
        "batch" => "apis__batch__v1_openapi.json",
        "networking.k8s.io" => "apis__networking.k8s.io__v1_openapi.json",
        "policy" => "apis__policy__v1_openapi.json",
        "storage.k8s.io" => "apis__storage.k8s.io__v1_openapi.json",
        "scheduling.k8s.io" => "apis__scheduling.k8s.io__v1_openapi.json",
        "coordination.k8s.io" => "apis__coordination.k8s.io__v1_openapi.json",
        "node.k8s.io" => "apis__node.k8s.io__v1_openapi.json",
        "autoscaling" => "apis__autoscaling__v2_openapi.json",
        other => bail!(
            "no vendored OpenAPI schema for group {other:?}: extend schemas::schema_path_for_group"
        ),
    };
    Ok(schema_dir.join(fname))
}

pub fn load_docs(schema_dir: &Path) -> Result<BTreeMap<&'static str, OpenApiDoc>> {
    let mut docs: BTreeMap<&'static str, OpenApiDoc> = BTreeMap::new();
    for entry in KIND_CATALOG {
        if entry.opaque || docs.contains_key(entry.group) {
            continue;
        }
        let path = schema_path_for_group(schema_dir, entry.group)?;
        let doc = OpenApiDoc::load(&path)
            .with_context(|| format!("load schema for group {:?}", entry.group))?;
        docs.insert(entry.group, doc);
    }
    Ok(docs)
}

#[must_use]
pub fn merge(docs: &BTreeMap<&'static str, OpenApiDoc>) -> BTreeMap<String, SchemaView> {
    let mut schemas: BTreeMap<String, SchemaView> = BTreeMap::new();
    for doc in docs.values() {
        for (key, shape) in &doc.components.schemas {
            schemas.entry(key.clone()).or_insert_with(|| SchemaView {
                description: shape.description.clone(),
                properties: shape.properties.clone(),
                required: shape.required.clone(),
            });
        }
    }
    schemas
}

pub fn load_schemas(schema_dir: &Path) -> Result<BTreeMap<String, SchemaView>> {
    Ok(merge(&load_docs(schema_dir)?))
}
