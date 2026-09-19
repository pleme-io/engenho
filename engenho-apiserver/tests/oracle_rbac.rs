//! RBAC resource matching against upstream `ResourceMatches` (Kubernetes
//! v1.34.0), row by row, through the public authorizer.
//!
//! Each row asks one question: does a rule whose `resources` are
//! `rule_resources` grant a request for `resource` (+ `subresource`)? The
//! adapter builds one ClusterRole with that rule (verbs and apiGroups `*`, so
//! only the resource match decides), binds it to a user, and reads Allow as
//! "matches". Going through `Authorizer::authorize` rather than the private
//! matcher means the row also covers how a request's subresource reaches it.

use std::collections::HashMap;

use async_trait::async_trait;
use engenho_apiserver::authz::{Attributes, Authorizer, RbacAuthorizer, RbacStoreEnv};
use engenho_oracle::{Answer, Case, Vector, assert_table};
use engenho_types::auth::UserInfo;
use engenho_types::generated_v1_34::rbac_v1::{ClusterRole, ClusterRoleBinding, Role, RoleBinding};
use serde_json::{Value, json};

/// One ClusterRole ("oracle") bound to one user ("alice"). Nothing else.
struct OneRule {
    binding: ClusterRoleBinding,
    role: ClusterRole,
}

impl OneRule {
    fn granting(resources: &Value) -> Self {
        let role = serde_json::from_value(json!({
            "metadata": {"name": "oracle"},
            "rules": [{"verbs": ["*"], "apiGroups": ["*"], "resources": resources}],
        }))
        .expect("ClusterRole from the row");
        let binding = serde_json::from_value(json!({
            "metadata": {"name": "oracle"},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "oracle"},
            "subjects": [{"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": "alice"}],
        }))
        .expect("ClusterRoleBinding");
        Self { binding, role }
    }
}

#[async_trait]
impl RbacStoreEnv for OneRule {
    async fn list_cluster_role_bindings(&self) -> Vec<ClusterRoleBinding> {
        vec![self.binding.clone()]
    }
    async fn list_role_bindings(&self, _ns: &str) -> Vec<RoleBinding> {
        Vec::new()
    }
    async fn get_cluster_role(&self, name: &str) -> Option<ClusterRole> {
        (name == "oracle").then(|| self.role.clone())
    }
    async fn get_role(&self, _ns: &str, _name: &str) -> Option<Role> {
        None
    }
}

fn text(v: &Value, key: &str) -> String {
    v[key].as_str().unwrap_or_default().to_owned()
}

async fn answer(case: &Case) -> Answer {
    let input = &case.input;
    let authorizer = RbacAuthorizer::new(OneRule::granting(&input["rule_resources"]));
    let attrs = Attributes {
        user: UserInfo {
            username: "alice".to_owned(),
            uid: String::new(),
            groups: Vec::new(),
            extra: Default::default(),
        },
        verb: "get".to_owned(),
        group: String::new(),
        version: "v1".to_owned(),
        resource: text(input, "resource"),
        // Verbatim, "" included: a SubjectAccessReview may carry an empty
        // subresource, and upstream treats it as none.
        subresource: Some(text(input, "subresource")),
        namespace: None,
        name: None,
        non_resource_url: None,
    };
    Answer::Checked(json!({"matches": authorizer.authorize(&attrs).await.is_allow()}))
}

#[tokio::test]
async fn rbac_resource_matching_agrees_with_upstream() {
    let table = Vector::RbacResourceMatches.load();
    let mut answers = HashMap::new();
    for case in &table.cases {
        answers.insert(case.name.clone(), answer(case).await);
    }
    let report = assert_table(&table, &[], &[], |case| {
        answers.remove(&case.name).expect("answered")
    });
    assert_eq!(
        report.checked,
        table.cases.len(),
        "every RBAC row is checked"
    );
}
