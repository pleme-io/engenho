use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use engenho_config::{EngenhoConfig, KubeletBackendKind};
use engenho_kubelet::{ContainerRuntime, FakeBackend};
use engenho_runtime::Runtime;
use serde_json::{Value, json};
use shikumi::TieredConfig;

const TABLE: &str = "application/json;as=Table;v=v1;g=meta.k8s.io,application/json";

fn config(data_dir: &std::path::Path) -> EngenhoConfig {
    let mut cfg = EngenhoConfig::prescribed_default();
    cfg.runtime.listen_addr = "127.0.0.1:0".into();
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.runtime.durable = true;
    cfg.runtime.node_name = "node-A".into();
    cfg.runtime.kubelet_backend = KubeletBackendKind::Fake;
    cfg.runtime.leadership_timeout_seconds = 5;
    cfg.runtime.tls.enabled = false;
    let enable = &mut cfg.controllers.enable;
    enable.deployment = true;
    enable.replicaset = true;
    enable.statefulset = true;
    enable.daemonset = true;
    enable.job = true;
    enable.gc = true;
    cfg.controllers.fallback_interval_seconds = 1;
    cfg.controllers.debounce_milliseconds = 20;
    cfg
}

fn admin_client(data_dir: &std::path::Path) -> reqwest::Client {
    let token = std::fs::read_to_string(data_dir.join("pki/admin.token"))
        .expect("runtime minted the admin bearer token");
    let mut headers = reqwest::header::HeaderMap::new();
    let mut auth = reqwest::header::HeaderValue::from_str(&["Bearer ", token.trim()].concat())
        .expect("valid bearer header");
    auth.set_sensitive(true);
    headers.insert(reqwest::header::AUTHORIZATION, auth);
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .expect("admin client builds")
}

fn template(app: &str) -> Value {
    json!({
        "metadata": { "labels": { "app": app } },
        "spec": { "containers": [{ "name": "main", "image": "busybox:1.36" }] }
    })
}

fn writers() -> Vec<(&'static str, Value)> {
    vec![
        (
            "/api/v1/namespaces/default/pods",
            json!({"metadata": {"name": "bare"}, "spec": {"containers": [{"name": "main", "image": "busybox:1.36"}]}}),
        ),
        (
            "/apis/apps/v1/namespaces/default/deployments",
            json!({"metadata": {"name": "dep"}, "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "dep"}}, "template": template("dep")}}),
        ),
        (
            "/apis/apps/v1/namespaces/default/statefulsets",
            json!({"metadata": {"name": "sts"}, "spec": {"replicas": 1, "serviceName": "sts", "selector": {"matchLabels": {"app": "sts"}}, "template": template("sts")}}),
        ),
        (
            "/apis/apps/v1/namespaces/default/daemonsets",
            json!({"metadata": {"name": "ds"}, "spec": {"selector": {"matchLabels": {"app": "ds"}}, "template": template("ds")}}),
        ),
        (
            "/apis/batch/v1/namespaces/default/jobs",
            json!({"metadata": {"name": "job"}, "spec": {"template": {"spec": {"restartPolicy": "Never", "containers": [{"name": "main", "image": "busybox:1.36"}]}}}}),
        ),
    ]
}

fn owner_kind(pod: &Value) -> &str {
    pod.pointer("/metadata/ownerReferences/0/kind")
        .and_then(Value::as_str)
        .unwrap_or("none")
}

fn missing(pod: &Value) -> Vec<&'static str> {
    let mut gaps = Vec::new();
    for (pointer, what) in [
        ("/metadata", "metadata"),
        ("/spec", "spec"),
        ("/status", "status"),
    ] {
        if !pod.pointer(pointer).is_some_and(Value::is_object) {
            gaps.push(what);
        }
    }
    for (pointer, what) in [
        ("/metadata/name", "metadata.name"),
        ("/metadata/namespace", "metadata.namespace"),
        ("/metadata/uid", "metadata.uid"),
        ("/metadata/resourceVersion", "metadata.resourceVersion"),
        ("/metadata/creationTimestamp", "metadata.creationTimestamp"),
        ("/status/phase", "status.phase"),
    ] {
        if !pod.pointer(pointer).is_some_and(Value::is_string) {
            gaps.push(what);
        }
    }
    if !pod.pointer("/spec/containers").is_some_and(Value::is_array) {
        gaps.push("spec.containers");
    }
    gaps
}

struct Findings(Vec<String>);

impl Findings {
    fn check_watch(&mut self, label: &str, events: &[Value]) -> BTreeSet<String> {
        let mut types = BTreeSet::new();
        for event in events {
            let kind = event["type"].as_str().unwrap_or("?").to_owned();
            if kind != "BOOKMARK" {
                let label = [label, " ", &kind].concat();
                let object = &event["object"];
                match object["rows"].as_array() {
                    Some(rows) => rows
                        .iter()
                        .for_each(|row| self.check(&label, &row["object"])),
                    None => self.check(&label, object),
                }
            }
            types.insert(kind);
        }
        types
    }

    fn check(&mut self, path: &str, pod: &Value) {
        let gaps = missing(pod);
        if !gaps.is_empty() {
            let name = pod
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .unwrap_or("?");
            self.0.push(
                [
                    path,
                    " ",
                    name,
                    " (",
                    owner_kind(pod),
                    "): missing ",
                    &gaps.join(", "),
                ]
                .concat(),
            );
        }
    }
}

async fn watch_all(
    client: reqwest::Client,
    url: String,
    accept: Option<&'static str>,
    until: Instant,
) -> Vec<Value> {
    let mut req = client.get(url);
    if let Some(accept) = accept {
        req = req.header(reqwest::header::ACCEPT, accept);
    }
    let mut resp = req.send().await.expect("watch opens");
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "watch answered");
    let mut buf: Vec<u8> = Vec::new();
    let mut events = Vec::new();
    while let Some(left) = until.checked_duration_since(Instant::now()) {
        match tokio::time::timeout(left, resp.chunk()).await {
            Ok(Ok(Some(bytes))) => buf.extend_from_slice(&bytes),
            _ => break,
        }
        while let Some(pos) = buf.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = buf.drain(..=pos).collect();
            if let Ok(event) = serde_json::from_slice::<Value>(&line[..line.len() - 1]) {
                events.push(event);
            }
        }
    }
    events
}

async fn get_json(client: &reqwest::Client, url: &str, accept: Option<&str>) -> Value {
    let mut req = client.get(url);
    if let Some(accept) = accept {
        req = req.header(reqwest::header::ACCEPT, accept);
    }
    let resp = req.send().await.expect("GET answered");
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "GET {url}");
    resp.json().await.expect("GET json")
}

async fn pods_from_every_writer(client: &reqwest::Client, base: &str) -> Vec<Value> {
    let wanted: BTreeSet<&str> = ["none", "ReplicaSet", "StatefulSet", "DaemonSet", "Job"].into();
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        let list = get_json(client, &[base, "/api/v1/pods"].concat(), None).await;
        let items = list["items"].as_array().cloned().unwrap_or_default();
        let seen: BTreeSet<&str> = items.iter().map(owner_kind).collect();
        if wanted.is_subset(&seen) || Instant::now() >= deadline {
            let missing_writers: Vec<&&str> = wanted.difference(&seen).collect();
            assert!(
                missing_writers.is_empty(),
                "no pod was created by: {missing_writers:?}"
            );
            return items;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn every_pod_a_client_is_served_is_a_complete_pod() {
    let tmp = tempfile::tempdir().unwrap();
    let backend: Arc<dyn ContainerRuntime> = Arc::new(FakeBackend::new());
    let rt = Runtime::start_with_backend(config(tmp.path()), backend)
        .await
        .expect("runtime boots");
    let addr = rt.local_addr();
    let client = admin_client(tmp.path());
    let base = ["http://", &addr.to_string()].concat();

    let watch = tokio::spawn(watch_all(
        client.clone(),
        [base.as_str(), "/api/v1/pods?watch=true"].concat(),
        None,
        Instant::now() + Duration::from_secs(20),
    ));
    let table_watch = tokio::spawn(watch_all(
        client.clone(),
        [
            base.as_str(),
            "/api/v1/pods?watch=true&includeObject=Object",
        ]
        .concat(),
        Some(TABLE),
        Instant::now() + Duration::from_secs(20),
    ));

    let mut findings = Findings(Vec::new());
    for (path, body) in writers() {
        let resp = client
            .post([base.as_str(), path].concat())
            .json(&body)
            .send()
            .await
            .expect("POST answered");
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED, "POST {path}");
        if path.ends_with("/pods") {
            let created: Value = resp.json().await.unwrap();
            findings.check("create response", &created);
        }
    }

    let pods = pods_from_every_writer(&client, &base).await;

    for pod in &pods {
        findings.check("LIST", pod);
        let name = pod["metadata"]["name"].as_str().unwrap();
        let one = get_json(
            &client,
            &[base.as_str(), "/api/v1/namespaces/default/pods/", name].concat(),
            None,
        )
        .await;
        findings.check("GET", &one);
    }

    let table = get_json(
        &client,
        &[base.as_str(), "/api/v1/pods?includeObject=Object"].concat(),
        Some(TABLE),
    )
    .await;
    for row in table["rows"].as_array().cloned().unwrap_or_default() {
        findings.check("Table row (includeObject=Object)", &row["object"]);
    }

    let deleted = client
        .delete([base.as_str(), "/api/v1/namespaces/default/pods/bare"].concat())
        .send()
        .await
        .expect("DELETE answered");
    assert!(
        deleted.status().is_success(),
        "DELETE bare: {}",
        deleted.status()
    );

    let types = findings.check_watch("watch", &watch.await.unwrap());
    for kind in ["ADDED", "MODIFIED", "DELETED"] {
        assert!(
            types.contains(kind),
            "the watch never saw a {kind} event: {types:?}"
        );
    }
    let table_types = findings.check_watch("Table watch", &table_watch.await.unwrap());
    assert!(
        !table_types.is_empty(),
        "the Table watch delivered no events"
    );

    assert!(
        findings.0.is_empty(),
        "{} served pods were incomplete:\n  {}",
        findings.0.len(),
        findings.0.join("\n  ")
    );
}
