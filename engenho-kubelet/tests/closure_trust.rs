#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use engenho_config::{ClosureLocalBuilds, NativeSecurity};
use engenho_controllers::Controller;
use engenho_controllers::attestation::{
    Attestation, PathAttestation, SignatureVerifier, VerifierError,
};
use engenho_controllers::event_recorder::{CollectingEventSink, EventSink};
use engenho_kubelet::closure_trust::ClosureTrust;
use engenho_kubelet::{ClosureState, ClosureStore, FakeBackend, Kubelet, RootName};
use engenho_store::{
    InProcessRouter, ResourceKey, StoreMesh,
    command::{Reason, ResourceCommand},
    default_config,
};
use serde_json::json;

const CLOSURE: &str = "/nix/store/00000000000000000000000000000000-app";
const KEY: &str = "fleet:AAAA";

#[derive(Default)]
struct PresentStore;

#[async_trait::async_trait]
impl ClosureStore for PresentStore {
    fn name(&self) -> &'static str {
        "present"
    }
    async fn ensure(&self, _root: &RootName, _closure: &Path) -> ClosureState {
        ClosureState::Rooted
    }
    fn is_present(&self, _closure: &Path) -> bool {
        true
    }
    fn roots(&self) -> std::io::Result<BTreeMap<RootName, PathBuf>> {
        Ok(BTreeMap::new())
    }
    fn release(&self, _root: &RootName) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Store {
    Signed,
    Unsigned,
    SignedByAStranger,
    Local,
    Unreadable,
}

struct Scripted {
    store: Store,
    asked: Mutex<Vec<String>>,
}

impl Scripted {
    fn new(store: Store) -> Arc<Self> {
        Arc::new(Self {
            store,
            asked: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait::async_trait]
impl SignatureVerifier for Scripted {
    fn name(&self) -> &'static str {
        "scripted"
    }

    async fn attest(&self, image_ref: &str) -> Result<Attestation, VerifierError> {
        self.asked.lock().unwrap().push(image_ref.to_string());
        let path = |ultimate, claimed: &[&str], verified: &[&str]| PathAttestation {
            path: CLOSURE.to_string(),
            ultimate,
            content_addressed: false,
            claimed: claimed.iter().map(ToString::to_string).collect(),
            verified_by: verified.iter().map(ToString::to_string).collect(),
        };
        let row = match self.store {
            Store::Signed => path(false, &["fleet"], &[KEY]),
            Store::Unsigned => path(false, &[], &[]),
            Store::SignedByAStranger => path(false, &["stranger"], &[]),
            Store::Local => path(true, &[], &[]),
            Store::Unreadable => {
                return Err(VerifierError::Backend("no nix found on this node".into()));
            }
        };
        Ok(Attestation {
            store_keys: vec![KEY.to_string()],
            paths: vec![row],
        })
    }
}

async fn run(
    name: &str,
    store: Store,
    mode: NativeSecurity,
    local_builds: ClosureLocalBuilds,
) -> (
    serde_json::Value,
    Arc<FakeBackend>,
    Arc<Scripted>,
    Vec<String>,
) {
    let mesh = Arc::new(
        StoreMesh::start(
            1,
            "in-process://1".into(),
            InProcessRouter::new(),
            default_config(name).unwrap(),
        )
        .await
        .unwrap(),
    );
    mesh.initialize_singleton().await.unwrap();
    assert!(mesh.wait_for_leadership(Duration::from_secs(3)).await);
    let backend = Arc::new(FakeBackend::new());
    let verifier = Scripted::new(store);
    let events = Arc::new(CollectingEventSink::default());
    let sink: Arc<dyn EventSink> = events.clone();
    let kubelet = Kubelet::new(mesh.clone(), backend.clone(), "node-A")
        .with_event_sink(sink)
        .with_closure_store(Arc::new(PresentStore))
        .with_closure_trust(Arc::new(ClosureTrust::new(
            verifier.clone(),
            mode,
            Vec::new(),
            local_builds,
        )));
    let key = ResourceKey::namespaced("", "v1", "Pod", "default", "app");
    mesh.propose(ResourceCommand::put(
        key.clone(),
        json!({"kind": "Pod", "apiVersion": "v1", "metadata": {"name": "app", "namespace": "default"},
               "spec": {"nodeName": "node-A", "restartPolicy": "Always",
                        "containers": [{"name": "c", "image": format!("nix:{CLOSURE}")}]}}),
        Reason::Operator,
    ))
    .await
    .unwrap();
    kubelet.tick().await.unwrap();
    let pod = mesh.get(&key).await.unwrap();
    let said = events.drain().into_iter().map(|e| e.message).collect();
    (pod, backend, verifier, said)
}

fn refused(pod: &serde_json::Value) -> bool {
    pod.pointer("/status/phase") == Some(&json!("Failed"))
        && pod.pointer("/status/reason") == Some(&json!("ClosureUntrusted"))
}

async fn started(backend: &FakeBackend) -> usize {
    backend.running_count().await
}

#[tokio::test]
async fn a_signed_closure_runs_in_every_mode() {
    for (i, mode) in [
        NativeSecurity::Enforce,
        NativeSecurity::Audit,
        NativeSecurity::Off,
    ]
    .into_iter()
    .enumerate()
    {
        let (pod, backend, _, said) = run(
            &format!("trust-signed-{i}"),
            Store::Signed,
            mode,
            ClosureLocalBuilds::Trust,
        )
        .await;
        assert!(!refused(&pod), "{mode:?}: {pod}");
        assert_eq!(started(&backend).await, 1, "{mode:?}");
        assert!(
            !said.iter().any(|m| m.contains("would refuse")),
            "{mode:?}: {said:?}"
        );
    }
}

#[tokio::test]
async fn an_unsigned_closure_is_refused_under_enforce_naming_it_and_nothing_starts() {
    let (pod, backend, _, _) = run(
        "trust-unsigned-enforce",
        Store::Unsigned,
        NativeSecurity::Enforce,
        ClosureLocalBuilds::Trust,
    )
    .await;
    assert!(refused(&pod), "{pod}");
    let message = pod.pointer("/status/message").unwrap().as_str().unwrap();
    assert!(message.contains(CLOSURE), "{message}");
    assert!(message.contains("no signature"), "{message}");
    assert_eq!(started(&backend).await, 0);
}

#[tokio::test]
async fn an_unsigned_closure_runs_and_is_named_under_audit() {
    let (pod, backend, _, said) = run(
        "trust-unsigned-audit",
        Store::Unsigned,
        NativeSecurity::Audit,
        ClosureLocalBuilds::Trust,
    )
    .await;
    assert!(!refused(&pod), "{pod}");
    assert_eq!(started(&backend).await, 1);
    assert!(
        said.iter()
            .any(|m| m.contains("would refuse under enforce")),
        "{said:?}"
    );
}

#[tokio::test]
async fn off_runs_it_says_nothing_and_never_reads_the_store() {
    let (pod, backend, verifier, said) = run(
        "trust-unsigned-off",
        Store::Unsigned,
        NativeSecurity::Off,
        ClosureLocalBuilds::Trust,
    )
    .await;
    assert!(!refused(&pod), "{pod}");
    assert_eq!(started(&backend).await, 1);
    assert!(verifier.asked.lock().unwrap().is_empty());
    assert!(!said.iter().any(|m| m.contains("closure")), "{said:?}");
}

#[tokio::test]
async fn a_signature_under_a_key_this_node_does_not_trust_is_refused_by_name() {
    let (pod, backend, _, _) = run(
        "trust-stranger",
        Store::SignedByAStranger,
        NativeSecurity::Enforce,
        ClosureLocalBuilds::Trust,
    )
    .await;
    assert!(refused(&pod), "{pod}");
    let message = pod.pointer("/status/message").unwrap().as_str().unwrap();
    assert!(message.contains("stranger"), "{message}");
    assert_eq!(started(&backend).await, 0);
}

#[tokio::test]
async fn a_local_build_runs_under_enforce_and_is_said_as_plo_measured() {
    let (pod, backend, _, said) = run(
        "trust-local",
        Store::Local,
        NativeSecurity::Enforce,
        ClosureLocalBuilds::Trust,
    )
    .await;
    assert!(!refused(&pod), "{pod}");
    assert_eq!(started(&backend).await, 1);
    assert!(
        said.iter().any(|m| m.contains("built on this node")),
        "{said:?}"
    );
}

#[tokio::test]
async fn a_node_can_refuse_its_local_builds() {
    let (pod, _, _, _) = run(
        "trust-local-refused",
        Store::Local,
        NativeSecurity::Enforce,
        ClosureLocalBuilds::Refuse,
    )
    .await;
    assert!(refused(&pod), "{pod}");
}

#[tokio::test]
async fn a_store_that_cannot_be_read_refuses_under_enforce_and_warns_under_audit() {
    let (pod, _, _, _) = run(
        "trust-blind-enforce",
        Store::Unreadable,
        NativeSecurity::Enforce,
        ClosureLocalBuilds::Trust,
    )
    .await;
    assert!(refused(&pod), "{pod}");
    let (pod, backend, _, said) = run(
        "trust-blind-audit",
        Store::Unreadable,
        NativeSecurity::Audit,
        ClosureLocalBuilds::Trust,
    )
    .await;
    assert!(!refused(&pod), "{pod}");
    assert_eq!(started(&backend).await, 1);
    assert!(said.iter().any(|m| m.contains("no nix")), "{said:?}");
}

#[tokio::test]
async fn a_declared_key_narrows_what_the_store_trusts() {
    let verifier = Scripted::new(Store::Signed);
    let trust = ClosureTrust::new(
        verifier,
        NativeSecurity::Enforce,
        vec!["mine:BBBB".to_string()],
        ClosureLocalBuilds::Trust,
    );
    let verdict = trust.check(Path::new(CLOSURE)).await;
    assert!(
        verdict
            .refused
            .as_deref()
            .is_some_and(|w| w.contains("fleet")),
        "{verdict:?}"
    );
}

#[tokio::test]
async fn a_closure_of_the_real_store_reads_and_stands() {
    let verifier = Arc::new(engenho_kubelet::closure_trust::NixStoreVerifier::discover(
        Vec::new(),
    ));
    let scratch = std::env::temp_dir().join(format!("engenho-trust-{}", std::process::id()));
    std::fs::write(&scratch, format!("engenho {}", std::process::id())).unwrap();
    let nix_store =
        engenho_kubelet::closure_store::find_nix_tool("ENGENHO_NIX_STORE_BIN", "nix-store")
            .expect("nix-store is required for this test (it fails rather than skips)");
    #[allow(clippy::disallowed_methods)]
    let added = std::process::Command::new(nix_store)
        .arg("--add")
        .arg(&scratch)
        .output()
        .unwrap();
    let _ = std::fs::remove_file(&scratch);
    let closure = String::from_utf8(added.stdout).unwrap();
    let closure = closure.trim();
    let attested = verifier.attest(&format!("nix:{closure}")).await.unwrap();
    assert_eq!(attested.paths.len(), 1, "{attested:?}");
    assert!(attested.paths[0].content_addressed, "{attested:?}");
    assert!(!attested.store_keys.is_empty(), "{attested:?}");
    let trust = ClosureTrust::new(
        verifier,
        NativeSecurity::Enforce,
        Vec::new(),
        ClosureLocalBuilds::Trust,
    );
    assert_eq!(trust.check(Path::new(closure)).await.refused, None);
}
