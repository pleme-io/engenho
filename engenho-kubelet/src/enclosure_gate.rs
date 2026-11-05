//! The verdict of [`crate::kakoi`] for a backend that has no enclosure of its
//! own.
//!
//! A container runtime runs what it is told, so the gate stands in front of
//! it: a container whose spec asks for root or a capability reaches the runtime
//! only with what `kakoi::enclose` answers for a native pod. Only the refusal is
//! taken, because the runtime gives a container its own account and namespaces.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;

use crate::backend::{
    Confinement, ContainerRuntime, ContainerSpec, ContainerStatus, ExecOutcome, LogOptions,
    LogRead, Readoption,
};
use crate::error::KubeletError;
use crate::kakoi::{Kakoi, NativeSecurity, Verdict};

/// A runtime whose starts pass `kakoi::enclose` first.
pub struct EnclosureGate {
    inner: Arc<dyn ContainerRuntime>,
    mode: NativeSecurity,
    kakoi: OnceLock<Result<Kakoi, String>>,
}

impl EnclosureGate {
    /// `inner`, gated for a node in `mode`.
    #[must_use]
    pub fn new(inner: Arc<dyn ContainerRuntime>, mode: NativeSecurity) -> Self {
        Self {
            inner,
            mode,
            kakoi: OnceLock::new(),
        }
    }

    /// `None` for a spec that declares nothing: there is nothing to decide, so
    /// a program that failed to load refuses only what it decides.
    async fn verdict(&self, spec: &ContainerSpec) -> Option<Result<Verdict, String>> {
        if declares_nothing(&spec.confinement) {
            return None;
        }
        let kakoi = match self.kakoi.get_or_init(Kakoi::start) {
            Ok(kakoi) => kakoi,
            Err(why) => return Some(Err(why.clone())),
        };
        Some(
            kakoi
                .enclose(&spec.confinement, &spec.pod.namespace, &[], self.mode, true)
                .await,
        )
    }
}

fn declares_nothing(c: &Confinement) -> bool {
    *c == Confinement::default()
}

fn refusal(mode: NativeSecurity, verdict: Option<Result<Verdict, String>>) -> Option<String> {
    match (verdict?, mode) {
        (Ok(verdict), _) => verdict.refused,
        (Err(_), NativeSecurity::Off | NativeSecurity::Audit) => None,
        (Err(why), NativeSecurity::Enforce) => {
            Some(format!("the kakoi program did not answer: {why}"))
        }
    }
}

fn warnings(mode: NativeSecurity, verdict: Option<Result<Verdict, String>>) -> Vec<String> {
    match (verdict, mode) {
        (Some(Ok(verdict)), NativeSecurity::Audit) => verdict.warnings,
        (Some(Err(why)), NativeSecurity::Audit) => {
            vec![format!("the kakoi program did not answer: {why}")]
        }
        _ => Vec::new(),
    }
}

#[async_trait]
impl ContainerRuntime for EnclosureGate {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn version(&self) -> Option<String> {
        self.inner.version()
    }

    fn readoption(&self) -> Readoption {
        self.inner.readoption()
    }

    fn readoption_of(&self, runtime_class: Option<&str>) -> Readoption {
        self.inner.readoption_of(runtime_class)
    }

    async fn confinement_warnings(&self, spec: &ContainerSpec) -> Vec<String> {
        let mut out = warnings(self.mode, self.verdict(spec).await);
        out.extend(self.inner.confinement_warnings(spec).await);
        out
    }

    fn resource_enforcement(&self) -> crate::cgroup::ResourceEnforcement {
        self.inner.resource_enforcement()
    }

    async fn exec(&self, container_id: &str, argv: &[String]) -> Result<ExecOutcome, KubeletError> {
        self.inner.exec(container_id, argv).await
    }

    async fn exec_stream(
        &self,
        container_id: &str,
        req: &crate::stream::StreamRequest,
    ) -> Result<crate::stream::StreamSession, KubeletError> {
        self.inner.exec_stream(container_id, req).await
    }

    async fn attach(
        &self,
        container_id: &str,
        req: &crate::stream::StreamRequest,
    ) -> Result<crate::stream::StreamSession, KubeletError> {
        self.inner.attach(container_id, req).await
    }

    #[allow(clippy::disallowed_methods)]
    async fn start(&self, spec: &ContainerSpec) -> Result<ContainerStatus, KubeletError> {
        if let Some(why) = refusal(self.mode, self.verdict(spec).await) {
            return Err(KubeletError::Backend(format!(
                "{} backend: {why}",
                self.inner.name()
            )));
        }
        self.inner.start(spec).await
    }

    async fn status(&self, container_id: &str) -> Result<Option<ContainerStatus>, KubeletError> {
        self.inner.status(container_id).await
    }

    async fn stop(&self, container_id: &str) -> Result<(), KubeletError> {
        self.inner.stop(container_id).await
    }

    async fn remove(&self, container_id: &str) -> Result<(), KubeletError> {
        self.inner.remove(container_id).await
    }

    async fn logs(&self, container_id: &str, opts: &LogOptions) -> Result<String, KubeletError> {
        self.inner.logs(container_id, opts).await
    }

    async fn usage(&self, container_id: &str) -> Result<crate::usage::Reading, KubeletError> {
        self.inner.usage(container_id).await
    }

    async fn read_log(
        &self,
        container_id: &str,
        opts: &LogOptions,
    ) -> Result<LogRead, KubeletError> {
        self.inner.read_log(container_id, opts).await
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::backend::{FakeBackend, FakeEvent};

    fn spec(confinement: Confinement) -> ContainerSpec {
        ContainerSpec {
            name: "default/p/c".to_string(),
            image: "img".to_string(),
            confinement,
            ..ContainerSpec::default()
        }
    }

    fn privileged(grant: Option<&str>) -> Confinement {
        Confinement {
            privileged: true,
            root_grant: grant.map(str::to_string),
            ..Confinement::default()
        }
    }

    fn gate(mode: NativeSecurity) -> (FakeBackend, EnclosureGate) {
        let fake = FakeBackend::new();
        let gate = EnclosureGate::new(Arc::new(fake.clone()), mode);
        (fake, gate)
    }

    async fn started(fake: &FakeBackend) -> usize {
        fake.events()
            .await
            .iter()
            .filter(|e| matches!(e, FakeEvent::Start(_)))
            .count()
    }

    #[tokio::test]
    async fn a_privileged_pod_with_no_grant_is_refused_by_name_and_never_reaches_the_runtime() {
        let (fake, gate) = gate(NativeSecurity::Enforce);
        let shown = gate
            .start(&spec(privileged(None)))
            .await
            .expect_err("refused")
            .to_string();
        assert!(shown.contains("privileged"), "{shown}");
        assert!(shown.contains(crate::kakoi::GRANT_ANNOTATION), "{shown}");
        assert_eq!(started(&fake).await, 0);
    }

    #[tokio::test]
    async fn an_added_capability_with_no_grant_is_refused() {
        let (fake, gate) = gate(NativeSecurity::Enforce);
        let c = Confinement {
            cap_add: vec!["NET_ADMIN".to_string()],
            ..Confinement::default()
        };
        assert!(gate.start(&spec(c)).await.is_err());
        assert_eq!(started(&fake).await, 0);
    }

    #[tokio::test]
    async fn a_grant_passes_what_the_pod_declares_to_the_runtime() {
        let (fake, gate) = gate(NativeSecurity::Enforce);
        let id = gate
            .start(&spec(privileged(Some("needs the host"))))
            .await
            .expect("granted")
            .container_id;
        assert_eq!(started(&fake).await, 1);
        assert!(fake.spec_of(&id).await.unwrap().confinement.privileged);
        assert!(
            gate.confinement_warnings(&spec(privileged(Some("needs the host"))))
                .await
                .is_empty(),
            "a grant under enforce has nothing to warn of: podman honours the fields"
        );
    }

    #[tokio::test]
    async fn a_pod_that_says_nothing_reaches_the_runtime() {
        let (fake, gate) = gate(NativeSecurity::Enforce);
        gate.start(&spec(Confinement::default()))
            .await
            .expect("nothing to refuse");
        assert_eq!(started(&fake).await, 1);
    }

    #[tokio::test]
    async fn audit_starts_it_and_names_what_enforce_would_refuse() {
        let (fake, gate) = gate(NativeSecurity::Audit);
        let wanted = spec(privileged(None));
        let warned = gate.confinement_warnings(&wanted).await;
        assert!(
            warned.iter().any(|w| w.contains("would refuse")),
            "{warned:?}"
        );
        gate.start(&wanted).await.expect("audit does not refuse");
        assert_eq!(started(&fake).await, 1);
    }

    #[tokio::test]
    async fn off_starts_it_and_says_nothing() {
        let (fake, gate) = gate(NativeSecurity::Off);
        let wanted = spec(privileged(None));
        assert!(gate.confinement_warnings(&wanted).await.is_empty());
        gate.start(&wanted).await.expect("off does not refuse");
        assert_eq!(started(&fake).await, 1);
    }

    #[test]
    fn a_program_that_did_not_answer_refuses_only_under_enforce() {
        let failed = || Some(Err("no program".to_string()));
        assert!(
            refusal(NativeSecurity::Enforce, failed())
                .expect("refused")
                .contains("no program")
        );
        assert_eq!(refusal(NativeSecurity::Audit, failed()), None);
        assert_eq!(refusal(NativeSecurity::Off, failed()), None);
        assert_eq!(refusal(NativeSecurity::Enforce, None), None);
        assert!(warnings(NativeSecurity::Audit, failed())[0].contains("no program"));
        assert!(warnings(NativeSecurity::Enforce, failed()).is_empty());
        assert!(warnings(NativeSecurity::Off, failed()).is_empty());
    }

    #[tokio::test]
    async fn every_other_call_reaches_the_runtime_behind_it() {
        let fake = FakeBackend::new().with_readoption(Readoption::AdoptsRunning);
        let gate = EnclosureGate::new(Arc::new(fake.clone()), NativeSecurity::Enforce);
        assert_eq!(gate.name(), fake.name());
        assert_eq!(gate.version(), fake.version());
        assert_eq!(gate.readoption(), Readoption::AdoptsRunning);
        assert_eq!(gate.readoption_of(None), Readoption::AdoptsRunning);
        assert_eq!(gate.resource_enforcement(), fake.resource_enforcement());

        let id = gate
            .start(&spec(Confinement::default()))
            .await
            .unwrap()
            .container_id;
        assert_eq!(
            gate.status(&id).await.unwrap().unwrap().container_id,
            id.clone()
        );
        let opts = LogOptions::default();
        assert_eq!(
            gate.logs(&id, &opts).await.unwrap(),
            fake.logs(&id, &opts).await.unwrap()
        );
        assert_eq!(
            gate.read_log(&id, &opts).await.unwrap().text,
            fake.read_log(&id, &opts).await.unwrap().text
        );
        assert_eq!(gate.exec(&id, &[]).await.unwrap().exit_code, 0);
        assert!(gate.usage(&id).await.is_ok());
        let req = crate::stream::StreamRequest::default();
        assert_eq!(
            gate.exec_stream(&id, &req)
                .await
                .err()
                .map(|e| e.to_string()),
            fake.exec_stream(&id, &req)
                .await
                .err()
                .map(|e| e.to_string()),
        );
        assert_eq!(
            gate.attach(&id, &req).await.err().map(|e| e.to_string()),
            fake.attach(&id, &req).await.err().map(|e| e.to_string()),
        );
        gate.stop(&id).await.unwrap();
        gate.remove(&id).await.unwrap();
        assert!(fake.status(&id).await.unwrap().is_none());
    }
}
