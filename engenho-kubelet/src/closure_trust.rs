//! Whether a node runs the Nix closure a pod names, decided by the embedded
//! `trust` blue program (`blue/trust/trust.b`).
//!
//! Rust reads the store and checks each signature; blue says which keys count
//! and what a path with no signature means. The check sits in the kubelet, on
//! the path to a pod's first start, because a Nix store is per node, a closure
//! may not be realised before that point, and a controller's write passes no
//! admission.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use base64::Engine as _;
use ed25519_dalek::{Signature, Verifier as _, VerifyingKey};
use engenho_controllers::attestation::{
    Attestation, PathAttestation, SignatureVerifier, VerifierError,
};
use serde::{Deserialize, Serialize};

use crate::blue_worker::BlueWorker;
use crate::image_source::ImageSource;
use crate::kakoi::NativeSecurity;

pub use engenho_config::ClosureLocalBuilds;

const FILES: &[(&str, &str)] = &[("trust/trust.b", include_str!("../blue/trust/trust.b"))];
const ENTRY: &str = "use(\"trust\")\n";

const NIX32_ALPHABET: &[u8; 32] = b"0123456789abcdfghijklmnpqrsvwxyz";

/// What `trust::decide` answered.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct TrustVerdict {
    /// Why this closure may not run.
    pub refused: Option<String>,
    /// What a reader should see.
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Serialize)]
struct Facts<'a> {
    mode: NativeSecurity,
    closure: String,
    keys: &'a [String],
    store_keys: &'a [String],
    local_builds: ClosureLocalBuilds,
    blind: Option<String>,
    paths: &'a [PathAttestation],
}

/// The closure check of one node.
pub struct ClosureTrust {
    verifier: Arc<dyn SignatureVerifier>,
    program: OnceLock<Result<BlueWorker, String>>,
    mode: NativeSecurity,
    keys: Vec<String>,
    local_builds: ClosureLocalBuilds,
}

impl ClosureTrust {
    /// A check through `verifier` for a node in `mode`. An empty `keys` trusts
    /// what the node's store trusts.
    #[must_use]
    pub fn new(
        verifier: Arc<dyn SignatureVerifier>,
        mode: NativeSecurity,
        keys: Vec<String>,
        local_builds: ClosureLocalBuilds,
    ) -> Self {
        Self {
            verifier,
            program: OnceLock::new(),
            mode,
            keys,
            local_builds,
        }
    }

    /// The verdict for `closure`.
    pub async fn check(&self, closure: &Path) -> TrustVerdict {
        if self.mode == NativeSecurity::Off {
            return TrustVerdict::default();
        }
        let image = format!("nix:{}", closure.display());
        let (attestation, blind) = match self.verifier.attest(&image).await {
            Ok(attestation) => (attestation, None),
            Err(e) => (Attestation::default(), Some(e.to_string())),
        };
        let facts = Facts {
            mode: self.mode,
            closure: closure.display().to_string(),
            keys: &self.keys,
            store_keys: &attestation.store_keys,
            local_builds: self.local_builds,
            blind,
            paths: &attestation.paths,
        };
        self.decide(&facts).await
    }

    async fn decide(&self, facts: &Facts<'_>) -> TrustVerdict {
        let answer = match self
            .program
            .get_or_init(|| BlueWorker::start("trust", ENTRY, FILES))
        {
            Ok(program) => match serde_json::to_string(facts) {
                Ok(text) => program.ask("trust::decide", text).await,
                Err(e) => Err(e.to_string()),
            },
            Err(why) => Err(why.clone()),
        };
        answer.unwrap_or_else(|why| undecided(self.mode, &why))
    }
}

fn undecided(mode: NativeSecurity, why: &str) -> TrustVerdict {
    let why = format!("the trust program did not answer: {why}");
    match mode {
        NativeSecurity::Enforce => TrustVerdict {
            refused: Some(why),
            warnings: Vec::new(),
        },
        NativeSecurity::Audit | NativeSecurity::Off => TrustVerdict {
            refused: None,
            warnings: vec![why],
        },
    }
}

/// The signature facts of a node's Nix store, through its `nix` binary.
#[derive(Debug, Clone)]
pub struct NixStoreVerifier {
    nix: Option<PathBuf>,
    keys: Vec<String>,
    timeout: Duration,
}

impl NixStoreVerifier {
    /// How long one store query may take.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

    /// A verifier driving the `nix` binary at `nix`, which also checks each
    /// signature under the node's declared `keys`. `None` reads as a store it
    /// cannot read, so each closure it is asked about is blind.
    #[must_use]
    pub fn new(nix: Option<PathBuf>, keys: Vec<String>) -> Self {
        Self {
            nix,
            keys,
            timeout: Self::DEFAULT_TIMEOUT,
        }
    }

    /// A verifier over the first `nix` this host has.
    #[must_use]
    pub fn discover(keys: Vec<String>) -> Self {
        Self::new(
            crate::closure_store::find_nix_tool("ENGENHO_NIX_BIN", "nix"),
            keys,
        )
    }

    #[allow(
        clippy::disallowed_methods,
        reason = "nix is a node tool, not a pod's process"
    )]
    async fn nix(&self, args: &[&str]) -> Result<String, VerifierError> {
        let nix = self
            .nix
            .as_ref()
            .ok_or_else(|| VerifierError::Backend("no nix found on this node".to_string()))?;
        let mut cmd = tokio::process::Command::new(nix);
        cmd.args(["--extra-experimental-features", "nix-command"])
            .args(args)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true);
        let out = tokio::time::timeout(self.timeout, cmd.output())
            .await
            .map_err(|_| {
                VerifierError::Backend(format!(
                    "nix {} did not finish within {}s",
                    args.join(" "),
                    self.timeout.as_secs()
                ))
            })?
            .map_err(|e| VerifierError::Backend(format!("cannot run {}: {e}", nix.display())))?;
        if !out.status.success() {
            return Err(VerifierError::Backend(
                String::from_utf8_lossy(&out.stderr).trim().to_string(),
            ));
        }
        String::from_utf8(out.stdout).map_err(|e| VerifierError::Backend(e.to_string()))
    }
}

#[async_trait::async_trait]
impl SignatureVerifier for NixStoreVerifier {
    fn name(&self) -> &'static str {
        "nix-store"
    }

    async fn attest(&self, image_ref: &str) -> Result<Attestation, VerifierError> {
        let source =
            ImageSource::parse(image_ref).map_err(|e| VerifierError::InvalidRef(e.to_string()))?;
        let closure = source
            .closure()
            .ok_or_else(|| VerifierError::InvalidRef(format!("{image_ref} is not a closure")))?
            .display()
            .to_string();
        let store_keys =
            parse_trusted_keys(&self.nix(&["config", "show", "trusted-public-keys"]).await?);
        let info = self
            .nix(&["path-info", "--recursive", "--json", &closure])
            .await?;
        let mut candidates = self.keys.clone();
        candidates.extend(store_keys.iter().cloned());
        Ok(Attestation {
            paths: parse_path_info(&info, &candidates).map_err(VerifierError::Backend)?,
            store_keys,
        })
    }
}

fn parse_trusted_keys(text: &str) -> Vec<String> {
    let mut keys: Vec<String> = text.split_whitespace().map(str::to_string).collect();
    keys.sort();
    keys.dedup();
    keys
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PathInfo {
    #[serde(default)]
    path: Option<String>,
    nar_hash: String,
    nar_size: u64,
    #[serde(default)]
    references: Vec<String>,
    #[serde(default)]
    signatures: Vec<String>,
    #[serde(default)]
    ca: Option<serde_json::Value>,
    #[serde(default)]
    ultimate: bool,
}

/// `nix path-info --json`, as an object keyed by path or as a list, with each
/// signature checked under `candidates`.
fn parse_path_info(text: &str, candidates: &[String]) -> Result<Vec<PathAttestation>, String> {
    let value: serde_json::Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
    let rows: Vec<(Option<String>, serde_json::Value)> = match value {
        serde_json::Value::Object(map) => map.into_iter().map(|(k, v)| (Some(k), v)).collect(),
        serde_json::Value::Array(list) => list.into_iter().map(|v| (None, v)).collect(),
        other => return Err(format!("path-info answered {other}")),
    };
    rows.into_iter()
        .map(|(key, row)| {
            let info: PathInfo = serde_json::from_value(row).map_err(|e| e.to_string())?;
            let path = key
                .or_else(|| info.path.clone())
                .ok_or_else(|| "path-info names no path".to_string())?;
            attest_path(path, &info, candidates)
        })
        .collect()
}

fn attest_path(
    path: String,
    info: &PathInfo,
    candidates: &[String],
) -> Result<PathAttestation, String> {
    let print = fingerprint(&path, &info.nar_hash, info.nar_size, &info.references)?;
    let mut claimed = Vec::new();
    let mut verified_by = Vec::new();
    for signature in &info.signatures {
        let Some((name, _)) = signature.split_once(':') else {
            continue;
        };
        claimed.push(name.to_string());
        verified_by.extend(
            candidates
                .iter()
                .filter(|key| signature_verifies(key, signature, print.as_bytes()))
                .cloned(),
        );
    }
    Ok(PathAttestation {
        path,
        ultimate: info.ultimate,
        content_addressed: info.ca.as_ref().is_some_and(|ca| !ca.is_null()),
        claimed,
        verified_by,
    })
}

/// What a Nix store signs for a path.
fn fingerprint(
    path: &str,
    nar_hash: &str,
    nar_size: u64,
    references: &[String],
) -> Result<String, String> {
    let hash = nix32_nar_hash(nar_hash)?;
    let store_root = Path::new(path)
        .parent()
        .ok_or_else(|| format!("{path} has no store root"))?;
    let mut refs: Vec<String> = references
        .iter()
        .map(|r| {
            if r.starts_with('/') {
                r.clone()
            } else {
                store_root.join(r).display().to_string()
            }
        })
        .collect();
    refs.sort();
    Ok(format!("1;{path};{hash};{nar_size};{}", refs.join(",")))
}

/// A NAR hash as a fingerprint carries it: `sha256:` and base-32. Nix prints
/// it either that way or in SRI form.
fn nix32_nar_hash(hash: &str) -> Result<String, String> {
    if hash.starts_with("sha256:") {
        return Ok(hash.to_string());
    }
    let encoded = hash
        .strip_prefix("sha256-")
        .ok_or_else(|| format!("unknown NAR hash form: {hash}"))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|e| format!("NAR hash {hash}: {e}"))?;
    Ok(format!("sha256:{}", nix32(&bytes)))
}

fn nix32(bytes: &[u8]) -> String {
    let len = (bytes.len() * 8).div_ceil(5);
    (0..len)
        .rev()
        .map(|n| {
            let bit = n * 5;
            let (byte, shift) = (bit / 8, bit % 8);
            let low = u16::from(bytes[byte]) >> shift;
            let high = bytes
                .get(byte + 1)
                .map_or(0, |next| u16::from(*next) << (8 - shift));
            char::from(NIX32_ALPHABET[usize::from((low | high) & 0x1f)])
        })
        .collect()
}

/// Whether `signature` (`name:base64`) verifies `message` under `key`
/// (`name:base64`). A key that does not read verifies nothing.
fn signature_verifies(key: &str, signature: &str, message: &[u8]) -> bool {
    let (Some((key_name, key_b64)), Some((signed_by, sig_b64))) =
        (key.split_once(':'), signature.split_once(':'))
    else {
        return false;
    };
    if key_name != signed_by {
        return false;
    }
    let engine = base64::engine::general_purpose::STANDARD;
    let (Ok(key_bytes), Ok(sig_bytes)) = (engine.decode(key_b64), engine.decode(sig_b64)) else {
        return false;
    };
    let (Ok(key_bytes), Ok(sig_bytes)) = (
        <[u8; 32]>::try_from(key_bytes.as_slice()),
        <[u8; 64]>::try_from(sig_bytes.as_slice()),
    ) else {
        return false;
    };
    VerifyingKey::from_bytes(&key_bytes).is_ok_and(|key| {
        key.verify(message, &Signature::from_bytes(&sig_bytes))
            .is_ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const RIO_SUI: &str = "rio-sui-1:L+8OL041A0ivcB0bglTAhREd0NqAlVRxwS4JbugFEKA=";
    const NIXOS: &str = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=";
    const SOURCE_CONTROLLER: &str =
        "/nix/store/d3a6iihn6ia4imdz042q1qnn136yl9di-source-controller-1.9.6";

    fn measured(signature: &str) -> String {
        serde_json::json!({
            SOURCE_CONTROLLER: {
                "ca": null,
                "narHash": "sha256-Ym0yS4ybH/Zv/bB4iopU6tVa8a42JVd5Wz96A0Kl1Dc=",
                "narSize": 101_079_904,
                "references": [
                    "/nix/store/fsrw69w5mx838p7xydbdvywhwl6wnbjw-tzdata-2026e",
                    "/nix/store/i68jrr4ixqmpb5dk5phddx7bnhlwqffs-mailcap-2.1.54",
                    "/nix/store/j1dffl1f3g8wphn77q69k4y4ccw36bzw-iana-etc-20251215",
                ],
                "signatures": [signature],
                "ultimate": false,
            }
        })
        .to_string()
    }

    const MEASURED_SIG_BYTES: &str =
        "a2/mRaxkIq4Zg6BBg3Az8i4JdcSowy/1XKsidCv3R+TE3V1d1H2+Fh3R5JsTM2BO1gtUHJkftB1XPerOI4EMDQ==";

    fn measured_sig() -> String {
        ["rio-sui-1", MEASURED_SIG_BYTES].join(":")
    }

    #[test]
    fn a_signature_measured_on_plo_verifies_under_its_own_key() {
        let paths = parse_path_info(&measured(&measured_sig()), &[RIO_SUI.to_string()]).unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].path, SOURCE_CONTROLLER);
        assert_eq!(paths[0].claimed, ["rio-sui-1"]);
        assert_eq!(paths[0].verified_by, [RIO_SUI]);
        assert!(!paths[0].ultimate);
        assert!(!paths[0].content_addressed);
    }

    #[test]
    fn a_key_of_the_right_name_and_the_wrong_bytes_verifies_nothing() {
        let wrong = format!("rio-sui-1:{}", NIXOS.split_once(':').unwrap().1);
        let paths = parse_path_info(&measured(&measured_sig()), &[wrong]).unwrap();
        assert_eq!(paths[0].claimed, ["rio-sui-1"]);
        assert!(paths[0].verified_by.is_empty());
    }

    #[test]
    fn a_tampered_size_breaks_the_signature() {
        let tampered = measured(&measured_sig()).replace("101079904", "101079905");
        let paths = parse_path_info(&tampered, &[RIO_SUI.to_string()]).unwrap();
        assert!(paths[0].verified_by.is_empty());
    }

    #[test]
    fn a_signature_is_only_checked_under_its_own_name() {
        assert!(!signature_verifies(NIXOS, &measured_sig(), b"anything"));
        assert!(!signature_verifies(
            "no-colon",
            &measured_sig(),
            b"anything"
        ));
        assert!(!signature_verifies(RIO_SUI, "no-colon", b"anything"));
        assert!(!signature_verifies("rio-sui-1:!!", &measured_sig(), b"x"));
        assert!(!signature_verifies("rio-sui-1:AAAA", &measured_sig(), b"x"));
        assert!(!signature_verifies(RIO_SUI, "rio-sui-1:AAAA", b"x"));
    }

    #[test]
    fn the_list_form_and_a_content_address_and_the_local_flag_read() {
        let text = serde_json::json!([{
            "path": "/nix/store/aaa-x",
            "narHash": "sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s",
            "narSize": 1,
            "ca": "fixed:r:sha256:abc",
            "ultimate": true,
        }])
        .to_string();
        let paths = parse_path_info(&text, &[]).unwrap();
        assert_eq!(paths[0].path, "/nix/store/aaa-x");
        assert!(paths[0].content_addressed);
        assert!(paths[0].ultimate);
        assert!(paths[0].claimed.is_empty());
    }

    #[test]
    fn what_does_not_read_is_an_error_not_an_empty_answer() {
        assert!(parse_path_info("not json", &[]).is_err());
        assert!(parse_path_info("3", &[]).is_err());
        assert!(parse_path_info("[{\"narHash\": \"sha256:x\", \"narSize\": 1}]", &[]).is_err());
        assert!(nix32_nar_hash("md5-abc").is_err());
        assert!(nix32_nar_hash("sha256-!!").is_err());
    }

    #[test]
    fn nix32_matches_nix_for_a_known_digest() {
        let empty = base64::engine::general_purpose::STANDARD
            .decode("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=")
            .unwrap();
        assert_eq!(
            nix32(&empty),
            "0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73"
        );
    }

    #[test]
    fn references_are_sorted_and_a_bare_name_takes_the_store_root() {
        let print = fingerprint(
            "/nix/store/aaa-x",
            "sha256:abc",
            1,
            &["ccc-z".to_string(), "/nix/store/bbb-y".to_string()],
        )
        .unwrap();
        assert_eq!(
            print,
            "1;/nix/store/aaa-x;sha256:abc;1;/nix/store/bbb-y,/nix/store/ccc-z"
        );
    }

    #[test]
    fn trusted_keys_are_read_and_deduplicated() {
        assert_eq!(
            parse_trusted_keys(&format!("{NIXOS} {RIO_SUI} {NIXOS}\n")),
            [NIXOS, RIO_SUI]
        );
    }

    #[tokio::test]
    async fn a_node_with_no_nix_is_a_backend_error_naming_it() {
        let v = NixStoreVerifier::new(None, Vec::new());
        assert_eq!(v.name(), "nix-store");
        let err = v.attest("nix:/nix/store/aaa-x").await.unwrap_err();
        assert!(err.to_string().contains("no nix"), "{err}");
    }

    #[tokio::test]
    async fn a_missing_binary_and_an_image_that_is_not_a_closure_say_so() {
        let v = NixStoreVerifier::new(Some(PathBuf::from("/nonexistent/nix")), Vec::new());
        let err = v.attest("nix:/nix/store/aaa-x").await.unwrap_err();
        assert!(err.to_string().contains("/nonexistent/nix"), "{err}");
        assert_eq!(
            v.attest("postgres:16").await.unwrap_err().kind(),
            "invalid_ref"
        );
        assert_eq!(
            v.attest("nix:/tmp/x").await.unwrap_err().kind(),
            "invalid_ref"
        );
    }

    #[test]
    fn a_program_that_did_not_answer_refuses_only_under_enforce() {
        assert!(undecided(NativeSecurity::Enforce, "x").refused.is_some());
        let audit = undecided(NativeSecurity::Audit, "x");
        assert!(audit.refused.is_none());
        assert!(audit.warnings[0].contains("did not answer"));
    }
}
