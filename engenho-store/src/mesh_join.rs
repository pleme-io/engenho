use engenho_control_types::pin::Spki;
use serde::{Deserialize, Serialize};

use crate::network::LearnerVerdict;
use crate::type_config::RaftNodeId;

const REQUEST: &[u8] = b"engenho mesh join request v1";
const ANSWER: &[u8] = b"engenho mesh join answer v1";

#[derive(Clone)]
pub struct JoinKey([u8; 32]);

impl std::fmt::Debug for JoinKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JoinKey(..)")
    }
}

impl JoinKey {
    pub fn derive(cluster_secret: &[u8]) -> Self {
        Self(blake3::derive_key(
            "engenho mesh join key v1",
            cluster_secret,
        ))
    }

    fn mac(&self, label: &[u8], body: &[u8]) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new_keyed(&self.0);
        hasher.update(&(label.len() as u64).to_be_bytes());
        hasher.update(label);
        hasher.update(body);
        hasher.finalize()
    }

    fn seal<T: Serialize>(&self, label: &[u8], value: &T) -> Result<Sealed, serde_json::Error> {
        let body = serde_json::to_string(value)?;
        let mac = self.mac(label, body.as_bytes()).to_hex().to_string();
        Ok(Sealed { body, mac })
    }

    fn open<T: for<'de> Deserialize<'de>>(&self, label: &[u8], sealed: &Sealed) -> Option<T> {
        let expected = self.mac(label, sealed.body.as_bytes());
        let presented = blake3::Hash::from_hex(&sealed.mac).ok()?;
        (expected == presented)
            .then(|| serde_json::from_str(&sealed.body).ok())
            .flatten()
    }
}

#[derive(Clone, Debug)]
pub struct JoinGate {
    pub cluster: String,
    pub key: JoinKey,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sealed {
    body: String,
    mac: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JoinRequest {
    pub(crate) cluster: String,
    pub(crate) node_id: RaftNodeId,
    pub(crate) addr: String,
    pub(crate) nonce: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JoinAnswer {
    pub(crate) nonce: String,
    pub(crate) seed: Spki,
    pub(crate) verdict: LearnerVerdict,
}

impl JoinGate {
    pub(crate) fn seal_request(&self, request: &JoinRequest) -> Result<Sealed, serde_json::Error> {
        self.key.seal(REQUEST, request)
    }

    pub(crate) fn open_request(&self, sealed: &Sealed) -> Option<JoinRequest> {
        self.key.open(REQUEST, sealed)
    }

    pub(crate) fn seal_answer(&self, answer: &JoinAnswer) -> Result<Sealed, serde_json::Error> {
        self.key.seal(ANSWER, answer)
    }

    pub(crate) fn open_answer(&self, sealed: &Sealed) -> Option<JoinAnswer> {
        self.key.open(ANSWER, sealed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> JoinRequest {
        JoinRequest {
            cluster: "aldeia".into(),
            node_id: 1 << 52,
            addr: "127.0.0.1:7946".into(),
            nonce: "00".into(),
        }
    }

    #[test]
    fn a_sealed_request_opens_only_under_the_key_and_label_it_was_sealed_with() {
        let gate = JoinGate {
            cluster: "aldeia".into(),
            key: JoinKey::derive(b"the cluster CA key"),
        };
        let other = JoinGate {
            cluster: "aldeia".into(),
            key: JoinKey::derive(b"another CA key"),
        };
        let sealed = gate.seal_request(&request()).unwrap();
        assert_eq!(gate.open_request(&sealed), Some(request()));
        assert_eq!(other.open_request(&sealed), None);
        let as_answer: Option<JoinAnswer> = gate.key.open(ANSWER, &sealed);
        assert_eq!(as_answer, None);
        let mut tampered = sealed.clone();
        tampered.body = tampered.body.replace("aldeia", "aldeib");
        assert_eq!(gate.open_request(&tampered), None);
        let mut garbage = sealed;
        garbage.mac = "not hex".into();
        assert_eq!(gate.open_request(&garbage), None);
    }
}
