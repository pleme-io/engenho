use std::path::{Path, PathBuf};

use base64::Engine as _;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use engenho_substrate::WriteOnce;
use openraft::{Entry, EntryPayload, LogId};

use crate::command::{LoggedCommand, ResourceCommand};
use crate::resource::{ResourceKey, ResourceValue};
use crate::type_config::{RaftNodeId, TypeConfig};

/// Every sealed value begins with this; a stored Kubernetes object is a JSON
/// object, so a sealed value and a legacy plaintext one cannot be confused.
pub const ENVELOPE_PREFIX: &str = "engenho-sealed:v1:";
/// The key file's name inside the keys directory.
pub const KEY_FILE: &str = "secrets-at-rest.key";
/// `<store_path><KEYS_DIR_SUFFIX>` is the keys directory, a sibling of the store.
pub const KEYS_DIR_SUFFIX: &str = ".keys";

const SEALED_ENTRY_PREFIX: &[u8] = b"ENGENHO-SEALED-ENTRY:v1\n";
const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 24;
const KEY_ID_DERIVATION: &str = "engenho-store secrets-at-rest key id v1";
const RESOURCE_AAD: &[u8] = b"resource:";
const LOG_AAD: &[u8] = b"log-entry:";

const SEALED_KINDS: &[&str] = &["Secret"];

/// Whether a resource's kind is sealed at rest.
#[must_use]
pub fn is_sealed_kind(key: &ResourceKey) -> bool {
    key.group.is_empty() && SEALED_KINDS.contains(&key.kind.as_str())
}

/// Whether a store seals what it writes. Reading accepts both forms either way.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SealWrites {
    /// Sealed kinds are written encrypted.
    #[default]
    Seal,
    /// Everything is written in plaintext, and sealed values convert back when
    /// next written, if the key can open them.
    Plain,
}

/// The public fingerprint of a secrets key, recorded in each envelope.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyId([u8; 8]);

impl KeyId {
    fn of(key: &[u8; KEY_LEN]) -> Self {
        let derived = blake3::derive_key(KEY_ID_DERIVATION, key);
        let mut id = [0u8; 8];
        id.copy_from_slice(&derived[..8]);
        Self(id)
    }

    fn hex(self) -> String {
        self.0
            .iter()
            .fold(String::with_capacity(16), |mut hex, byte| {
                use std::fmt::Write as _;
                let _ = write!(hex, "{byte:02x}");
                hex
            })
    }

    fn parse(hex: &str) -> Option<Self> {
        if hex.len() != 16 || !hex.is_ascii() {
            return None;
        }
        let mut id = [0u8; 8];
        for (slot, pair) in id.iter_mut().zip(hex.as_bytes().chunks(2)) {
            *slot = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
        }
        Some(Self(id))
    }
}

impl std::fmt::Display for KeyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.hex())
    }
}

impl std::fmt::Debug for KeyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyId({})", self.hex())
    }
}

/// Why a sealed value cannot be opened, or a sealed kind cannot be written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SealFault {
    /// No key file exists, and this store or value was sealed under `wanted`.
    #[error("the secrets key {wanted} is missing; restore {KEY_FILE} from its backup")]
    KeyMissing {
        /// The key the value or store names.
        wanted: KeyId,
    },
    /// The key file exists and cannot be used.
    #[error("the secrets key file is unusable; restore {KEY_FILE} from its backup")]
    KeyUnusable,
    /// The value or store is sealed under another key than this node holds.
    #[error(
        "sealed under secrets key {wanted}, this node holds key {have}; restore the matching {KEY_FILE}"
    )]
    WrongKey {
        /// The key the value or store names.
        wanted: KeyId,
        /// The key this node holds.
        have: KeyId,
    },
    /// The key matches and authentication failed.
    #[error(
        "the sealed value failed authentication under key {key}: damaged, or moved from another resource"
    )]
    Damaged {
        /// The key that tried.
        key: KeyId,
    },
    /// The value was decoded by a store that holds no key at all.
    #[error("the sealed value was not opened by this store")]
    Unopened,
}

engenho_substrate::impl_error_kind! {
    SealFault {
        { KeyMissing { .. } } => "key_missing",
        KeyUnusable => "key_unusable",
        { WrongKey { .. } } => "wrong_key",
        { Damaged { .. } } => "damaged",
        Unopened => "unopened",
    }
}

/// `true` when a stored value is a sealed envelope.
#[must_use]
pub(crate) fn is_envelope(value: &ResourceValue) -> bool {
    value
        .as_str()
        .is_some_and(|text| text.starts_with(ENVELOPE_PREFIX))
}

enum KeyState {
    Held {
        cipher: Box<XChaCha20Poly1305>,
        id: KeyId,
    },
    Absent,
    Unusable,
}

/// One store's secrets key and its write policy.
pub struct SecretSealer {
    writes: SealWrites,
    key: KeyState,
    pinned: Option<KeyId>,
}

/// The keys directory of the store at `store_path`.
#[must_use]
pub fn keys_dir_for(store_path: &Path) -> PathBuf {
    let mut dir = store_path.as_os_str().to_os_string();
    dir.push(KEYS_DIR_SUFFIX);
    dir.into()
}

fn read_key(path: &Path) -> Result<Option<[u8; KEY_LEN]>, ()> {
    match std::fs::read(path) {
        Ok(bytes) => <[u8; KEY_LEN]>::try_from(bytes.as_slice())
            .map(Some)
            .map_err(|_| {
                tracing::error!(path = %path.display(), len = bytes.len(), "the secrets key file has the wrong length");
            }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            tracing::error!(path = %path.display(), %error, "the secrets key file cannot be read");
            Err(())
        }
    }
}

fn generate_key(path: &Path) -> Result<[u8; KEY_LEN], String> {
    let dir = path.parent().ok_or("the key path has no parent")?;
    std::fs::create_dir_all(dir).map_err(|e| format!("mkdir {}: {e}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("chmod {}: {e}", dir.display()))?;
    }
    let fresh = XChaCha20Poly1305::generate_key(&mut OsRng);
    let outcome = engenho_substrate::write_new_atomic_mode(path, fresh.as_slice(), 0o600)
        .map_err(|e| e.to_string())?;
    match outcome {
        WriteOnce::Created => {
            <[u8; KEY_LEN]>::try_from(fresh.as_slice()).map_err(|e| e.to_string())
        }
        WriteOnce::AlreadyExisted => match read_key(path) {
            Ok(Some(key)) => Ok(key),
            _ => Err(format!("{} exists and cannot be used", path.display())),
        },
    }
}

impl SecretSealer {
    /// Load the key for the store at `store_path`. `pinned` is the key the
    /// store recorded when it first held one: while it is set, a missing key
    /// file stays missing, so no new key can strand what the old one sealed.
    #[must_use]
    pub fn load(store_path: &Path, writes: SealWrites, pinned: Option<KeyId>) -> Self {
        let path = keys_dir_for(store_path).join(KEY_FILE);
        let key = match read_key(&path) {
            Ok(Some(key)) => {
                tighten(&path);
                Some(key)
            }
            Err(()) => {
                return Self {
                    writes,
                    key: KeyState::Unusable,
                    pinned,
                };
            }
            Ok(None) if pinned.is_some() || writes == SealWrites::Plain => None,
            Ok(None) => match generate_key(&path) {
                Ok(key) => Some(key),
                Err(error) => {
                    tracing::error!(%error, "the secrets key could not be generated");
                    return Self {
                        writes,
                        key: KeyState::Unusable,
                        pinned,
                    };
                }
            },
        };
        let key = key.map_or(KeyState::Absent, |key| KeyState::Held {
            cipher: Box::new(XChaCha20Poly1305::new(Key::from_slice(&key))),
            id: KeyId::of(&key),
        });
        Self {
            writes,
            key,
            pinned,
        }
    }

    /// The key this store should record, when it holds one and records none.
    #[must_use]
    pub(crate) fn pin_to_record(&self) -> Option<KeyId> {
        match (&self.key, self.pinned) {
            (KeyState::Held { id, .. }, None) => Some(*id),
            _ => None,
        }
    }

    /// Why a sealed kind cannot be written now; `None` when it can.
    #[must_use]
    pub fn write_fault(&self) -> Option<SealFault> {
        if self.writes == SealWrites::Plain {
            return None;
        }
        match (&self.key, self.pinned) {
            (KeyState::Held { id, .. }, Some(wanted)) if *id != wanted => {
                Some(SealFault::WrongKey { wanted, have: *id })
            }
            (KeyState::Held { .. }, _) => None,
            (KeyState::Absent, Some(wanted)) => Some(SealFault::KeyMissing { wanted }),
            (KeyState::Absent | KeyState::Unusable, _) => Some(SealFault::KeyUnusable),
        }
    }

    pub(crate) fn seals(&self) -> bool {
        self.writes == SealWrites::Seal && self.write_fault().is_none()
    }

    fn seal(&self, aad: &[u8], plain: &[u8]) -> Result<String, SealFault> {
        let (KeyState::Held { cipher, id }, None) = (&self.key, self.write_fault()) else {
            return Err(self.write_fault().unwrap_or(SealFault::KeyUnusable));
        };
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let body = cipher
            .encrypt(&nonce, Payload { msg: plain, aad })
            .map_err(|_| SealFault::Damaged { key: *id })?;
        let mut sealed = Vec::with_capacity(NONCE_LEN + body.len());
        sealed.extend_from_slice(nonce.as_slice());
        sealed.extend_from_slice(&body);
        let encoded = base64::engine::general_purpose::STANDARD.encode(sealed);
        Ok(format!("{ENVELOPE_PREFIX}{id}:{encoded}"))
    }

    fn open(&self, aad: &[u8], envelope: &str) -> Result<Vec<u8>, SealFault> {
        let unparsed = || match &self.key {
            KeyState::Held { id, .. } => SealFault::Damaged { key: *id },
            KeyState::Absent | KeyState::Unusable => SealFault::KeyUnusable,
        };
        let rest = envelope
            .strip_prefix(ENVELOPE_PREFIX)
            .ok_or_else(unparsed)?;
        let (id, encoded) = rest.split_once(':').ok_or_else(unparsed)?;
        let wanted = KeyId::parse(id).ok_or_else(unparsed)?;
        let (cipher, have) = match &self.key {
            KeyState::Held { cipher, id } => (cipher, *id),
            KeyState::Absent => return Err(SealFault::KeyMissing { wanted }),
            KeyState::Unusable => return Err(SealFault::KeyUnusable),
        };
        if have != wanted {
            return Err(SealFault::WrongKey { wanted, have });
        }
        let damaged = SealFault::Damaged { key: have };
        let sealed = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| damaged)?;
        if sealed.len() < NONCE_LEN {
            return Err(damaged);
        }
        let (nonce, body) = sealed.split_at(NONCE_LEN);
        cipher
            .decrypt(XNonce::from_slice(nonce), Payload { msg: body, aad })
            .map_err(|_| damaged)
    }

    /// Seal one resource value, bound to `key`.
    ///
    /// # Errors
    ///
    /// The [`SealFault`] that stops this store writing a sealed kind.
    pub fn seal_value(
        &self,
        key: &ResourceKey,
        value: &ResourceValue,
    ) -> Result<ResourceValue, SealFault> {
        let plain = serde_json::to_vec(value).map_err(|_| SealFault::KeyUnusable)?;
        self.seal(&resource_aad(key), &plain)
            .map(ResourceValue::String)
    }

    /// Open one sealed resource value, which must be bound to `key`.
    ///
    /// # Errors
    ///
    /// The [`SealFault`] that names why the value cannot be opened.
    pub fn open_value(
        &self,
        key: &ResourceKey,
        envelope: &str,
    ) -> Result<ResourceValue, SealFault> {
        let plain = self.open(&resource_aad(key), envelope)?;
        serde_json::from_slice(&plain).map_err(|_| self.damaged())
    }

    fn damaged(&self) -> SealFault {
        match &self.key {
            KeyState::Held { id, .. } => SealFault::Damaged { key: *id },
            KeyState::Absent | KeyState::Unusable => SealFault::KeyUnusable,
        }
    }

    /// The bytes a log entry is stored as.
    pub(crate) fn encode_entry(&self, entry: &Entry<TypeConfig>) -> Result<Vec<u8>, EntryRefused> {
        let EntryPayload::Normal(logged) = &entry.payload else {
            return serde_json::to_vec(entry).map_err(EntryRefused::Encode);
        };
        if logged.command().is_sealed_placeholder() {
            return Err(EntryRefused::Placeholder {
                index: entry.log_id.index,
            });
        }
        if self.writes == SealWrites::Plain
            || !logged.command().keys().into_iter().any(is_sealed_kind)
        {
            return serde_json::to_vec(entry).map_err(EntryRefused::Encode);
        }
        let plain = serde_json::to_vec(entry).map_err(EntryRefused::Encode)?;
        let header = serde_json::to_vec(&entry.log_id).map_err(EntryRefused::Encode)?;
        let envelope = self
            .seal(&log_aad(&header), &plain)
            .map_err(EntryRefused::Fault)?;
        let mut record =
            Vec::with_capacity(SEALED_ENTRY_PREFIX.len() + header.len() + 1 + envelope.len());
        record.extend_from_slice(SEALED_ENTRY_PREFIX);
        record.extend_from_slice(&header);
        record.push(b'\n');
        record.extend_from_slice(envelope.as_bytes());
        Ok(record)
    }

    /// Decode a stored log entry. A sealed entry this node cannot open comes
    /// back as a placeholder under its own log id, so raft can still read the
    /// log's shape; [`Self::entry_fault`] names it, and a store refuses to
    /// append or apply the placeholder.
    pub(crate) fn decode_entry(
        &self,
        bytes: &[u8],
    ) -> Result<Entry<TypeConfig>, serde_json::Error> {
        let Some(sealed) = split_sealed_entry(bytes) else {
            return serde_json::from_slice(bytes);
        };
        let (header, envelope) = sealed;
        let log_id: LogId<RaftNodeId> = serde_json::from_slice(header)?;
        match self.open_entry(header, envelope) {
            Ok(plain) => serde_json::from_slice(&plain),
            Err(_) => Ok(Entry {
                log_id,
                payload: EntryPayload::Normal(LoggedCommand::proposed(
                    ResourceCommand::sealed_placeholder(),
                )),
            }),
        }
    }

    /// Why a stored log entry cannot be opened; `None` for a plaintext entry
    /// and for one this node can open.
    pub(crate) fn entry_fault(&self, bytes: &[u8]) -> Option<SealFault> {
        let (header, envelope) = split_sealed_entry(bytes)?;
        self.open_entry(header, envelope).err()
    }

    fn open_entry(&self, header: &[u8], envelope: &[u8]) -> Result<Vec<u8>, SealFault> {
        let envelope = std::str::from_utf8(envelope).map_err(|_| self.damaged())?;
        self.open(&log_aad(header), envelope)
    }
}

/// Why a log entry was not written.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EntryRefused {
    #[error("encode the log entry: {0}")]
    Encode(serde_json::Error),
    #[error("a sealed entry cannot be stored: {0}")]
    Fault(SealFault),
    #[error(
        "log entry {index} is a placeholder for a sealed entry the sending node could not open"
    )]
    Placeholder { index: u64 },
}

fn split_sealed_entry(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let rest = bytes.strip_prefix(SEALED_ENTRY_PREFIX)?;
    let split = rest.iter().position(|byte| *byte == b'\n')?;
    Some((&rest[..split], &rest[split + 1..]))
}

fn resource_aad(key: &ResourceKey) -> Vec<u8> {
    let mut aad = RESOURCE_AAD.to_vec();
    aad.extend_from_slice(&serde_json::to_vec(key).unwrap_or_default());
    aad
}

fn log_aad(header: &[u8]) -> Vec<u8> {
    [LOG_AAD, header].concat()
}

fn tighten(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(error) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            tracing::warn!(path = %path.display(), %error, "the secrets key file keeps its wider mode");
        }
    }
    #[cfg(not(unix))]
    let _ = path;
}

/// Parse a recorded [`KeyId`].
pub(crate) fn parse_key_id(text: &str) -> Option<KeyId> {
    KeyId::parse(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLUE_POLICY: &str = include_str!("../blue/sealed_kinds/sealed_kinds.b");

    fn secret(name: &str) -> ResourceKey {
        ResourceKey::namespaced("", "v1", "Secret", "default", name)
    }

    fn sealer(dir: &Path) -> SecretSealer {
        SecretSealer::load(&dir.join("store"), SealWrites::Seal, None)
    }

    #[test]
    fn the_rust_list_is_the_blue_policy() {
        let declared: Vec<&str> = BLUE_POLICY
            .split("def sealed_kinds()")
            .nth(1)
            .and_then(|body| body.split('[').nth(1))
            .and_then(|tail| tail.split(']').next())
            .expect("the blue word returns a list literal")
            .split(',')
            .map(|name| name.trim().trim_matches('"'))
            .collect();
        assert_eq!(declared, SEALED_KINDS);
    }

    #[test]
    fn only_the_core_group_kinds_in_the_policy_are_sealed() {
        assert!(is_sealed_kind(&secret("a")));
        assert!(!is_sealed_kind(&ResourceKey::namespaced(
            "",
            "v1",
            "ConfigMap",
            "default",
            "a"
        )));
        assert!(!is_sealed_kind(&ResourceKey::namespaced(
            "example.io",
            "v1",
            "Secret",
            "default",
            "a"
        )));
    }

    #[test]
    fn a_value_round_trips_and_cannot_move_to_another_key() {
        let tmp = tempfile::tempdir().unwrap();
        let sealer = sealer(tmp.path());
        let value = serde_json::json!({"data": {"k": "v"}});
        let stored = sealer.seal_value(&secret("a"), &value).unwrap();
        let envelope = stored.as_str().unwrap();
        assert!(is_envelope(&stored));
        assert_eq!(sealer.open_value(&secret("a"), envelope).unwrap(), value);
        let moved = sealer.open_value(&secret("b"), envelope).unwrap_err();
        assert_eq!(moved.kind(), "damaged");
    }

    #[test]
    fn two_seals_of_one_value_differ() {
        let tmp = tempfile::tempdir().unwrap();
        let sealer = sealer(tmp.path());
        let value = serde_json::json!({"data": {"k": "v"}});
        assert_ne!(
            sealer.seal_value(&secret("a"), &value).unwrap(),
            sealer.seal_value(&secret("a"), &value).unwrap()
        );
    }

    #[test]
    fn a_tampered_envelope_is_damaged() {
        let tmp = tempfile::tempdir().unwrap();
        let sealer = sealer(tmp.path());
        let stored = sealer
            .seal_value(&secret("a"), &serde_json::json!({"data": {}}))
            .unwrap();
        let mut envelope = stored.as_str().unwrap().to_owned();
        let last = envelope.pop().unwrap();
        envelope.push(if last == 'A' { 'B' } else { 'A' });
        assert_eq!(
            sealer
                .open_value(&secret("a"), &envelope)
                .unwrap_err()
                .kind(),
            "damaged"
        );
    }

    #[test]
    fn the_key_file_is_private_and_generated_once() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let path = keys_dir_for(&store).join(KEY_FILE);
        let first = SecretSealer::load(&store, SealWrites::Seal, None);
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), KEY_LEN);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let file = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            let dir = std::fs::metadata(keys_dir_for(&store))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!((file, dir), (0o600, 0o700));
        }
        let second = SecretSealer::load(&store, SealWrites::Seal, first.pin_to_record());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert!(second.seals());
    }

    #[test]
    fn a_pinned_store_never_generates_a_key_to_replace_a_missing_one() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let first = SecretSealer::load(&store, SealWrites::Seal, None);
        let pinned = first.pin_to_record().unwrap();
        let path = keys_dir_for(&store).join(KEY_FILE);
        std::fs::remove_file(&path).unwrap();
        let second = SecretSealer::load(&store, SealWrites::Seal, Some(pinned));
        assert!(!path.exists(), "a missing key was regenerated");
        assert_eq!(
            second.write_fault(),
            Some(SealFault::KeyMissing { wanted: pinned })
        );
    }

    #[test]
    fn a_malformed_key_file_is_unusable_and_left_as_found() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let path = keys_dir_for(&store).join(KEY_FILE);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"short").unwrap();
        let sealer = SecretSealer::load(&store, SealWrites::Seal, None);
        assert_eq!(sealer.write_fault(), Some(SealFault::KeyUnusable));
        assert_eq!(std::fs::read(&path).unwrap(), b"short");
    }

    #[test]
    fn plain_writes_generate_no_key() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let sealer = SecretSealer::load(&store, SealWrites::Plain, None);
        assert!(sealer.write_fault().is_none());
        assert!(!sealer.seals());
        assert!(!keys_dir_for(&store).exists());
    }
}
