//! The clients a remote listener admits: each pin with the name and the tier
//! the SERVER declared for it. A certificate never carries a tier.
//!
//! The set is swapped whole ([`Pins::replace`]) when the declared file
//! changes, and every handshake reads it afresh, so revoking a client is
//! removing its pin — the next connection is refused.

use std::collections::BTreeMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use engenho_config::{AuthorizedClientConfig, RemoteTier};
use engenho_control_types::AuthorityTier;
use engenho_control_types::pin::{PinSet, Spki};
use engenho_control_types::types;

/// One admitted client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Client {
    /// Its name, as the server declared it.
    pub name: String,
    /// Its pin.
    pub spki: Spki,
    /// What it may do.
    pub tier: AuthorityTier,
}

/// Every admitted client, by pin.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthorizedSet {
    by_spki: BTreeMap<Spki, Client>,
}

impl AuthorizedSet {
    /// The set the configuration declares.
    ///
    /// # Errors
    ///
    /// A pin that does not parse (the configuration's own validation says
    /// the same, earlier).
    pub fn from_config(clients: &[AuthorizedClientConfig]) -> Result<Self, String> {
        let by_spki = clients
            .iter()
            .map(|c| {
                let spki = Spki::parse(&c.spki_sha256).map_err(|e| e.to_string())?;
                let tier = match c.tier {
                    RemoteTier::Observe => AuthorityTier::Observe,
                    RemoteTier::Mutate => AuthorityTier::Mutate,
                    RemoteTier::Destructive => AuthorityTier::Destructive,
                };
                Ok((
                    spki,
                    Client {
                        name: c.name.clone(),
                        spki,
                        tier,
                    },
                ))
            })
            .collect::<Result<_, String>>()?;
        Ok(Self { by_spki })
    }

    /// The client holding `spki`.
    #[must_use]
    pub fn get(&self, spki: &Spki) -> Option<&Client> {
        self.by_spki.get(spki)
    }

    /// Whether no client is admitted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_spki.is_empty()
    }

    /// The set as the control API lists it, by name.
    #[must_use]
    pub fn views(&self) -> Vec<types::AuthorizedClient> {
        let mut views: Vec<types::AuthorizedClient> = self
            .by_spki
            .values()
            .filter_map(|c| {
                Some(types::AuthorizedClient {
                    name: c.name.clone(),
                    spki: types::SpkiSha256::try_from(c.spki.to_string()).ok()?,
                    tier: c.tier,
                })
            })
            .collect();
        views.sort_by(|a, b| a.name.cmp(&b.name));
        views
    }
}

/// The live set a listener reads per handshake. Cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct Pins(Arc<ArcSwap<AuthorizedSet>>);

impl Pins {
    /// A live set starting as `set`.
    #[must_use]
    pub fn new(set: AuthorizedSet) -> Self {
        Self(Arc::new(ArcSwap::from_pointee(set)))
    }

    /// The set now.
    #[must_use]
    pub fn current(&self) -> Arc<AuthorizedSet> {
        self.0.load_full()
    }

    /// Replace the whole set: the next handshake sees the new one.
    pub fn replace(&self, set: AuthorizedSet) {
        self.0.store(Arc::new(set));
    }

    /// The client holding `spki`, now.
    #[must_use]
    pub fn client(&self, spki: &Spki) -> Option<Client> {
        self.0.load().get(spki).cloned()
    }
}

impl PinSet for Pins {
    fn admits(&self, spki: &Spki) -> bool {
        self.0.load().get(spki).is_some()
    }
}

#[cfg(test)]
mod tests {
    use engenho_control_types::pin::KeyMaterial;

    use super::*;

    fn config(name: &str, spki: Spki, tier: RemoteTier) -> AuthorizedClientConfig {
        AuthorizedClientConfig {
            name: name.into(),
            spki_sha256: spki.to_string(),
            tier,
        }
    }

    #[test]
    fn a_pin_admits_its_client_at_the_declared_tier_and_revoking_it_takes_effect() {
        let laptop = KeyMaterial::generate().unwrap().spki();
        let ci = KeyMaterial::generate().unwrap().spki();
        let pins = Pins::new(
            AuthorizedSet::from_config(&[
                config("laptop", laptop, RemoteTier::Destructive),
                config("ci", ci, RemoteTier::Observe),
            ])
            .unwrap(),
        );
        assert!(pins.admits(&laptop));
        assert_eq!(pins.client(&ci).unwrap().tier, AuthorityTier::Observe);
        let views = pins.current().views();
        assert_eq!(
            views.iter().map(|v| v.name.as_str()).collect::<Vec<_>>(),
            ["ci", "laptop"]
        );

        pins.replace(AuthorizedSet::from_config(&[config("ci", ci, RemoteTier::Observe)]).unwrap());
        assert!(!pins.admits(&laptop), "revoked");
        assert!(pins.admits(&ci));
    }

    #[test]
    fn a_bad_pin_is_refused() {
        let bad = AuthorizedClientConfig {
            name: "x".into(),
            spki_sha256: "sha256:nope".into(),
            tier: RemoteTier::Observe,
        };
        assert!(AuthorizedSet::from_config(&[bad]).is_err());
    }
}
