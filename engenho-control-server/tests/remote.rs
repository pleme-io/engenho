//! The remote listener, in process: a pinned client is served at the tier the
//! server declared for it; everything else fails at the handshake — an
//! unknown key, a pinned certificate presented without its private key, and
//! (from the client's side) a server whose pin the client does not hold. A
//! revoked pin is refused on the next request, open connection or not.

use std::sync::Arc;
use std::time::{Duration, Instant};

use engenho_config::{
    AuthorizedClientConfig, GroupTier, RemoteControlConfig, RemoteTier, SocketAccess,
};
use engenho_control_client::{ClientError, ControlClient, RemoteEndpoint};
use engenho_control_server::{
    Absence, AuthorizedSet, ControlIdentity, GrantPolicy, NoAudit, Pins, RemoteListener,
    RemoteState, Router, daemon_euid,
};
use engenho_control_types::ControlError;
use engenho_control_types::ops::{
    Hello, HelloRequest, StartRuntime, StartRuntimeRequest, Unserved,
};
use engenho_control_types::pin::{CERT_NAME, KeyMaterial, client_config};
use engenho_control_types::types::RefusalReason;
use engenho_serve::stop_channel;

fn authorized(name: &str, key: &KeyMaterial, tier: RemoteTier) -> AuthorizedClientConfig {
    AuthorizedClientConfig {
        name: name.into(),
        spki_sha256: key.spki().to_string(),
        tier,
    }
}

async fn serving(listener: &RemoteListener) -> std::net::SocketAddr {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let RemoteState::Serving { addr, .. } = listener.state() {
            return addr;
        }
        assert!(
            Instant::now() < deadline,
            "never served: {:?}",
            listener.state()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn refusal(err: &ClientError) -> Option<RefusalReason> {
    match err {
        ClientError::Control(ControlError::Refused(r)) => Some(r.reason),
        _ => None,
    }
}

/// A client presenting `cert`'s certificate but signing with `signer`'s key.
#[derive(Debug)]
struct Copied(Arc<rustls::sign::CertifiedKey>);

impl rustls::client::ResolvesClientCert for Copied {
    fn resolve(
        &self,
        _: &[&[u8]],
        _: &[rustls::SignatureScheme],
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(Arc::clone(&self.0))
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// `pinned`'s certificate presented with `other`'s key: the handshake
/// signature does not verify, so it never gets through.
async fn a_copied_certificate_does_not_get_through(
    endpoint: &RemoteEndpoint,
    pinned: &KeyMaterial,
    other: &KeyMaterial,
) {
    let mut copied = client_config(other, endpoint.server_spki.clone()).expect("tls");
    let provider = rustls::crypto::ring::default_provider();
    let (pinned_cert, _) = pinned.certificate(CERT_NAME).expect("cert");
    let (_, other_key) = other.certificate(CERT_NAME).expect("cert");
    let signer = provider
        .key_provider
        .load_private_key(other_key)
        .expect("signing key");
    copied.client_auth_cert_resolver = Arc::new(Copied(Arc::new(rustls::sign::CertifiedKey::new(
        vec![pinned_cert],
        signer,
    ))));
    let http = reqwest::Client::builder()
        .use_preconfigured_tls(copied)
        .build()
        .expect("client");
    let sent = http
        .get(["https://", &endpoint.address, "/v1/hello"].concat())
        .send()
        .await;
    assert!(sent.is_err(), "a copied certificate got through: {sent:?}");
}

#[tokio::test]
async fn only_pinned_clients_get_through_and_only_at_their_tier() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let identity =
        Arc::new(ControlIdentity::load_or_create(&tmp.path().join("identity")).expect("identity"));
    let observer = KeyMaterial::generate().expect("key");
    let stranger = KeyMaterial::generate().expect("key");

    let config = RemoteControlConfig {
        enable: true,
        listen_addr: "127.0.0.1:0".into(),
        authorized_clients: vec![authorized("observer", &observer, RemoteTier::Observe)],
    };
    let pins = Pins::new(AuthorizedSet::from_config(&config.authorized_clients).expect("pins"));
    let router = Arc::new(Router::new(
        Arc::new(Unserved),
        GrantPolicy::new(daemon_euid(), SocketAccess::Owner, GroupTier::Observe),
        Arc::new(NoAudit),
    ));
    let (stop, signal) = stop_channel();
    let listener = RemoteListener::spawn(
        &config,
        Ok(Arc::clone(&identity)),
        pins.clone(),
        router,
        signal,
        RemoteListener::channel().0,
    );
    let addr = serving(&listener).await;
    let endpoint = RemoteEndpoint {
        address: addr.to_string(),
        server_spki: vec![identity.spki()],
        key: None,
    };

    // The pinned observer gets through: the daemon's own answer (here, the
    // refuse-everything `Unserved`) is what comes back, not a transport error.
    let client = ControlClient::remote(&endpoint, &observer).expect("client");
    let err = client
        .call::<Hello>(&HelloRequest {})
        .await
        .expect_err("Unserved");
    assert_eq!(refusal(&err), Some(RefusalReason::Unsupported), "{err}");
    // ...at the tier the server declared for its pin, never more.
    let err = client
        .call::<StartRuntime>(&StartRuntimeRequest {})
        .await
        .expect_err("observe only");
    assert_eq!(
        refusal(&err),
        Some(RefusalReason::InsufficientAuthority),
        "{err}"
    );

    // An unknown key never completes the handshake.
    let err = ControlClient::remote(&endpoint, &stranger)
        .expect("client")
        .call::<Hello>(&HelloRequest {})
        .await
        .expect_err("not pinned");
    assert!(matches!(err, ClientError::Unreachable { .. }), "{err}");

    a_copied_certificate_does_not_get_through(&endpoint, &observer, &stranger).await;

    // The client refuses a server whose pin it does not hold.
    let impostor = RemoteEndpoint {
        server_spki: vec![stranger.spki()],
        ..endpoint.clone()
    };
    let err = ControlClient::remote(&impostor, &observer)
        .expect("client")
        .call::<Hello>(&HelloRequest {})
        .await
        .expect_err("server not pinned");
    assert!(matches!(err, ClientError::Unreachable { .. }), "{err}");

    // Revoked: refused on the next request over the connection already open
    // (the client pools it), because the pin is looked up per request.
    pins.replace(AuthorizedSet::default());
    let err = client
        .call::<Hello>(&HelloRequest {})
        .await
        .expect_err("revoked");
    assert_eq!(
        refusal(&err),
        Some(RefusalReason::InsufficientAuthority),
        "{err}"
    );
    // A new connection does not get past the handshake at all.
    let err = ControlClient::remote(&endpoint, &observer)
        .expect("client")
        .call::<Hello>(&HelloRequest {})
        .await
        .expect_err("revoked");
    assert!(matches!(err, ClientError::Unreachable { .. }), "{err}");

    stop.stop();
    listener.stopped().await;
}

/// A rotated identity is presented from the next handshake, while the
/// listener serves: a client pinned to the old key is refused by its own
/// verifier, a client pinned to the new one gets through.
#[tokio::test]
async fn a_rotated_identity_is_presented_without_a_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let identity =
        Arc::new(ControlIdentity::load_or_create(&tmp.path().join("identity")).expect("identity"));
    let operator = KeyMaterial::generate().expect("key");
    let config = RemoteControlConfig {
        enable: true,
        listen_addr: "127.0.0.1:0".into(),
        authorized_clients: vec![authorized("operator", &operator, RemoteTier::Observe)],
    };
    let (stop, signal) = stop_channel();
    let listener = RemoteListener::spawn(
        &config,
        Ok(Arc::clone(&identity)),
        Pins::new(AuthorizedSet::from_config(&config.authorized_clients).expect("pins")),
        Arc::new(Router::new(
            Arc::new(Unserved),
            GrantPolicy::new(daemon_euid(), SocketAccess::Owner, GroupTier::Observe),
            Arc::new(NoAudit),
        )),
        signal,
        RemoteListener::channel().0,
    );
    let addr = serving(&listener).await.to_string();
    let pinned_to = |spki| RemoteEndpoint {
        address: addr.clone(),
        server_spki: vec![spki],
        key: None,
    };
    let reaches = |endpoint: RemoteEndpoint| {
        let operator = &operator;
        async move {
            let err = ControlClient::remote(&endpoint, operator)
                .expect("client")
                .call::<Hello>(&HelloRequest {})
                .await
                .expect_err("Unserved refuses everything");
            refusal(&err).is_some()
        }
    };
    let old = identity.spki();
    assert!(
        reaches(pinned_to(old)).await,
        "the first key was not served"
    );

    let new = identity
        .rotate(&tmp.path().join("attic").join("key.pem"))
        .expect("rotated");

    assert_ne!(new, old);
    assert!(
        !reaches(pinned_to(old)).await,
        "the old key is still presented"
    );
    assert!(
        reaches(pinned_to(new)).await,
        "the new key is not presented"
    );

    stop.stop();
    listener.stopped().await;
}

#[tokio::test]
async fn what_keeps_it_from_serving_is_its_state() {
    let (stop, signal) = stop_channel();
    let router = Arc::new(Router::new(
        Arc::new(Unserved),
        GrantPolicy::new(daemon_euid(), SocketAccess::Owner, GroupTier::Observe),
        Arc::new(NoAudit),
    ));
    let absent = |config: &RemoteControlConfig, identity| match RemoteListener::spawn(
        config,
        identity,
        Pins::new(AuthorizedSet::from_config(&config.authorized_clients).unwrap_or_default()),
        Arc::clone(&router),
        signal.clone(),
        RemoteListener::channel().0,
    )
    .state()
    {
        RemoteState::Absent { absence, .. } => absence,
        RemoteState::Serving { .. } => panic!("served"),
    };
    let key = KeyMaterial::generate().expect("key");
    let enabled = RemoteControlConfig {
        enable: true,
        listen_addr: "127.0.0.1:0".into(),
        authorized_clients: vec![authorized("k", &key, RemoteTier::Observe)],
    };
    assert_eq!(
        absent(&RemoteControlConfig::default(), Err("x".into())),
        Absence::Disabled
    );
    assert_eq!(
        absent(
            &RemoteControlConfig {
                authorized_clients: Vec::new(),
                ..enabled.clone()
            },
            Err("x".into())
        ),
        Absence::NoAuthorizedClients
    );
    assert!(matches!(
        absent(&enabled, Err("no key".into())),
        Absence::IdentityUnavailable(why) if why == "no key"
    ));
    assert!(matches!(
        absent(
            &RemoteControlConfig {
                listen_addr: "nowhere".into(),
                ..enabled
            },
            Err("x".into())
        ),
        Absence::ControlConfigInvalid(_)
    ));
    stop.stop();
}
