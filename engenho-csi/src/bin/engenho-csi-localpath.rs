//! `engenho-csi-localpath` — engenho's own CSI driver, as a binary.
//!
//! A conformant CSI driver serving Identity, Controller and Node over a
//! unix socket, plus the plugin-registration service on a second socket so
//! a kubelet's plugin watcher discovers it without a `node-driver-registrar`
//! sidecar.
//!
//! ★ IT SERVES ITS OWN REGISTRATION, WHICH IS A DELIBERATE SIMPLIFICATION
//! AND A STATED ONE. Upstream splits the two sockets across two containers
//! so the registrar can restart independently of the driver. engenho runs
//! the driver in-process on the node, where that split buys nothing and
//! costs a sidecar. Both sockets are still served — a runtime expecting two
//! files finds two files — so the difference is invisible from outside.
//!
//! ```text
//! engenho-csi-localpath \
//!   --endpoint /var/lib/kubelet/plugins/localpath.csi.engenho.io/csi.sock \
//!   --registration-socket /var/lib/kubelet/plugins_registry/localpath-reg.sock \
//!   --root /var/lib/engenho/csi --node-id cid
//! ```

use std::path::PathBuf;

use engenho_csi::localpath::{DRIVER_NAME, LocalPathDriver};
use engenho_csi::{pb, reg};
use tonic::{Request, Response, Status};

/// The registration service, answering the kubelet's plugin watcher.
#[derive(Clone)]
struct Registration {
    endpoint: String,
}

#[tonic::async_trait]
impl reg::registration_server::Registration for Registration {
    async fn get_info(
        &self,
        _r: Request<reg::InfoRequest>,
    ) -> Result<Response<reg::PluginInfo>, Status> {
        Ok(Response::new(reg::PluginInfo {
            // CSIPlugin, not DevicePlugin: the discriminator that stops a
            // kubelet dialing this as a GPU plugin.
            r#type: engenho_csi::registry::CSI_PLUGIN_TYPE.to_string(),
            name: DRIVER_NAME.to_string(),
            endpoint: self.endpoint.clone(),
            supported_versions: vec![engenho_csi::registry::SUPPORTED_CSI_VERSION.to_string()],
        }))
    }

    async fn notify_registration_status(
        &self,
        r: Request<reg::RegistrationStatus>,
    ) -> Result<Response<reg::RegistrationStatusResponse>, Status> {
        let r = r.into_inner();
        if r.plugin_registered {
            tracing::info!(driver = DRIVER_NAME, "registered with the node");
        } else {
            // The rejection reason is the whole diagnostic value of this
            // callback. Dropping it leaves a driver that appears to flap
            // for no reason.
            tracing::error!(driver = DRIVER_NAME, error = %r.error, "registration REFUSED");
        }
        Ok(Response::new(reg::RegistrationStatusResponse {}))
    }
}

fn flag(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| default.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_target(false).init();

    let endpoint = flag("--endpoint", "/tmp/engenho-csi/csi.sock");
    let registration = flag("--registration-socket", "/tmp/engenho-csi/reg.sock");
    let root = flag("--root", "/var/lib/engenho/csi");
    let node_id = flag("--node-id", "localhost");

    let driver = LocalPathDriver::new(&root, &node_id);
    std::fs::create_dir_all(&root)?;

    // A stale socket from a previous run blocks bind with EADDRINUSE, and
    // the resulting error names the address rather than the cause. Removing
    // it is what every unix-socket server does, upstream drivers included.
    for path in [&endpoint, &registration] {
        let p = PathBuf::from(path);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if p.exists() {
            std::fs::remove_file(&p)?;
        }
    }

    let driver_listener = tokio::net::UnixListener::bind(&endpoint)?;
    let reg_listener = tokio::net::UnixListener::bind(&registration)?;
    tracing::info!(driver = DRIVER_NAME, %endpoint, %registration, %root, %node_id, "serving");

    let reg_svc = Registration {
        // The DRIVER socket, not this one — the whole point of the
        // two-socket handshake is that GetInfo redirects.
        endpoint: endpoint.clone(),
    };

    let driver_server = tonic::transport::Server::builder()
        .add_service(pb::identity_server::IdentityServer::new(driver.clone()))
        .add_service(pb::controller_server::ControllerServer::new(driver.clone()))
        .add_service(pb::node_server::NodeServer::new(driver))
        .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(
            driver_listener,
        ));

    let reg_server = tonic::transport::Server::builder()
        .add_service(reg::registration_server::RegistrationServer::new(reg_svc))
        .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(
            reg_listener,
        ));

    tokio::try_join!(driver_server, reg_server)?;
    Ok(())
}
