//! The CSI client — engenho dialing a vendor's driver over a unix socket.
//!
//! ★ WHY A UNIX SOCKET AND NOT A TCP ADDRESS. The CSI deployment contract
//! puts the driver's socket in a directory the kubelet also mounts, and
//! every driver in existence defaults to `unix:///csi/csi.sock`. A TCP
//! client would work against nothing that ships.
//!
//! ★ THE `unix://` PREFIX IS NOT DECORATION. A driver advertises its
//! endpoint as a URI, and the registration protocol hands us that string
//! verbatim. Accepting only a bare path would reject every conformant
//! driver; accepting only the URI would reject a hand-configured one.
//! [`socket_path`] normalises both, and refuses a scheme engenho cannot
//! actually dial rather than silently treating it as a path.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tonic::transport::{Channel, Endpoint, Uri};

use crate::pb::controller_client::ControllerClient;
use crate::pb::identity_client::IdentityClient;
use crate::pb::node_client::NodeClient;

/// Errors dialing or talking to a driver.
#[derive(Debug, thiserror::Error)]
pub enum CsiError {
    /// The endpoint string names a transport engenho cannot dial.
    #[error("unsupported CSI endpoint {0:?}: only unix:// (or a bare path) is dialable")]
    UnsupportedEndpoint(String),
    /// The socket could not be reached.
    #[error("connecting to CSI driver at {path}: {source}")]
    Connect {
        /// Socket path that failed.
        path: String,
        /// Underlying transport error.
        #[source]
        source: tonic::transport::Error,
    },
    /// The driver answered with a gRPC error.
    #[error("CSI {rpc} failed: {status}")]
    Rpc {
        /// The RPC name, so a log line says which call broke.
        rpc: &'static str,
        /// The driver's status.
        status: Box<tonic::Status>,
    },
    /// The driver answered, but with a response engenho cannot act on.
    #[error("CSI {rpc} returned an unusable response: {detail}")]
    Malformed {
        /// The RPC name.
        rpc: &'static str,
        /// What was wrong.
        detail: String,
    },
}

engenho_substrate::impl_error_kind! {
    CsiError {
        (UnsupportedEndpoint(_)) => "unsupported_endpoint",
        { Connect { .. } } => "connect",
        { Rpc { .. } } => "rpc",
        { Malformed { .. } } => "malformed",
    }
}

/// Normalise a driver endpoint to a filesystem socket path.
///
/// Accepts `unix:///a/b`, `unix://a/b` (the two-slash form real drivers
/// emit) and a bare `/a/b`. Refuses anything else rather than treating an
/// unknown scheme as a relative path — dialing `tcp:` as a file produces a
/// confusing ENOENT instead of naming the real problem.
///
/// # Errors
/// [`CsiError::UnsupportedEndpoint`] for a scheme engenho cannot dial.
pub fn socket_path(endpoint: &str) -> Result<PathBuf, CsiError> {
    if let Some(rest) = endpoint.strip_prefix("unix://") {
        // `unix:///var/x` → "/var/x"; `unix://var/x` → "var/x", which real
        // drivers write meaning the absolute path. Both normalise the same
        // way by keeping the leading slash when it is there and adding one
        // when the remainder is clearly absolute-intended.
        return Ok(PathBuf::from(if rest.starts_with('/') {
            rest.to_string()
        } else {
            format!("/{rest}")
        }));
    }
    if let Some((scheme, _)) = endpoint.split_once("://") {
        return Err(CsiError::UnsupportedEndpoint(format!("{scheme}://…")));
    }
    if endpoint.is_empty() {
        return Err(CsiError::UnsupportedEndpoint(String::new()));
    }
    Ok(PathBuf::from(endpoint))
}

/// Dial a CSI driver's unix socket.
///
/// # Errors
/// [`CsiError::Connect`] if the socket is absent or refuses.
///
/// # Panics
/// Never in practice: the only `expect` is on a compile-time-constant URI.
pub async fn connect(path: &Path) -> Result<Channel, CsiError> {
    let owned = path.to_path_buf();
    let display = owned.display().to_string();
    // The URI is ignored by the custom connector but tonic requires a
    // syntactically valid one; `http://csi.local` is the conventional
    // placeholder and never leaves the process.
    Endpoint::try_from("http://csi.local")
        .expect("a static valid URI")
        .connect_timeout(Duration::from_secs(10))
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let p = owned.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(p).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .map_err(|source| CsiError::Connect {
            path: display,
            source,
        })
}

/// What a driver told us about itself.
///
/// ★ THE CAPABILITY FLAGS ARE READ, NOT ASSUMED, and that is the whole
/// point of `GetPluginCapabilities`. Calling `ControllerPublishVolume` on a
/// driver that does not declare `PUBLISH_UNPUBLISH_VOLUME` is not a
/// harmless no-op — it is an `Unimplemented` that a naive caller retries
/// forever. Same for staging: a driver without `STAGE_UNSTAGE_VOLUME`
/// expects `NodePublishVolume` to be called DIRECTLY, and staging first
/// makes the mount fail in a way that reads as a broken volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverInfo {
    /// The driver's own name (`ebs.csi.aws.com`). Keyed on everywhere.
    pub name: String,
    /// Vendor version string, for logs and events.
    pub vendor_version: String,
    /// Driver implements the Controller service (provision, attach).
    pub has_controller_service: bool,
    /// Driver requires `NodeStageVolume` before `NodePublishVolume`.
    pub stage_unstage: bool,
    /// Driver implements `ControllerPublishVolume` (attach/detach).
    pub publish_unpublish: bool,
    /// The node id this driver uses for THIS node — a driver's own
    /// identifier, not engenho's node name, and the two are routinely
    /// different (an EBS driver reports an EC2 instance id).
    pub node_id: String,
}

/// A connected driver.
#[derive(Clone)]
pub struct CsiClient {
    channel: Channel,
    /// The socket, kept for error messages.
    path: String,
}

impl std::fmt::Debug for CsiClient {
    /// Hand-written because `Channel` has no useful `Debug` and printing it
    /// in a log line adds noise without adding information. The socket path
    /// is the only field that identifies WHICH driver this is.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsiClient")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

fn rpc_err(rpc: &'static str) -> impl Fn(tonic::Status) -> CsiError {
    move |status| CsiError::Rpc {
        rpc,
        status: Box::new(status),
    }
}

impl CsiClient {
    /// Dial the driver at `endpoint` (a `unix://` URI or a bare path).
    ///
    /// # Errors
    /// [`CsiError::UnsupportedEndpoint`] or [`CsiError::Connect`].
    pub async fn dial(endpoint: &str) -> Result<Self, CsiError> {
        let path = socket_path(endpoint)?;
        let channel = connect(&path).await?;
        Ok(Self {
            channel,
            path: path.display().to_string(),
        })
    }

    /// The socket this client is bound to.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.path
    }

    /// `Identity.Probe` — is the driver ready to serve?
    ///
    /// An absent `ready` field means READY: the field is a
    /// `google.protobuf.BoolValue` precisely so a driver can decline to
    /// answer, and upstream treats no-answer as ready. Defaulting the other
    /// way would leave every driver that omits it permanently unready.
    ///
    /// # Errors
    /// [`CsiError::Rpc`] if the driver refuses.
    pub async fn probe(&self) -> Result<bool, CsiError> {
        let resp = IdentityClient::new(self.channel.clone())
            .probe(crate::pb::ProbeRequest {})
            .await
            .map_err(rpc_err("Probe"))?;
        // prost maps `google.protobuf.BoolValue` to `Option<bool>`, so the
        // "declined to answer" case is the `None` and not a wrapper struct.
        Ok(resp.into_inner().ready.unwrap_or(true))
    }

    /// `Identity.GetPluginInfo` + `GetPluginCapabilities` +
    /// `Node.NodeGetInfo`, folded into one [`DriverInfo`].
    ///
    /// # Errors
    /// [`CsiError::Rpc`] on any of the three; [`CsiError::Malformed`] if the
    /// driver returns an empty name, which would make it unaddressable.
    pub async fn info(&self) -> Result<DriverInfo, CsiError> {
        let mut identity = IdentityClient::new(self.channel.clone());
        let plugin = identity
            .get_plugin_info(crate::pb::GetPluginInfoRequest {})
            .await
            .map_err(rpc_err("GetPluginInfo"))?
            .into_inner();
        if plugin.name.is_empty() {
            return Err(CsiError::Malformed {
                rpc: "GetPluginInfo",
                detail: "empty driver name: the driver would be unaddressable".into(),
            });
        }

        let caps = identity
            .get_plugin_capabilities(crate::pb::GetPluginCapabilitiesRequest {})
            .await
            .map_err(rpc_err("GetPluginCapabilities"))?
            .into_inner();
        let has_controller_service = caps.capabilities.iter().any(|c| {
            matches!(
                &c.r#type,
                Some(crate::pb::plugin_capability::Type::Service(s))
                    if s.r#type == crate::pb::plugin_capability::service::Type::ControllerService as i32
            )
        });

        let node = NodeClient::new(self.channel.clone())
            .node_get_info(crate::pb::NodeGetInfoRequest {})
            .await
            .map_err(rpc_err("NodeGetInfo"))?
            .into_inner();

        let (stage_unstage, publish_unpublish) = self.node_and_controller_caps().await?;

        Ok(DriverInfo {
            name: plugin.name,
            vendor_version: plugin.vendor_version,
            has_controller_service,
            stage_unstage,
            publish_unpublish,
            node_id: node.node_id,
        })
    }

    /// `(stage_unstage, publish_unpublish)` from the node + controller
    /// capability RPCs.
    ///
    /// A driver with no controller service legitimately fails
    /// `ControllerGetCapabilities`, so that half degrades to `false` rather
    /// than failing the whole probe — a node-only driver is a normal,
    /// supported deployment, not an error.
    async fn node_and_controller_caps(&self) -> Result<(bool, bool), CsiError> {
        let node_caps = NodeClient::new(self.channel.clone())
            .node_get_capabilities(crate::pb::NodeGetCapabilitiesRequest {})
            .await
            .map_err(rpc_err("NodeGetCapabilities"))?
            .into_inner();
        let stage_unstage = node_caps.capabilities.iter().any(|c| {
            matches!(
                &c.r#type,
                Some(crate::pb::node_service_capability::Type::Rpc(r))
                    if r.r#type == crate::pb::node_service_capability::rpc::Type::StageUnstageVolume as i32
            )
        });

        let publish_unpublish = match ControllerClient::new(self.channel.clone())
            .controller_get_capabilities(crate::pb::ControllerGetCapabilitiesRequest {})
            .await
        {
            Ok(r) => r.into_inner().capabilities.iter().any(|c| {
                matches!(
                    &c.r#type,
                    Some(crate::pb::controller_service_capability::Type::Rpc(r))
                        if r.r#type
                            == crate::pb::controller_service_capability::rpc::Type::PublishUnpublishVolume as i32
                )
            }),
            Err(_) => false,
        };

        Ok((stage_unstage, publish_unpublish))
    }

    /// `Node.NodeStageVolume`.
    ///
    /// # Errors
    /// [`CsiError::Rpc`] if the driver refuses.
    pub async fn node_stage(&self, req: crate::pb::NodeStageVolumeRequest) -> Result<(), CsiError> {
        NodeClient::new(self.channel.clone())
            .node_stage_volume(req)
            .await
            .map_err(rpc_err("NodeStageVolume"))?;
        Ok(())
    }

    /// `Node.NodeUnstageVolume`.
    ///
    /// # Errors
    /// [`CsiError::Rpc`] if the driver refuses.
    pub async fn node_unstage(
        &self,
        req: crate::pb::NodeUnstageVolumeRequest,
    ) -> Result<(), CsiError> {
        NodeClient::new(self.channel.clone())
            .node_unstage_volume(req)
            .await
            .map_err(rpc_err("NodeUnstageVolume"))?;
        Ok(())
    }

    /// `Node.NodePublishVolume` — the call that actually makes the volume
    /// visible at the pod's mount path.
    ///
    /// # Errors
    /// [`CsiError::Rpc`] if the driver refuses.
    pub async fn node_publish(
        &self,
        req: crate::pb::NodePublishVolumeRequest,
    ) -> Result<(), CsiError> {
        NodeClient::new(self.channel.clone())
            .node_publish_volume(req)
            .await
            .map_err(rpc_err("NodePublishVolume"))?;
        Ok(())
    }

    /// `Node.NodeUnpublishVolume`.
    ///
    /// # Errors
    /// [`CsiError::Rpc`] if the driver refuses.
    pub async fn node_unpublish(
        &self,
        req: crate::pb::NodeUnpublishVolumeRequest,
    ) -> Result<(), CsiError> {
        NodeClient::new(self.channel.clone())
            .node_unpublish_volume(req)
            .await
            .map_err(rpc_err("NodeUnpublishVolume"))?;
        Ok(())
    }

    /// `Controller.CreateVolume` — dynamic provisioning.
    ///
    /// # Errors
    /// [`CsiError::Rpc`] if the driver refuses; [`CsiError::Malformed`] if
    /// it reports success without a volume id, which would leave a PV
    /// pointing at nothing.
    pub async fn create_volume(
        &self,
        req: crate::pb::CreateVolumeRequest,
    ) -> Result<crate::pb::Volume, CsiError> {
        let resp = ControllerClient::new(self.channel.clone())
            .create_volume(req)
            .await
            .map_err(rpc_err("CreateVolume"))?
            .into_inner();
        let volume = resp.volume.ok_or(CsiError::Malformed {
            rpc: "CreateVolume",
            detail: "success with no volume".into(),
        })?;
        if volume.volume_id.is_empty() {
            return Err(CsiError::Malformed {
                rpc: "CreateVolume",
                detail: "success with an empty volume id: the PV would reference nothing".into(),
            });
        }
        Ok(volume)
    }

    /// `Controller.DeleteVolume`.
    ///
    /// # Errors
    /// [`CsiError::Rpc`] if the driver refuses.
    pub async fn delete_volume(&self, req: crate::pb::DeleteVolumeRequest) -> Result<(), CsiError> {
        ControllerClient::new(self.channel.clone())
            .delete_volume(req)
            .await
            .map_err(rpc_err("DeleteVolume"))?;
        Ok(())
    }

    /// `Controller.ControllerPublishVolume` — attach.
    ///
    /// # Errors
    /// [`CsiError::Rpc`] if the driver refuses.
    pub async fn controller_publish(
        &self,
        req: crate::pb::ControllerPublishVolumeRequest,
    ) -> Result<std::collections::HashMap<String, String>, CsiError> {
        let resp = ControllerClient::new(self.channel.clone())
            .controller_publish_volume(req)
            .await
            .map_err(rpc_err("ControllerPublishVolume"))?
            .into_inner();
        Ok(resp.publish_context)
    }

    /// `Controller.ControllerUnpublishVolume` — detach.
    ///
    /// # Errors
    /// [`CsiError::Rpc`] if the driver refuses.
    pub async fn controller_unpublish(
        &self,
        req: crate::pb::ControllerUnpublishVolumeRequest,
    ) -> Result<(), CsiError> {
        ControllerClient::new(self.channel.clone())
            .controller_unpublish_volume(req)
            .await
            .map_err(rpc_err("ControllerUnpublishVolume"))?;
        Ok(())
    }
}

#[cfg(test)]
mod endpoint_tests {
    use super::*;

    #[test]
    fn every_form_a_real_driver_emits_resolves_to_the_same_path() {
        assert_eq!(
            socket_path("unix:///csi/csi.sock").unwrap(),
            PathBuf::from("/csi/csi.sock")
        );
        // The two-slash form. Real drivers write this and mean the same
        // absolute path; treating it as relative would dial the wrong file
        // from whatever cwd the runtime happens to have.
        assert_eq!(
            socket_path("unix://csi/csi.sock").unwrap(),
            PathBuf::from("/csi/csi.sock")
        );
        assert_eq!(
            socket_path("/csi/csi.sock").unwrap(),
            PathBuf::from("/csi/csi.sock")
        );
    }

    #[test]
    fn a_scheme_engenho_cannot_dial_is_refused_by_name_not_treated_as_a_path() {
        // Silently treating `tcp://host:9000` as a filename produces an
        // ENOENT naming a path nobody wrote, which is the worst possible
        // diagnostic for a misconfigured driver.
        let e = socket_path("tcp://host:9000").unwrap_err();
        assert!(matches!(e, CsiError::UnsupportedEndpoint(_)), "{e:?}");
        assert!(e.to_string().contains("tcp"), "{e}");
        assert!(socket_path("").is_err(), "an empty endpoint is not a path");
    }
}
