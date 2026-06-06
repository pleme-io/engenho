//! GENERATED — DO NOT EDIT by hand. Source: engenho-kube-codegen.
//!
//! Shared sub-structs referenced by the generated kinds — emitted once,
//! globally deduplicated, so every kind references one canonical type.

#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};
use crate::generated_v1_34::core_v1::PersistentVolumeClaim;

/// Represents a Persistent Disk resource in AWS.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AWSElasticBlockStoreVolumeSource {
    /// fsType is the filesystem type of the volume that you want to mount. Tip: Ensure that the filesystem type is supported by the host operating system. Examples: "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified. More info: https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// partition is the partition in the volume that you want to mount. If omitted, the default is to mount by volume name. Examples: For volume /dev/sda1, you specify the partition as "1". Similarly, the volume partition for /dev/sda is "0" (or you can leave the property empty).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<i32>,
    /// readOnly value true will force the readOnly setting in VolumeMounts. More info: https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// volumeID is unique ID of the persistent disk resource in AWS (Amazon EBS volume). More info: https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore
    #[serde(default, rename = "volumeID")]
    pub volume_id: String,
}

/// Affinity is a group of affinity scheduling rules.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Affinity {
    /// Describes node affinity scheduling rules for the pod.
    #[serde(default, rename = "nodeAffinity", skip_serializing_if = "Option::is_none")]
    pub node_affinity: Option<NodeAffinity>,
    /// Describes pod affinity scheduling rules (e.g. co-locate this pod in the same node, zone, etc. as some other pod(s)).
    #[serde(default, rename = "podAffinity", skip_serializing_if = "Option::is_none")]
    pub pod_affinity: Option<PodAffinity>,
    /// Describes pod anti-affinity scheduling rules (e.g. avoid putting this pod in the same node, zone, etc. as some other pod(s)).
    #[serde(default, rename = "podAntiAffinity", skip_serializing_if = "Option::is_none")]
    pub pod_anti_affinity: Option<PodAntiAffinity>,
}

/// AggregationRule describes how to locate ClusterRoles to aggregate into the ClusterRole
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AggregationRule {
    /// ClusterRoleSelectors holds a list of selectors which will be used to find ClusterRoles and create the rules. If any of the selectors match, then the ClusterRole's permissions will be added
    #[serde(default, rename = "clusterRoleSelectors", skip_serializing_if = "Vec::is_empty")]
    pub cluster_role_selectors: Vec<LabelSelector>,
}

/// AppArmorProfile defines a pod or container's AppArmor settings.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AppArmorProfile {
    /// localhostProfile indicates a profile loaded on the node that should be used. The profile must be preconfigured on the node to work. Must match the loaded name of the profile. Must be set if and only if type is "Localhost".
    #[serde(default, rename = "localhostProfile", skip_serializing_if = "Option::is_none")]
    pub localhost_profile: Option<String>,
    /// type indicates which kind of AppArmor profile will be applied. Valid options are:
    /// Localhost - a profile pre-loaded on the node.
    /// RuntimeDefault - the container runtime's default profile.
    /// Unconfined - no AppArmor enforcement.
    #[serde(default, rename = "type")]
    pub r#type: String,
}

/// AttachedVolume describes a volume attached to a node
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AttachedVolume {
    /// DevicePath represents the device path where the volume should be available
    #[serde(default, rename = "devicePath")]
    pub device_path: String,
    /// Name of the attached volume
    #[serde(default)]
    pub name: String,
}

/// AzureDisk represents an Azure Data Disk mount on the host and bind mount to the pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AzureDiskVolumeSource {
    /// cachingMode is the Host Caching mode: None, Read Only, Read Write.
    #[serde(default, rename = "cachingMode", skip_serializing_if = "Option::is_none")]
    pub caching_mode: Option<String>,
    /// diskName is the Name of the data disk in the blob storage
    #[serde(default, rename = "diskName")]
    pub disk_name: String,
    /// diskURI is the URI of data disk in the blob storage
    #[serde(default, rename = "diskURI")]
    pub disk_uri: String,
    /// fsType is Filesystem type to mount. Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// readOnly Defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

/// AzureFile represents an Azure File Service mount on the host and bind mount to the pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AzureFilePersistentVolumeSource {
    /// readOnly defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretName is the name of secret that contains Azure Storage Account Name and Key
    #[serde(default, rename = "secretName")]
    pub secret_name: String,
    /// secretNamespace is the namespace of the secret that contains Azure Storage Account Name and Key default is the same as the Pod
    #[serde(default, rename = "secretNamespace", skip_serializing_if = "Option::is_none")]
    pub secret_namespace: Option<String>,
    /// shareName is the azure Share Name
    #[serde(default, rename = "shareName")]
    pub share_name: String,
}

/// AzureFile represents an Azure File Service mount on the host and bind mount to the pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AzureFileVolumeSource {
    /// readOnly defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretName is the  name of secret that contains Azure Storage Account Name and Key
    #[serde(default, rename = "secretName")]
    pub secret_name: String,
    /// shareName is the azure share Name
    #[serde(default, rename = "shareName")]
    pub share_name: String,
}

/// Represents storage that is managed by an external CSI volume driver
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CSIPersistentVolumeSource {
    /// controllerExpandSecretRef is a reference to the secret object containing sensitive information to pass to the CSI driver to complete the CSI ControllerExpandVolume call. This field is optional, and may be empty if no secret is required. If the secret object contains more than one secret, all secrets are passed.
    #[serde(default, rename = "controllerExpandSecretRef", skip_serializing_if = "Option::is_none")]
    pub controller_expand_secret_ref: Option<SecretReference>,
    /// controllerPublishSecretRef is a reference to the secret object containing sensitive information to pass to the CSI driver to complete the CSI ControllerPublishVolume and ControllerUnpublishVolume calls. This field is optional, and may be empty if no secret is required. If the secret object contains more than one secret, all secrets are passed.
    #[serde(default, rename = "controllerPublishSecretRef", skip_serializing_if = "Option::is_none")]
    pub controller_publish_secret_ref: Option<SecretReference>,
    /// driver is the name of the driver to use for this volume. Required.
    #[serde(default)]
    pub driver: String,
    /// fsType to mount. Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs", "ntfs".
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// nodeExpandSecretRef is a reference to the secret object containing sensitive information to pass to the CSI driver to complete the CSI NodeExpandVolume call. This field is optional, may be omitted if no secret is required. If the secret object contains more than one secret, all secrets are passed.
    #[serde(default, rename = "nodeExpandSecretRef", skip_serializing_if = "Option::is_none")]
    pub node_expand_secret_ref: Option<SecretReference>,
    /// nodePublishSecretRef is a reference to the secret object containing sensitive information to pass to the CSI driver to complete the CSI NodePublishVolume and NodeUnpublishVolume calls. This field is optional, and may be empty if no secret is required. If the secret object contains more than one secret, all secrets are passed.
    #[serde(default, rename = "nodePublishSecretRef", skip_serializing_if = "Option::is_none")]
    pub node_publish_secret_ref: Option<SecretReference>,
    /// nodeStageSecretRef is a reference to the secret object containing sensitive information to pass to the CSI driver to complete the CSI NodeStageVolume and NodeStageVolume and NodeUnstageVolume calls. This field is optional, and may be empty if no secret is required. If the secret object contains more than one secret, all secrets are passed.
    #[serde(default, rename = "nodeStageSecretRef", skip_serializing_if = "Option::is_none")]
    pub node_stage_secret_ref: Option<SecretReference>,
    /// readOnly value to pass to ControllerPublishVolumeRequest. Defaults to false (read/write).
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// volumeAttributes of the volume to publish.
    #[serde(default, rename = "volumeAttributes", skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub volume_attributes: std::collections::BTreeMap<String, String>,
    /// volumeHandle is the unique volume name returned by the CSI volume plugin’s CreateVolume to refer to the volume on all subsequent calls. Required.
    #[serde(default, rename = "volumeHandle")]
    pub volume_handle: String,
}

/// Represents a source location of a volume to mount, managed by an external CSI driver
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CSIVolumeSource {
    /// driver is the name of the CSI driver that handles this volume. Consult with your admin for the correct name as registered in the cluster.
    #[serde(default)]
    pub driver: String,
    /// fsType to mount. Ex. "ext4", "xfs", "ntfs". If not provided, the empty value is passed to the associated CSI driver which will determine the default filesystem to apply.
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// nodePublishSecretRef is a reference to the secret object containing sensitive information to pass to the CSI driver to complete the CSI NodePublishVolume and NodeUnpublishVolume calls. This field is optional, and  may be empty if no secret is required. If the secret object contains more than one secret, all secret references are passed.
    #[serde(default, rename = "nodePublishSecretRef", skip_serializing_if = "Option::is_none")]
    pub node_publish_secret_ref: Option<LocalObjectReference>,
    /// readOnly specifies a read-only configuration for the volume. Defaults to false (read/write).
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// volumeAttributes stores driver-specific properties that are passed to the CSI driver. Consult your driver's documentation for supported values.
    #[serde(default, rename = "volumeAttributes", skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub volume_attributes: std::collections::BTreeMap<String, String>,
}

/// Adds and removes POSIX capabilities from running containers.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Added capabilities
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub add: Vec<String>,
    /// Removed capabilities
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub drop: Vec<String>,
}

/// Represents a Ceph Filesystem mount that lasts the lifetime of a pod Cephfs volumes do not support ownership management or SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CephFSPersistentVolumeSource {
    /// monitors is Required: Monitors is a collection of Ceph monitors More info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub monitors: Vec<String>,
    /// path is Optional: Used as the mounted root, rather than the full Ceph tree, default is /
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// readOnly is Optional: Defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts. More info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretFile is Optional: SecretFile is the path to key ring for User, default is /etc/ceph/user.secret More info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
    #[serde(default, rename = "secretFile", skip_serializing_if = "Option::is_none")]
    pub secret_file: Option<String>,
    /// secretRef is Optional: SecretRef is reference to the authentication secret for User, default is empty. More info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretReference>,
    /// user is Optional: User is the rados user name, default is admin More info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// Represents a Ceph Filesystem mount that lasts the lifetime of a pod Cephfs volumes do not support ownership management or SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CephFSVolumeSource {
    /// monitors is Required: Monitors is a collection of Ceph monitors More info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub monitors: Vec<String>,
    /// path is Optional: Used as the mounted root, rather than the full Ceph tree, default is /
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// readOnly is Optional: Defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts. More info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretFile is Optional: SecretFile is the path to key ring for User, default is /etc/ceph/user.secret More info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
    #[serde(default, rename = "secretFile", skip_serializing_if = "Option::is_none")]
    pub secret_file: Option<String>,
    /// secretRef is Optional: SecretRef is reference to the authentication secret for User, default is empty. More info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<LocalObjectReference>,
    /// user is optional: User is the rados user name, default is admin More info: https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// Represents a cinder volume resource in Openstack. A Cinder volume must exist before mounting to a container. The volume must also be in the same region as the kubelet. Cinder volumes support ownership management and SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CinderPersistentVolumeSource {
    /// fsType Filesystem type to mount. Must be a filesystem type supported by the host operating system. Examples: "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified. More info: https://examples.k8s.io/mysql-cinder-pd/README.md
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// readOnly is Optional: Defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts. More info: https://examples.k8s.io/mysql-cinder-pd/README.md
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretRef is Optional: points to a secret object containing parameters used to connect to OpenStack.
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretReference>,
    /// volumeID used to identify the volume in cinder. More info: https://examples.k8s.io/mysql-cinder-pd/README.md
    #[serde(default, rename = "volumeID")]
    pub volume_id: String,
}

/// Represents a cinder volume resource in Openstack. A Cinder volume must exist before mounting to a container. The volume must also be in the same region as the kubelet. Cinder volumes support ownership management and SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CinderVolumeSource {
    /// fsType is the filesystem type to mount. Must be a filesystem type supported by the host operating system. Examples: "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified. More info: https://examples.k8s.io/mysql-cinder-pd/README.md
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// readOnly defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts. More info: https://examples.k8s.io/mysql-cinder-pd/README.md
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretRef is optional: points to a secret object containing parameters used to connect to OpenStack.
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<LocalObjectReference>,
    /// volumeID used to identify the volume in cinder. More info: https://examples.k8s.io/mysql-cinder-pd/README.md
    #[serde(default, rename = "volumeID")]
    pub volume_id: String,
}

/// ClientIPConfig represents the configurations of Client IP based session affinity.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ClientIPConfig {
    /// timeoutSeconds specifies the seconds of ClientIP type session sticky time. The value must be >0 && <=86400(for 1 day) if ServiceAffinity == "ClientIP". Default value is 10800(for 3 hours).
    #[serde(default, rename = "timeoutSeconds", skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,
}

/// ClusterTrustBundleProjection describes how to select a set of ClusterTrustBundle objects and project their contents into the pod filesystem.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ClusterTrustBundleProjection {
    /// Select all ClusterTrustBundles that match this label selector.  Only has effect if signerName is set.  Mutually-exclusive with name.  If unset, interpreted as "match nothing".  If set but empty, interpreted as "match everything".
    #[serde(default, rename = "labelSelector", skip_serializing_if = "Option::is_none")]
    pub label_selector: Option<LabelSelector>,
    /// Select a single ClusterTrustBundle by object name.  Mutually-exclusive with signerName and labelSelector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// If true, don't block pod startup if the referenced ClusterTrustBundle(s) aren't available.  If using name, then the named ClusterTrustBundle is allowed not to exist.  If using signerName, then the combination of signerName and labelSelector is allowed to match zero ClusterTrustBundles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
    /// Relative path from the volume root to write the bundle.
    #[serde(default)]
    pub path: String,
    /// Select all ClusterTrustBundles that match this signer name. Mutually-exclusive with name.  The contents of all selected ClusterTrustBundles will be unified and deduplicated.
    #[serde(default, rename = "signerName", skip_serializing_if = "Option::is_none")]
    pub signer_name: Option<String>,
}

/// Condition contains details for one aspect of the current state of this API Resource.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Condition {
    /// lastTransitionTime is the last time the condition transitioned from one status to another. This should be when the underlying condition changed.  If that is not known, then using the time when the API field changed is acceptable.
    #[serde(default, rename = "lastTransitionTime")]
    pub last_transition_time: Time,
    /// message is a human readable message indicating details about the transition. This may be an empty string.
    #[serde(default)]
    pub message: String,
    /// observedGeneration represents the .metadata.generation that the condition was set based upon. For instance, if .metadata.generation is currently 12, but the .status.conditions[x].observedGeneration is 9, the condition is out of date with respect to the current state of the instance.
    #[serde(default, rename = "observedGeneration", skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// reason contains a programmatic identifier indicating the reason for the condition's last transition. Producers of specific condition types may define expected values and meanings for this field, and whether the values are considered a guaranteed API. The value should be a CamelCase string. This field may not be empty.
    #[serde(default)]
    pub reason: String,
    /// status of the condition, one of True, False, Unknown.
    #[serde(default)]
    pub status: String,
    /// type of condition in CamelCase or in foo.example.com/CamelCase.
    #[serde(default, rename = "type")]
    pub r#type: String,
}

/// ConfigMapEnvSource selects a ConfigMap to populate the environment variables with.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigMapEnvSource {
    /// Name of the referent. This field is effectively required, but due to backwards compatibility is allowed to be empty. Instances of this type with an empty value here are almost certainly wrong. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Specify whether the ConfigMap must be defined
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// Selects a key from a ConfigMap.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigMapKeySelector {
    /// The key to select.
    #[serde(default)]
    pub key: String,
    /// Name of the referent. This field is effectively required, but due to backwards compatibility is allowed to be empty. Instances of this type with an empty value here are almost certainly wrong. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Specify whether the ConfigMap or its key must be defined
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// ConfigMapNodeConfigSource contains the information to reference a ConfigMap as a config source for the Node. This API is deprecated since 1.22: https://git.k8s.io/enhancements/keps/sig-node/281-dynamic-kubelet-configuration
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigMapNodeConfigSource {
    /// KubeletConfigKey declares which key of the referenced ConfigMap corresponds to the KubeletConfiguration structure This field is required in all cases.
    #[serde(default, rename = "kubeletConfigKey")]
    pub kubelet_config_key: String,
    /// Name is the metadata.name of the referenced ConfigMap. This field is required in all cases.
    #[serde(default)]
    pub name: String,
    /// Namespace is the metadata.namespace of the referenced ConfigMap. This field is required in all cases.
    #[serde(default)]
    pub namespace: String,
    /// ResourceVersion is the metadata.ResourceVersion of the referenced ConfigMap. This field is forbidden in Node.Spec, and required in Node.Status.
    #[serde(default, rename = "resourceVersion", skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    /// UID is the metadata.UID of the referenced ConfigMap. This field is forbidden in Node.Spec, and required in Node.Status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}

/// Adapts a ConfigMap into a projected volume.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigMapProjection {
    /// items if unspecified, each key-value pair in the Data field of the referenced ConfigMap will be projected into the volume as a file whose name is the key and content is the value. If specified, the listed keys will be projected into the specified paths, and unlisted keys will not be present. If a key is specified which is not present in the ConfigMap, the volume setup will error unless it is marked optional. Paths must be relative and may not contain the '..' path or start with '..'.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<KeyToPath>,
    /// Name of the referent. This field is effectively required, but due to backwards compatibility is allowed to be empty. Instances of this type with an empty value here are almost certainly wrong. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// optional specify whether the ConfigMap or its keys must be defined
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// Adapts a ConfigMap into a volume.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigMapVolumeSource {
    /// defaultMode is optional: mode bits used to set permissions on created files by default. Must be an octal value between 0000 and 0777 or a decimal value between 0 and 511. YAML accepts both octal and decimal values, JSON requires decimal values for mode bits. Defaults to 0644. Directories within the path are not affected by this setting. This might be in conflict with other options that affect the file mode, like fsGroup, and the result can be other mode bits set.
    #[serde(default, rename = "defaultMode", skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<i32>,
    /// items if unspecified, each key-value pair in the Data field of the referenced ConfigMap will be projected into the volume as a file whose name is the key and content is the value. If specified, the listed keys will be projected into the specified paths, and unlisted keys will not be present. If a key is specified which is not present in the ConfigMap, the volume setup will error unless it is marked optional. Paths must be relative and may not contain the '..' path or start with '..'.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<KeyToPath>,
    /// Name of the referent. This field is effectively required, but due to backwards compatibility is allowed to be empty. Instances of this type with an empty value here are almost certainly wrong. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// optional specify whether the ConfigMap or its keys must be defined
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// A single application container that you want to run within a pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Container {
    /// Arguments to the entrypoint. The container image's CMD is used if this is not provided. Variable references $(VAR_NAME) are expanded using the container's environment. If a variable cannot be resolved, the reference in the input string will be unchanged. Double $$ are reduced to a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. "$$(VAR_NAME)" will produce the string literal "$(VAR_NAME)". Escaped references will never be expanded, regardless of whether the variable exists or not. Cannot be updated. More info: https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Entrypoint array. Not executed within a shell. The container image's ENTRYPOINT is used if this is not provided. Variable references $(VAR_NAME) are expanded using the container's environment. If a variable cannot be resolved, the reference in the input string will be unchanged. Double $$ are reduced to a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. "$$(VAR_NAME)" will produce the string literal "$(VAR_NAME)". Escaped references will never be expanded, regardless of whether the variable exists or not. Cannot be updated. More info: https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    /// List of environment variables to set in the container. Cannot be updated.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvVar>,
    /// List of sources to populate environment variables in the container. The keys defined within a source may consist of any printable ASCII characters except '='. When a key exists in multiple sources, the value associated with the last source will take precedence. Values defined by an Env with a duplicate key will take precedence. Cannot be updated.
    #[serde(default, rename = "envFrom", skip_serializing_if = "Vec::is_empty")]
    pub env_from: Vec<EnvFromSource>,
    /// Container image name. More info: https://kubernetes.io/docs/concepts/containers/images This field is optional to allow higher level config management to default or override container images in workload controllers like Deployments and StatefulSets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// Image pull policy. One of Always, Never, IfNotPresent. Defaults to Always if :latest tag is specified, or IfNotPresent otherwise. Cannot be updated. More info: https://kubernetes.io/docs/concepts/containers/images#updating-images
    #[serde(default, rename = "imagePullPolicy", skip_serializing_if = "Option::is_none")]
    pub image_pull_policy: Option<String>,
    /// Actions that the management system should take in response to container lifecycle events. Cannot be updated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<Lifecycle>,
    /// Periodic probe of container liveness. Container will be restarted if the probe fails. Cannot be updated. More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
    #[serde(default, rename = "livenessProbe", skip_serializing_if = "Option::is_none")]
    pub liveness_probe: Option<Probe>,
    /// Name of the container specified as a DNS_LABEL. Each container in a pod must have a unique name (DNS_LABEL). Cannot be updated.
    #[serde(default)]
    pub name: String,
    /// List of ports to expose from the container. Not specifying a port here DOES NOT prevent that port from being exposed. Any port which is listening on the default "0.0.0.0" address inside a container will be accessible from the network. Modifying this array with strategic merge patch may corrupt the data. For more information See https://github.com/kubernetes/kubernetes/issues/108255. Cannot be updated.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<ContainerPort>,
    /// Periodic probe of container service readiness. Container will be removed from service endpoints if the probe fails. Cannot be updated. More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
    #[serde(default, rename = "readinessProbe", skip_serializing_if = "Option::is_none")]
    pub readiness_probe: Option<Probe>,
    /// Resources resize policy for the container.
    #[serde(default, rename = "resizePolicy", skip_serializing_if = "Vec::is_empty")]
    pub resize_policy: Vec<ContainerResizePolicy>,
    /// Compute Resources required by this container. Cannot be updated. More info: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
    /// RestartPolicy defines the restart behavior of individual containers in a pod. This overrides the pod-level restart policy. When this field is not specified, the restart behavior is defined by the Pod's restart policy and the container type. Additionally, setting the RestartPolicy as "Always" for the init container will have the following effect: this init container will be continually restarted on exit until all regular containers have terminated. Once all regular containers have completed, all init containers with restartPolicy "Always" will be shut down. This lifecycle differs from normal init containers and is often referred to as a "sidecar" container. Although this init container still starts in the init container sequence, it does not wait for the container to complete before proceeding to the next init container. Instead, the next init container starts immediately after this init container is started, or after any startupProbe has successfully completed.
    #[serde(default, rename = "restartPolicy", skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>,
    /// Represents a list of rules to be checked to determine if the container should be restarted on exit. The rules are evaluated in order. Once a rule matches a container exit condition, the remaining rules are ignored. If no rule matches the container exit condition, the Container-level restart policy determines the whether the container is restarted or not. Constraints on the rules: - At most 20 rules are allowed. - Rules can have the same action. - Identical rules are not forbidden in validations. When rules are specified, container MUST set RestartPolicy explicitly even it if matches the Pod's RestartPolicy.
    #[serde(default, rename = "restartPolicyRules", skip_serializing_if = "Vec::is_empty")]
    pub restart_policy_rules: Vec<ContainerRestartRule>,
    /// SecurityContext defines the security options the container should be run with. If set, the fields of SecurityContext override the equivalent fields of PodSecurityContext. More info: https://kubernetes.io/docs/tasks/configure-pod-container/security-context/
    #[serde(default, rename = "securityContext", skip_serializing_if = "Option::is_none")]
    pub security_context: Option<SecurityContext>,
    /// StartupProbe indicates that the Pod has successfully initialized. If specified, no other probes are executed until this completes successfully. If this probe fails, the Pod will be restarted, just as if the livenessProbe failed. This can be used to provide different probe parameters at the beginning of a Pod's lifecycle, when it might take a long time to load data or warm a cache, than during steady-state operation. This cannot be updated. More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
    #[serde(default, rename = "startupProbe", skip_serializing_if = "Option::is_none")]
    pub startup_probe: Option<Probe>,
    /// Whether this container should allocate a buffer for stdin in the container runtime. If this is not set, reads from stdin in the container will always result in EOF. Default is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<bool>,
    /// Whether the container runtime should close the stdin channel after it has been opened by a single attach. When stdin is true the stdin stream will remain open across multiple attach sessions. If stdinOnce is set to true, stdin is opened on container start, is empty until the first client attaches to stdin, and then remains open and accepts data until the client disconnects, at which time stdin is closed and remains closed until the container is restarted. If this flag is false, a container processes that reads from stdin will never receive an EOF. Default is false
    #[serde(default, rename = "stdinOnce", skip_serializing_if = "Option::is_none")]
    pub stdin_once: Option<bool>,
    /// Optional: Path at which the file to which the container's termination message will be written is mounted into the container's filesystem. Message written is intended to be brief final status, such as an assertion failure message. Will be truncated by the node if greater than 4096 bytes. The total message length across all containers will be limited to 12kb. Defaults to /dev/termination-log. Cannot be updated.
    #[serde(default, rename = "terminationMessagePath", skip_serializing_if = "Option::is_none")]
    pub termination_message_path: Option<String>,
    /// Indicate how the termination message should be populated. File will use the contents of terminationMessagePath to populate the container status message on both success and failure. FallbackToLogsOnError will use the last chunk of container log output if the termination message file is empty and the container exited with an error. The log output is limited to 2048 bytes or 80 lines, whichever is smaller. Defaults to File. Cannot be updated.
    #[serde(default, rename = "terminationMessagePolicy", skip_serializing_if = "Option::is_none")]
    pub termination_message_policy: Option<String>,
    /// Whether this container should allocate a TTY for itself, also requires 'stdin' to be true. Default is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tty: Option<bool>,
    /// volumeDevices is the list of block devices to be used by the container.
    #[serde(default, rename = "volumeDevices", skip_serializing_if = "Vec::is_empty")]
    pub volume_devices: Vec<VolumeDevice>,
    /// Pod volumes to mount into the container's filesystem. Cannot be updated.
    #[serde(default, rename = "volumeMounts", skip_serializing_if = "Vec::is_empty")]
    pub volume_mounts: Vec<VolumeMount>,
    /// Container's working directory. If not specified, the container runtime's default will be used, which might be configured in the container image. Cannot be updated.
    #[serde(default, rename = "workingDir", skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
}

/// ContainerExtendedResourceRequest has the mapping of container name, extended resource name to the device request name.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerExtendedResourceRequest {
    /// The name of the container requesting resources.
    #[serde(default, rename = "containerName")]
    pub container_name: String,
    /// The name of the request in the special ResourceClaim which corresponds to the extended resource.
    #[serde(default, rename = "requestName")]
    pub request_name: String,
    /// The name of the extended resource in that container which gets backed by DRA.
    #[serde(default, rename = "resourceName")]
    pub resource_name: String,
}

/// Describe a container image
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerImage {
    /// Names by which this image is known. e.g. ["kubernetes.example/hyperkube:v1.0.7", "cloud-vendor.registry.example/cloud-vendor/hyperkube:v1.0.7"]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
    /// The size of the image in bytes.
    #[serde(default, rename = "sizeBytes", skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<i64>,
}

/// ContainerPort represents a network port in a single container.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerPort {
    /// Number of port to expose on the pod's IP address. This must be a valid port number, 0 < x < 65536.
    #[serde(default, rename = "containerPort")]
    pub container_port: i32,
    /// What host IP to bind the external port to.
    #[serde(default, rename = "hostIP", skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,
    /// Number of port to expose on the host. If specified, this must be a valid port number, 0 < x < 65536. If HostNetwork is specified, this must match ContainerPort. Most containers do not need this.
    #[serde(default, rename = "hostPort", skip_serializing_if = "Option::is_none")]
    pub host_port: Option<i32>,
    /// If specified, this must be an IANA_SVC_NAME and unique within the pod. Each named port in a pod must have a unique name. Name for the port that can be referred to by services.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Protocol for port. Must be UDP, TCP, or SCTP. Defaults to "TCP".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

/// ContainerResizePolicy represents resource resize policy for the container.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerResizePolicy {
    /// Name of the resource to which this resource resize policy applies. Supported values: cpu, memory.
    #[serde(default, rename = "resourceName")]
    pub resource_name: String,
    /// Restart policy to apply when specified resource is resized. If not specified, it defaults to NotRequired.
    #[serde(default, rename = "restartPolicy")]
    pub restart_policy: String,
}

/// ContainerRestartRule describes how a container exit is handled.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerRestartRule {
    /// Specifies the action taken on a container exit if the requirements are satisfied. The only possible value is "Restart" to restart the container.
    #[serde(default)]
    pub action: String,
    /// Represents the exit codes to check on container exits.
    #[serde(default, rename = "exitCodes", skip_serializing_if = "Option::is_none")]
    pub exit_codes: Option<ContainerRestartRuleOnExitCodes>,
}

/// ContainerRestartRuleOnExitCodes describes the condition for handling an exited container based on its exit codes.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerRestartRuleOnExitCodes {
    /// Represents the relationship between the container exit code(s) and the specified values. Possible values are: - In: the requirement is satisfied if the container exit code is in the
    /// set of specified values.
    /// - NotIn: the requirement is satisfied if the container exit code is
    /// not in the set of specified values.
    #[serde(default)]
    pub operator: String,
    /// Specifies the set of values to check for container exit codes. At most 255 elements are allowed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<i32>,
}

/// ContainerState holds a possible state of container. Only one of its members may be specified. If none of them is specified, the default one is ContainerStateWaiting.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerState {
    /// Details about a running container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running: Option<ContainerStateRunning>,
    /// Details about a terminated container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminated: Option<ContainerStateTerminated>,
    /// Details about a waiting container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<ContainerStateWaiting>,
}

/// ContainerStateRunning is a running state of a container.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerStateRunning {
    /// Time at which the container was last (re-)started
    #[serde(default, rename = "startedAt", skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Time>,
}

/// ContainerStateTerminated is a terminated state of a container.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerStateTerminated {
    /// Container's ID in the format '<type>://<container_id>'
    #[serde(default, rename = "containerID", skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,
    /// Exit status from the last termination of the container
    #[serde(default, rename = "exitCode")]
    pub exit_code: i32,
    /// Time at which the container last terminated
    #[serde(default, rename = "finishedAt", skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Time>,
    /// Message regarding the last termination of the container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// (brief) reason from the last termination of the container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Signal from the last termination of the container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    /// Time at which previous execution of the container started
    #[serde(default, rename = "startedAt", skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Time>,
}

/// ContainerStateWaiting is a waiting state of a container.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerStateWaiting {
    /// Message regarding why the container is not yet running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// (brief) reason the container is not yet running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// ContainerStatus contains details for the current status of this container.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerStatus {
    /// AllocatedResources represents the compute resources allocated for this container by the node. Kubelet sets this value to Container.Resources.Requests upon successful pod admission and after successfully admitting desired pod resize.
    #[serde(default, rename = "allocatedResources", skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub allocated_resources: std::collections::BTreeMap<String, Quantity>,
    /// AllocatedResourcesStatus represents the status of various resources allocated for this Pod.
    #[serde(default, rename = "allocatedResourcesStatus", skip_serializing_if = "Vec::is_empty")]
    pub allocated_resources_status: Vec<ResourceStatus>,
    /// ContainerID is the ID of the container in the format '<type>://<container_id>'. Where type is a container runtime identifier, returned from Version call of CRI API (for example "containerd").
    #[serde(default, rename = "containerID", skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,
    /// Image is the name of container image that the container is running. The container image may not match the image used in the PodSpec, as it may have been resolved by the runtime. More info: https://kubernetes.io/docs/concepts/containers/images.
    #[serde(default)]
    pub image: String,
    /// ImageID is the image ID of the container's image. The image ID may not match the image ID of the image used in the PodSpec, as it may have been resolved by the runtime.
    #[serde(default, rename = "imageID")]
    pub image_id: String,
    /// LastTerminationState holds the last termination state of the container to help debug container crashes and restarts. This field is not populated if the container is still running and RestartCount is 0.
    #[serde(default, rename = "lastState", skip_serializing_if = "Option::is_none")]
    pub last_state: Option<ContainerState>,
    /// Name is a DNS_LABEL representing the unique name of the container. Each container in a pod must have a unique name across all container types. Cannot be updated.
    #[serde(default)]
    pub name: String,
    /// Ready specifies whether the container is currently passing its readiness check. The value will change as readiness probes keep executing. If no readiness probes are specified, this field defaults to true once the container is fully started (see Started field).
    #[serde(default)]
    pub ready: bool,
    /// Resources represents the compute resource requests and limits that have been successfully enacted on the running container after it has been started or has been successfully resized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
    /// RestartCount holds the number of times the container has been restarted. Kubelet makes an effort to always increment the value, but there are cases when the state may be lost due to node restarts and then the value may be reset to 0. The value is never negative.
    #[serde(default, rename = "restartCount")]
    pub restart_count: i32,
    /// Started indicates whether the container has finished its postStart lifecycle hook and passed its startup probe. Initialized as false, becomes true after startupProbe is considered successful. Resets to false when the container is restarted, or if kubelet loses state temporarily. In both cases, startup probes will run again. Is always true when no startupProbe is defined and container is running and has passed the postStart lifecycle hook. The null value must be treated the same as false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started: Option<bool>,
    /// State holds details about the container's current condition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ContainerState>,
    /// StopSignal reports the effective stop signal for this container
    #[serde(default, rename = "stopSignal", skip_serializing_if = "Option::is_none")]
    pub stop_signal: Option<String>,
    /// User represents user identity information initially attached to the first process of the container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<ContainerUser>,
    /// Status of volume mounts.
    #[serde(default, rename = "volumeMounts", skip_serializing_if = "Vec::is_empty")]
    pub volume_mounts: Vec<VolumeMountStatus>,
}

/// ContainerUser represents user identity information
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerUser {
    /// Linux holds user identity information initially attached to the first process of the containers in Linux. Note that the actual running identity can be changed if the process has enough privilege to do so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linux: Option<LinuxContainerUser>,
}

/// DaemonEndpoint contains information about a single Daemon endpoint.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DaemonEndpoint {
    /// Port number of the given endpoint.
    #[serde(default, rename = "Port")]
    pub port: i32,
}

/// DaemonSetCondition describes the state of a DaemonSet at a certain point.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DaemonSetCondition {
    /// Last time the condition transitioned from one status to another.
    #[serde(default, rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<Time>,
    /// A human readable message indicating details about the transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The reason for the condition's last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Status of the condition, one of True, False, Unknown.
    #[serde(default)]
    pub status: String,
    /// Type of DaemonSet condition.
    #[serde(default, rename = "type")]
    pub r#type: String,
}

/// DaemonSetSpec is the specification of a daemon set.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DaemonSetSpec {
    /// The minimum number of seconds for which a newly created DaemonSet pod should be ready without any of its container crashing, for it to be considered available. Defaults to 0 (pod will be considered available as soon as it is ready).
    #[serde(default, rename = "minReadySeconds", skip_serializing_if = "Option::is_none")]
    pub min_ready_seconds: Option<i32>,
    /// The number of old history to retain to allow rollback. This is a pointer to distinguish between explicit zero and not specified. Defaults to 10.
    #[serde(default, rename = "revisionHistoryLimit", skip_serializing_if = "Option::is_none")]
    pub revision_history_limit: Option<i32>,
    /// A label query over pods that are managed by the daemon set. Must match in order to be controlled. It must match the pod template's labels. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
    #[serde(default)]
    pub selector: LabelSelector,
    /// An object that describes the pod that will be created. The DaemonSet will create exactly one copy of this pod on every node that matches the template's node selector (or on every node if no node selector is specified). The only allowed template.spec.restartPolicy value is "Always". More info: https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller#pod-template
    #[serde(default)]
    pub template: PodTemplateSpec,
    /// An update strategy to replace existing DaemonSet pods with new pods.
    #[serde(default, rename = "updateStrategy", skip_serializing_if = "Option::is_none")]
    pub update_strategy: Option<DaemonSetUpdateStrategy>,
}

/// DaemonSetStatus represents the current status of a daemon set.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DaemonSetStatus {
    /// Count of hash collisions for the DaemonSet. The DaemonSet controller uses this field as a collision avoidance mechanism when it needs to create the name for the newest ControllerRevision.
    #[serde(default, rename = "collisionCount", skip_serializing_if = "Option::is_none")]
    pub collision_count: Option<i32>,
    /// Represents the latest available observations of a DaemonSet's current state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<DaemonSetCondition>,
    /// The number of nodes that are running at least 1 daemon pod and are supposed to run the daemon pod. More info: https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
    #[serde(default, rename = "currentNumberScheduled")]
    pub current_number_scheduled: i32,
    /// The total number of nodes that should be running the daemon pod (including nodes correctly running the daemon pod). More info: https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
    #[serde(default, rename = "desiredNumberScheduled")]
    pub desired_number_scheduled: i32,
    /// The number of nodes that should be running the daemon pod and have one or more of the daemon pod running and available (ready for at least spec.minReadySeconds)
    #[serde(default, rename = "numberAvailable", skip_serializing_if = "Option::is_none")]
    pub number_available: Option<i32>,
    /// The number of nodes that are running the daemon pod, but are not supposed to run the daemon pod. More info: https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/
    #[serde(default, rename = "numberMisscheduled")]
    pub number_misscheduled: i32,
    /// numberReady is the number of nodes that should be running the daemon pod and have one or more of the daemon pod running with a Ready Condition.
    #[serde(default, rename = "numberReady")]
    pub number_ready: i32,
    /// The number of nodes that should be running the daemon pod and have none of the daemon pod running and available (ready for at least spec.minReadySeconds)
    #[serde(default, rename = "numberUnavailable", skip_serializing_if = "Option::is_none")]
    pub number_unavailable: Option<i32>,
    /// The most recent generation observed by the daemon set controller.
    #[serde(default, rename = "observedGeneration", skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The total number of nodes that are running updated daemon pod
    #[serde(default, rename = "updatedNumberScheduled", skip_serializing_if = "Option::is_none")]
    pub updated_number_scheduled: Option<i32>,
}

/// DaemonSetUpdateStrategy is a struct used to control the update strategy for a DaemonSet.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DaemonSetUpdateStrategy {
    /// Rolling update config params. Present only if type = "RollingUpdate".
    #[serde(default, rename = "rollingUpdate", skip_serializing_if = "Option::is_none")]
    pub rolling_update: Option<RollingUpdateDaemonSet>,
    /// Type of daemon set update. Can be "RollingUpdate" or "OnDelete". Default is RollingUpdate.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

/// DeploymentCondition describes the state of a deployment at a certain point.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DeploymentCondition {
    /// Last time the condition transitioned from one status to another.
    #[serde(default, rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<Time>,
    /// The last time this condition was updated.
    #[serde(default, rename = "lastUpdateTime", skip_serializing_if = "Option::is_none")]
    pub last_update_time: Option<Time>,
    /// A human readable message indicating details about the transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The reason for the condition's last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Status of the condition, one of True, False, Unknown.
    #[serde(default)]
    pub status: String,
    /// Type of deployment condition.
    #[serde(default, rename = "type")]
    pub r#type: String,
}

/// DeploymentSpec is the specification of the desired behavior of the Deployment.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DeploymentSpec {
    /// Minimum number of seconds for which a newly created pod should be ready without any of its container crashing, for it to be considered available. Defaults to 0 (pod will be considered available as soon as it is ready)
    #[serde(default, rename = "minReadySeconds", skip_serializing_if = "Option::is_none")]
    pub min_ready_seconds: Option<i32>,
    /// Indicates that the deployment is paused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused: Option<bool>,
    /// The maximum time in seconds for a deployment to make progress before it is considered to be failed. The deployment controller will continue to process failed deployments and a condition with a ProgressDeadlineExceeded reason will be surfaced in the deployment status. Note that progress will not be estimated during the time a deployment is paused. Defaults to 600s.
    #[serde(default, rename = "progressDeadlineSeconds", skip_serializing_if = "Option::is_none")]
    pub progress_deadline_seconds: Option<i32>,
    /// Number of desired pods. This is a pointer to distinguish between explicit zero and not specified. Defaults to 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// The number of old ReplicaSets to retain to allow rollback. This is a pointer to distinguish between explicit zero and not specified. Defaults to 10.
    #[serde(default, rename = "revisionHistoryLimit", skip_serializing_if = "Option::is_none")]
    pub revision_history_limit: Option<i32>,
    /// Label selector for pods. Existing ReplicaSets whose pods are selected by this will be the ones affected by this deployment. It must match the pod template's labels.
    #[serde(default)]
    pub selector: LabelSelector,
    /// The deployment strategy to use to replace existing pods with new ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<DeploymentStrategy>,
    /// Template describes the pods that will be created. The only allowed template.spec.restartPolicy value is "Always".
    #[serde(default)]
    pub template: PodTemplateSpec,
}

/// DeploymentStatus is the most recently observed status of the Deployment.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DeploymentStatus {
    /// Total number of available non-terminating pods (ready for at least minReadySeconds) targeted by this deployment.
    #[serde(default, rename = "availableReplicas", skip_serializing_if = "Option::is_none")]
    pub available_replicas: Option<i32>,
    /// Count of hash collisions for the Deployment. The Deployment controller uses this field as a collision avoidance mechanism when it needs to create the name for the newest ReplicaSet.
    #[serde(default, rename = "collisionCount", skip_serializing_if = "Option::is_none")]
    pub collision_count: Option<i32>,
    /// Represents the latest available observations of a deployment's current state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<DeploymentCondition>,
    /// The generation observed by the deployment controller.
    #[serde(default, rename = "observedGeneration", skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Total number of non-terminating pods targeted by this Deployment with a Ready Condition.
    #[serde(default, rename = "readyReplicas", skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    /// Total number of non-terminating pods targeted by this deployment (their labels match the selector).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// Total number of terminating pods targeted by this deployment. Terminating pods have a non-null .metadata.deletionTimestamp and have not yet reached the Failed or Succeeded .status.phase.
    #[serde(default, rename = "terminatingReplicas", skip_serializing_if = "Option::is_none")]
    pub terminating_replicas: Option<i32>,
    /// Total number of unavailable pods targeted by this deployment. This is the total number of pods that are still required for the deployment to have 100% available capacity. They may either be pods that are running but not yet available or pods that still have not been created.
    #[serde(default, rename = "unavailableReplicas", skip_serializing_if = "Option::is_none")]
    pub unavailable_replicas: Option<i32>,
    /// Total number of non-terminating pods targeted by this deployment that have the desired template spec.
    #[serde(default, rename = "updatedReplicas", skip_serializing_if = "Option::is_none")]
    pub updated_replicas: Option<i32>,
}

/// DeploymentStrategy describes how to replace existing pods with new ones.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DeploymentStrategy {
    /// Rolling update config params. Present only if DeploymentStrategyType = RollingUpdate.
    #[serde(default, rename = "rollingUpdate", skip_serializing_if = "Option::is_none")]
    pub rolling_update: Option<RollingUpdateDeployment>,
    /// Type of deployment. Can be "Recreate" or "RollingUpdate". Default is RollingUpdate.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

/// Represents downward API info for projecting into a projected volume. Note that this is identical to a downwardAPI volume source without the default mode.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DownwardAPIProjection {
    /// Items is a list of DownwardAPIVolume file
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<DownwardAPIVolumeFile>,
}

/// DownwardAPIVolumeFile represents information to create the file containing the pod field
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DownwardAPIVolumeFile {
    /// Required: Selects a field of the pod: only annotations, labels, name, namespace and uid are supported.
    #[serde(default, rename = "fieldRef", skip_serializing_if = "Option::is_none")]
    pub field_ref: Option<ObjectFieldSelector>,
    /// Optional: mode bits used to set permissions on this file, must be an octal value between 0000 and 0777 or a decimal value between 0 and 511. YAML accepts both octal and decimal values, JSON requires decimal values for mode bits. If not specified, the volume defaultMode will be used. This might be in conflict with other options that affect the file mode, like fsGroup, and the result can be other mode bits set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<i32>,
    /// Required: Path is  the relative path name of the file to be created. Must not be absolute or contain the '..' path. Must be utf-8 encoded. The first item of the relative path must not start with '..'
    #[serde(default)]
    pub path: String,
    /// Selects a resource of the container: only resources limits and requests (limits.cpu, limits.memory, requests.cpu and requests.memory) are currently supported.
    #[serde(default, rename = "resourceFieldRef", skip_serializing_if = "Option::is_none")]
    pub resource_field_ref: Option<ResourceFieldSelector>,
}

/// DownwardAPIVolumeSource represents a volume containing downward API info. Downward API volumes support ownership management and SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DownwardAPIVolumeSource {
    /// Optional: mode bits to use on created files by default. Must be a Optional: mode bits used to set permissions on created files by default. Must be an octal value between 0000 and 0777 or a decimal value between 0 and 511. YAML accepts both octal and decimal values, JSON requires decimal values for mode bits. Defaults to 0644. Directories within the path are not affected by this setting. This might be in conflict with other options that affect the file mode, like fsGroup, and the result can be other mode bits set.
    #[serde(default, rename = "defaultMode", skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<i32>,
    /// Items is a list of downward API volume file
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<DownwardAPIVolumeFile>,
}

/// Represents an empty directory for a pod. Empty directory volumes support ownership management and SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EmptyDirVolumeSource {
    /// medium represents what type of storage medium should back this directory. The default is "" which means to use the node's default medium. Must be an empty string (default) or Memory. More info: https://kubernetes.io/docs/concepts/storage/volumes#emptydir
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub medium: Option<String>,
    /// sizeLimit is the total amount of local storage required for this EmptyDir volume. The size limit is also applicable for memory medium. The maximum usage on memory medium EmptyDir would be the minimum value between the SizeLimit specified here and the sum of memory limits of all containers in a pod. The default is nil which means that the limit is undefined. More info: https://kubernetes.io/docs/concepts/storage/volumes#emptydir
    #[serde(default, rename = "sizeLimit", skip_serializing_if = "Option::is_none")]
    pub size_limit: Option<Quantity>,
}

/// EndpointAddress is a tuple that describes single IP address. Deprecated: This API is deprecated in v1.33+.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EndpointAddress {
    /// The Hostname of this endpoint
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    /// The IP of this endpoint. May not be loopback (127.0.0.0/8 or ::1), link-local (169.254.0.0/16 or fe80::/10), or link-local multicast (224.0.0.0/24 or ff02::/16).
    #[serde(default)]
    pub ip: String,
    /// Optional: Node hosting this endpoint. This can be used to determine endpoints local to a node.
    #[serde(default, rename = "nodeName", skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    /// Reference to object providing the endpoint.
    #[serde(default, rename = "targetRef", skip_serializing_if = "Option::is_none")]
    pub target_ref: Option<ObjectReference>,
}

/// EndpointPort is a tuple that describes a single port. Deprecated: This API is deprecated in v1.33+.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EndpointPort {
    /// The application protocol for this port. This is used as a hint for implementations to offer richer behavior for protocols that they understand. This field follows standard Kubernetes label syntax. Valid values are either:
    #[serde(default, rename = "appProtocol", skip_serializing_if = "Option::is_none")]
    pub app_protocol: Option<String>,
    /// The name of this port.  This must match the 'name' field in the corresponding ServicePort. Must be a DNS_LABEL. Optional only if one port is defined.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The port number of the endpoint.
    #[serde(default)]
    pub port: i32,
    /// The IP protocol for this port. Must be UDP, TCP, or SCTP. Default is TCP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

/// EndpointSubset is a group of addresses with a common set of ports. The expanded set of endpoints is the Cartesian product of Addresses x Ports. For example, given:
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EndpointSubset {
    /// IP addresses which offer the related ports that are marked as ready. These endpoints should be considered safe for load balancers and clients to utilize.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<EndpointAddress>,
    /// IP addresses which offer the related ports but are not currently marked as ready because they have not yet finished starting, have recently failed a readiness check, or have recently failed a liveness check.
    #[serde(default, rename = "notReadyAddresses", skip_serializing_if = "Vec::is_empty")]
    pub not_ready_addresses: Vec<EndpointAddress>,
    /// Port numbers available on the related IP addresses.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<EndpointPort>,
}

/// EnvFromSource represents the source of a set of ConfigMaps or Secrets
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EnvFromSource {
    /// The ConfigMap to select from
    #[serde(default, rename = "configMapRef", skip_serializing_if = "Option::is_none")]
    pub config_map_ref: Option<ConfigMapEnvSource>,
    /// Optional text to prepend to the name of each environment variable. May consist of any printable ASCII characters except '='.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// The Secret to select from
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretEnvSource>,
}

/// EnvVar represents an environment variable present in a Container.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EnvVar {
    /// Name of the environment variable. May consist of any printable ASCII characters except '='.
    #[serde(default)]
    pub name: String,
    /// Variable references $(VAR_NAME) are expanded using the previously defined environment variables in the container and any service environment variables. If a variable cannot be resolved, the reference in the input string will be unchanged. Double $$ are reduced to a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. "$$(VAR_NAME)" will produce the string literal "$(VAR_NAME)". Escaped references will never be expanded, regardless of whether the variable exists or not. Defaults to "".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Source for the environment variable's value. Cannot be used if value is not empty.
    #[serde(default, rename = "valueFrom", skip_serializing_if = "Option::is_none")]
    pub value_from: Option<EnvVarSource>,
}

/// EnvVarSource represents a source for the value of an EnvVar.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EnvVarSource {
    /// Selects a key of a ConfigMap.
    #[serde(default, rename = "configMapKeyRef", skip_serializing_if = "Option::is_none")]
    pub config_map_key_ref: Option<ConfigMapKeySelector>,
    /// Selects a field of the pod: supports metadata.name, metadata.namespace, `metadata.labels['<KEY>']`, `metadata.annotations['<KEY>']`, spec.nodeName, spec.serviceAccountName, status.hostIP, status.podIP, status.podIPs.
    #[serde(default, rename = "fieldRef", skip_serializing_if = "Option::is_none")]
    pub field_ref: Option<ObjectFieldSelector>,
    /// FileKeyRef selects a key of the env file. Requires the EnvFiles feature gate to be enabled.
    #[serde(default, rename = "fileKeyRef", skip_serializing_if = "Option::is_none")]
    pub file_key_ref: Option<FileKeySelector>,
    /// Selects a resource of the container: only resources limits and requests (limits.cpu, limits.memory, limits.ephemeral-storage, requests.cpu, requests.memory and requests.ephemeral-storage) are currently supported.
    #[serde(default, rename = "resourceFieldRef", skip_serializing_if = "Option::is_none")]
    pub resource_field_ref: Option<ResourceFieldSelector>,
    /// Selects a key of a secret in the pod's namespace
    #[serde(default, rename = "secretKeyRef", skip_serializing_if = "Option::is_none")]
    pub secret_key_ref: Option<SecretKeySelector>,
}

/// An EphemeralContainer is a temporary container that you may add to an existing Pod for user-initiated activities such as debugging. Ephemeral containers have no resource or scheduling guarantees, and they will not be restarted when they exit or when a Pod is removed or restarted. The kubelet may evict a Pod if an ephemeral container causes the Pod to exceed its resource allocation.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EphemeralContainer {
    /// Arguments to the entrypoint. The image's CMD is used if this is not provided. Variable references $(VAR_NAME) are expanded using the container's environment. If a variable cannot be resolved, the reference in the input string will be unchanged. Double $$ are reduced to a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. "$$(VAR_NAME)" will produce the string literal "$(VAR_NAME)". Escaped references will never be expanded, regardless of whether the variable exists or not. Cannot be updated. More info: https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Entrypoint array. Not executed within a shell. The image's ENTRYPOINT is used if this is not provided. Variable references $(VAR_NAME) are expanded using the container's environment. If a variable cannot be resolved, the reference in the input string will be unchanged. Double $$ are reduced to a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. "$$(VAR_NAME)" will produce the string literal "$(VAR_NAME)". Escaped references will never be expanded, regardless of whether the variable exists or not. Cannot be updated. More info: https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    /// List of environment variables to set in the container. Cannot be updated.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvVar>,
    /// List of sources to populate environment variables in the container. The keys defined within a source may consist of any printable ASCII characters except '='. When a key exists in multiple sources, the value associated with the last source will take precedence. Values defined by an Env with a duplicate key will take precedence. Cannot be updated.
    #[serde(default, rename = "envFrom", skip_serializing_if = "Vec::is_empty")]
    pub env_from: Vec<EnvFromSource>,
    /// Container image name. More info: https://kubernetes.io/docs/concepts/containers/images
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// Image pull policy. One of Always, Never, IfNotPresent. Defaults to Always if :latest tag is specified, or IfNotPresent otherwise. Cannot be updated. More info: https://kubernetes.io/docs/concepts/containers/images#updating-images
    #[serde(default, rename = "imagePullPolicy", skip_serializing_if = "Option::is_none")]
    pub image_pull_policy: Option<String>,
    /// Lifecycle is not allowed for ephemeral containers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<Lifecycle>,
    /// Probes are not allowed for ephemeral containers.
    #[serde(default, rename = "livenessProbe", skip_serializing_if = "Option::is_none")]
    pub liveness_probe: Option<Probe>,
    /// Name of the ephemeral container specified as a DNS_LABEL. This name must be unique among all containers, init containers and ephemeral containers.
    #[serde(default)]
    pub name: String,
    /// Ports are not allowed for ephemeral containers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<ContainerPort>,
    /// Probes are not allowed for ephemeral containers.
    #[serde(default, rename = "readinessProbe", skip_serializing_if = "Option::is_none")]
    pub readiness_probe: Option<Probe>,
    /// Resources resize policy for the container.
    #[serde(default, rename = "resizePolicy", skip_serializing_if = "Vec::is_empty")]
    pub resize_policy: Vec<ContainerResizePolicy>,
    /// Resources are not allowed for ephemeral containers. Ephemeral containers use spare resources already allocated to the pod.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
    /// Restart policy for the container to manage the restart behavior of each container within a pod. You cannot set this field on ephemeral containers.
    #[serde(default, rename = "restartPolicy", skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>,
    /// Represents a list of rules to be checked to determine if the container should be restarted on exit. You cannot set this field on ephemeral containers.
    #[serde(default, rename = "restartPolicyRules", skip_serializing_if = "Vec::is_empty")]
    pub restart_policy_rules: Vec<ContainerRestartRule>,
    /// Optional: SecurityContext defines the security options the ephemeral container should be run with. If set, the fields of SecurityContext override the equivalent fields of PodSecurityContext.
    #[serde(default, rename = "securityContext", skip_serializing_if = "Option::is_none")]
    pub security_context: Option<SecurityContext>,
    /// Probes are not allowed for ephemeral containers.
    #[serde(default, rename = "startupProbe", skip_serializing_if = "Option::is_none")]
    pub startup_probe: Option<Probe>,
    /// Whether this container should allocate a buffer for stdin in the container runtime. If this is not set, reads from stdin in the container will always result in EOF. Default is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<bool>,
    /// Whether the container runtime should close the stdin channel after it has been opened by a single attach. When stdin is true the stdin stream will remain open across multiple attach sessions. If stdinOnce is set to true, stdin is opened on container start, is empty until the first client attaches to stdin, and then remains open and accepts data until the client disconnects, at which time stdin is closed and remains closed until the container is restarted. If this flag is false, a container processes that reads from stdin will never receive an EOF. Default is false
    #[serde(default, rename = "stdinOnce", skip_serializing_if = "Option::is_none")]
    pub stdin_once: Option<bool>,
    /// If set, the name of the container from PodSpec that this ephemeral container targets. The ephemeral container will be run in the namespaces (IPC, PID, etc) of this container. If not set then the ephemeral container uses the namespaces configured in the Pod spec.
    #[serde(default, rename = "targetContainerName", skip_serializing_if = "Option::is_none")]
    pub target_container_name: Option<String>,
    /// Optional: Path at which the file to which the container's termination message will be written is mounted into the container's filesystem. Message written is intended to be brief final status, such as an assertion failure message. Will be truncated by the node if greater than 4096 bytes. The total message length across all containers will be limited to 12kb. Defaults to /dev/termination-log. Cannot be updated.
    #[serde(default, rename = "terminationMessagePath", skip_serializing_if = "Option::is_none")]
    pub termination_message_path: Option<String>,
    /// Indicate how the termination message should be populated. File will use the contents of terminationMessagePath to populate the container status message on both success and failure. FallbackToLogsOnError will use the last chunk of container log output if the termination message file is empty and the container exited with an error. The log output is limited to 2048 bytes or 80 lines, whichever is smaller. Defaults to File. Cannot be updated.
    #[serde(default, rename = "terminationMessagePolicy", skip_serializing_if = "Option::is_none")]
    pub termination_message_policy: Option<String>,
    /// Whether this container should allocate a TTY for itself, also requires 'stdin' to be true. Default is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tty: Option<bool>,
    /// volumeDevices is the list of block devices to be used by the container.
    #[serde(default, rename = "volumeDevices", skip_serializing_if = "Vec::is_empty")]
    pub volume_devices: Vec<VolumeDevice>,
    /// Pod volumes to mount into the container's filesystem. Subpath mounts are not allowed for ephemeral containers. Cannot be updated.
    #[serde(default, rename = "volumeMounts", skip_serializing_if = "Vec::is_empty")]
    pub volume_mounts: Vec<VolumeMount>,
    /// Container's working directory. If not specified, the container runtime's default will be used, which might be configured in the container image. Cannot be updated.
    #[serde(default, rename = "workingDir", skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
}

/// Represents an ephemeral volume that is handled by a normal storage driver.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EphemeralVolumeSource {
    /// Will be used to create a stand-alone PVC to provision the volume. The pod in which this EphemeralVolumeSource is embedded will be the owner of the PVC, i.e. the PVC will be deleted together with the pod.  The name of the PVC will be `<pod name>-<volume name>` where `<volume name>` is the name from the `PodSpec.Volumes` array entry. Pod validation will reject the pod if the concatenated name is not valid for a PVC (for example, too long).
    #[serde(default, rename = "volumeClaimTemplate", skip_serializing_if = "Option::is_none")]
    pub volume_claim_template: Option<PersistentVolumeClaimTemplate>,
}

/// ExecAction describes a "run in container" action.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ExecAction {
    /// Command is the command line to execute inside the container, the working directory for the command  is root ('/') in the container's filesystem. The command is simply exec'd, it is not run inside a shell, so traditional shell instructions ('|', etc) won't work. To use a shell, you need to explicitly call out to that shell. Exit status of 0 is treated as live/healthy and non-zero is unhealthy.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
}

/// Represents a Fibre Channel volume. Fibre Channel volumes can only be mounted as read/write once. Fibre Channel volumes support ownership management and SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FCVolumeSource {
    /// fsType is the filesystem type to mount. Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// lun is Optional: FC target lun number
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lun: Option<i32>,
    /// readOnly is Optional: Defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// targetWWNs is Optional: FC target worldwide names (WWNs)
    #[serde(default, rename = "targetWWNs", skip_serializing_if = "Vec::is_empty")]
    pub target_ww_ns: Vec<String>,
    /// wwids Optional: FC volume world wide identifiers (wwids) Either wwids or combination of targetWWNs and lun must be set, but not both simultaneously.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wwids: Vec<String>,
}

/// FileKeySelector selects a key of the env file.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FileKeySelector {
    /// The key within the env file. An invalid key will prevent the pod from starting. The keys defined within a source may consist of any printable ASCII characters except '='. During Alpha stage of the EnvFiles feature gate, the key size is limited to 128 characters.
    #[serde(default)]
    pub key: String,
    /// Specify whether the file or its key must be defined. If the file or key does not exist, then the env var is not published. If optional is set to true and the specified key does not exist, the environment variable will not be set in the Pod's containers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
    /// The path within the volume from which to select the file. Must be relative and may not contain the '..' path or start with '..'.
    #[serde(default)]
    pub path: String,
    /// The name of the volume mount containing the env file.
    #[serde(default, rename = "volumeName")]
    pub volume_name: String,
}

/// FlexPersistentVolumeSource represents a generic persistent volume resource that is provisioned/attached using an exec based plugin.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FlexPersistentVolumeSource {
    /// driver is the name of the driver to use for this volume.
    #[serde(default)]
    pub driver: String,
    /// fsType is the Filesystem type to mount. Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs", "ntfs". The default filesystem depends on FlexVolume script.
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// options is Optional: this field holds extra command options if any.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub options: std::collections::BTreeMap<String, String>,
    /// readOnly is Optional: defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretRef is Optional: SecretRef is reference to the secret object containing sensitive information to pass to the plugin scripts. This may be empty if no secret object is specified. If the secret object contains more than one secret, all secrets are passed to the plugin scripts.
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretReference>,
}

/// FlexVolume represents a generic volume resource that is provisioned/attached using an exec based plugin.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FlexVolumeSource {
    /// driver is the name of the driver to use for this volume.
    #[serde(default)]
    pub driver: String,
    /// fsType is the filesystem type to mount. Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs", "ntfs". The default filesystem depends on FlexVolume script.
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// options is Optional: this field holds extra command options if any.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub options: std::collections::BTreeMap<String, String>,
    /// readOnly is Optional: defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretRef is Optional: secretRef is reference to the secret object containing sensitive information to pass to the plugin scripts. This may be empty if no secret object is specified. If the secret object contains more than one secret, all secrets are passed to the plugin scripts.
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<LocalObjectReference>,
}

/// Represents a Flocker volume mounted by the Flocker agent. One and only one of datasetName and datasetUUID should be set. Flocker volumes do not support ownership management or SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FlockerVolumeSource {
    /// datasetName is Name of the dataset stored as metadata -> name on the dataset for Flocker should be considered as deprecated
    #[serde(default, rename = "datasetName", skip_serializing_if = "Option::is_none")]
    pub dataset_name: Option<String>,
    /// datasetUUID is the UUID of the dataset. This is unique identifier of a Flocker dataset
    #[serde(default, rename = "datasetUUID", skip_serializing_if = "Option::is_none")]
    pub dataset_uuid: Option<String>,
}

/// Represents a Persistent Disk resource in Google Compute Engine.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GCEPersistentDiskVolumeSource {
    /// fsType is filesystem type of the volume that you want to mount. Tip: Ensure that the filesystem type is supported by the host operating system. Examples: "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified. More info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// partition is the partition in the volume that you want to mount. If omitted, the default is to mount by volume name. Examples: For volume /dev/sda1, you specify the partition as "1". Similarly, the volume partition for /dev/sda is "0" (or you can leave the property empty). More info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<i32>,
    /// pdName is unique name of the PD resource in GCE. Used to identify the disk in GCE. More info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk
    #[serde(default, rename = "pdName")]
    pub pd_name: String,
    /// readOnly here will force the ReadOnly setting in VolumeMounts. Defaults to false. More info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

/// GRPCAction specifies an action involving a GRPC service.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GRPCAction {
    /// Port number of the gRPC service. Number must be in the range 1 to 65535.
    #[serde(default)]
    pub port: i32,
    /// Service is the name of the service to place in the gRPC HealthCheckRequest (see https://github.com/grpc/grpc/blob/master/doc/health-checking.md).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
}

/// Represents a volume that is populated with the contents of a git repository. Git repo volumes do not support ownership management. Git repo volumes support SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GitRepoVolumeSource {
    /// directory is the target directory name. Must not contain or start with '..'.  If '.' is supplied, the volume directory will be the git repository.  Otherwise, if specified, the volume will contain the git repository in the subdirectory with the given name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directory: Option<String>,
    /// repository is the URL
    #[serde(default)]
    pub repository: String,
    /// revision is the commit hash for the specified revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
}

/// Represents a Glusterfs mount that lasts the lifetime of a pod. Glusterfs volumes do not support ownership management or SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GlusterfsPersistentVolumeSource {
    /// endpoints is the endpoint name that details Glusterfs topology. More info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
    #[serde(default)]
    pub endpoints: String,
    /// endpointsNamespace is the namespace that contains Glusterfs endpoint. If this field is empty, the EndpointNamespace defaults to the same namespace as the bound PVC. More info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
    #[serde(default, rename = "endpointsNamespace", skip_serializing_if = "Option::is_none")]
    pub endpoints_namespace: Option<String>,
    /// path is the Glusterfs volume path. More info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
    #[serde(default)]
    pub path: String,
    /// readOnly here will force the Glusterfs volume to be mounted with read-only permissions. Defaults to false. More info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

/// Represents a Glusterfs mount that lasts the lifetime of a pod. Glusterfs volumes do not support ownership management or SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GlusterfsVolumeSource {
    /// endpoints is the endpoint name that details Glusterfs topology.
    #[serde(default)]
    pub endpoints: String,
    /// path is the Glusterfs volume path. More info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
    #[serde(default)]
    pub path: String,
    /// readOnly here will force the Glusterfs volume to be mounted with read-only permissions. Defaults to false. More info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

/// HTTPGetAction describes an action based on HTTP Get requests.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HTTPGetAction {
    /// Host name to connect to, defaults to the pod IP. You probably want to set "Host" in httpHeaders instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Custom headers to set in the request. HTTP allows repeated headers.
    #[serde(default, rename = "httpHeaders", skip_serializing_if = "Vec::is_empty")]
    pub http_headers: Vec<HTTPHeader>,
    /// Path to access on the HTTP server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Name or number of the port to access on the container. Number must be in the range 1 to 65535. Name must be an IANA_SVC_NAME.
    #[serde(default)]
    pub port: IntOrString,
    /// Scheme to use for connecting to the host. Defaults to HTTP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheme: Option<String>,
}

/// HTTPHeader describes a custom header to be used in HTTP probes
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HTTPHeader {
    /// The header field name. This will be canonicalized upon output, so case-variant names will be understood as the same header.
    #[serde(default)]
    pub name: String,
    /// The header field value
    #[serde(default)]
    pub value: String,
}

/// HostAlias holds the mapping between IP and hostnames that will be injected as an entry in the pod's hosts file.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HostAlias {
    /// Hostnames for the above IP address.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hostnames: Vec<String>,
    /// IP address of the host file entry.
    #[serde(default)]
    pub ip: String,
}

/// HostIP represents a single IP address allocated to the host.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HostIP {
    /// IP is the IP address assigned to the host
    #[serde(default)]
    pub ip: String,
}

/// Represents a host path mapped into a pod. Host path volumes do not support ownership management or SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HostPathVolumeSource {
    /// path of the directory on the host. If the path is a symlink, it will follow the link to the real path. More info: https://kubernetes.io/docs/concepts/storage/volumes#hostpath
    #[serde(default)]
    pub path: String,
    /// type for HostPath Volume Defaults to "" More info: https://kubernetes.io/docs/concepts/storage/volumes#hostpath
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

/// ISCSIPersistentVolumeSource represents an ISCSI disk. ISCSI volumes can only be mounted as read/write once. ISCSI volumes support ownership management and SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ISCSIPersistentVolumeSource {
    /// chapAuthDiscovery defines whether support iSCSI Discovery CHAP authentication
    #[serde(default, rename = "chapAuthDiscovery", skip_serializing_if = "Option::is_none")]
    pub chap_auth_discovery: Option<bool>,
    /// chapAuthSession defines whether support iSCSI Session CHAP authentication
    #[serde(default, rename = "chapAuthSession", skip_serializing_if = "Option::is_none")]
    pub chap_auth_session: Option<bool>,
    /// fsType is the filesystem type of the volume that you want to mount. Tip: Ensure that the filesystem type is supported by the host operating system. Examples: "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified. More info: https://kubernetes.io/docs/concepts/storage/volumes#iscsi
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// initiatorName is the custom iSCSI Initiator Name. If initiatorName is specified with iscsiInterface simultaneously, new iSCSI interface <target portal>:<volume name> will be created for the connection.
    #[serde(default, rename = "initiatorName", skip_serializing_if = "Option::is_none")]
    pub initiator_name: Option<String>,
    /// iqn is Target iSCSI Qualified Name.
    #[serde(default)]
    pub iqn: String,
    /// iscsiInterface is the interface Name that uses an iSCSI transport. Defaults to 'default' (tcp).
    #[serde(default, rename = "iscsiInterface", skip_serializing_if = "Option::is_none")]
    pub iscsi_interface: Option<String>,
    /// lun is iSCSI Target Lun number.
    #[serde(default)]
    pub lun: i32,
    /// portals is the iSCSI Target Portal List. The Portal is either an IP or ip_addr:port if the port is other than default (typically TCP ports 860 and 3260).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub portals: Vec<String>,
    /// readOnly here will force the ReadOnly setting in VolumeMounts. Defaults to false.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretRef is the CHAP Secret for iSCSI target and initiator authentication
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretReference>,
    /// targetPortal is iSCSI Target Portal. The Portal is either an IP or ip_addr:port if the port is other than default (typically TCP ports 860 and 3260).
    #[serde(default, rename = "targetPortal")]
    pub target_portal: String,
}

/// Represents an ISCSI disk. ISCSI volumes can only be mounted as read/write once. ISCSI volumes support ownership management and SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ISCSIVolumeSource {
    /// chapAuthDiscovery defines whether support iSCSI Discovery CHAP authentication
    #[serde(default, rename = "chapAuthDiscovery", skip_serializing_if = "Option::is_none")]
    pub chap_auth_discovery: Option<bool>,
    /// chapAuthSession defines whether support iSCSI Session CHAP authentication
    #[serde(default, rename = "chapAuthSession", skip_serializing_if = "Option::is_none")]
    pub chap_auth_session: Option<bool>,
    /// fsType is the filesystem type of the volume that you want to mount. Tip: Ensure that the filesystem type is supported by the host operating system. Examples: "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified. More info: https://kubernetes.io/docs/concepts/storage/volumes#iscsi
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// initiatorName is the custom iSCSI Initiator Name. If initiatorName is specified with iscsiInterface simultaneously, new iSCSI interface <target portal>:<volume name> will be created for the connection.
    #[serde(default, rename = "initiatorName", skip_serializing_if = "Option::is_none")]
    pub initiator_name: Option<String>,
    /// iqn is the target iSCSI Qualified Name.
    #[serde(default)]
    pub iqn: String,
    /// iscsiInterface is the interface Name that uses an iSCSI transport. Defaults to 'default' (tcp).
    #[serde(default, rename = "iscsiInterface", skip_serializing_if = "Option::is_none")]
    pub iscsi_interface: Option<String>,
    /// lun represents iSCSI Target Lun number.
    #[serde(default)]
    pub lun: i32,
    /// portals is the iSCSI Target Portal List. The portal is either an IP or ip_addr:port if the port is other than default (typically TCP ports 860 and 3260).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub portals: Vec<String>,
    /// readOnly here will force the ReadOnly setting in VolumeMounts. Defaults to false.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretRef is the CHAP Secret for iSCSI target and initiator authentication
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<LocalObjectReference>,
    /// targetPortal is iSCSI Target Portal. The Portal is either an IP or ip_addr:port if the port is other than default (typically TCP ports 860 and 3260).
    #[serde(default, rename = "targetPortal")]
    pub target_portal: String,
}

/// ImageVolumeSource represents a image volume resource.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ImageVolumeSource {
    /// Policy for pulling OCI objects. Possible values are: Always: the kubelet always attempts to pull the reference. Container creation will fail If the pull fails. Never: the kubelet never pulls the reference and only uses a local image or artifact. Container creation will fail if the reference isn't present. IfNotPresent: the kubelet pulls if the reference isn't already present on disk. Container creation will fail if the reference isn't present and the pull fails. Defaults to Always if :latest tag is specified, or IfNotPresent otherwise.
    #[serde(default, rename = "pullPolicy", skip_serializing_if = "Option::is_none")]
    pub pull_policy: Option<String>,
    /// Required: Image or artifact reference to be used. Behaves in the same way as pod.spec.containers[*].image. Pull secrets will be assembled in the same way as for the container image by looking up node credentials, SA image pull secrets, and pod spec image pull secrets. More info: https://kubernetes.io/docs/concepts/containers/images This field is optional to allow higher level config management to default or override container images in workload controllers like Deployments and StatefulSets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
}

/// IntOrString is a type that can hold an int32 or a string.  When used in JSON or YAML marshalling and unmarshalling, it produces or consumes the inner type.  This allows you to have, for example, a JSON field that can accept a name or number.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct IntOrString {
}

/// Maps a string key to a path within a volume.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct KeyToPath {
    /// key is the key to project.
    #[serde(default)]
    pub key: String,
    /// mode is Optional: mode bits used to set permissions on this file. Must be an octal value between 0000 and 0777 or a decimal value between 0 and 511. YAML accepts both octal and decimal values, JSON requires decimal values for mode bits. If not specified, the volume defaultMode will be used. This might be in conflict with other options that affect the file mode, like fsGroup, and the result can be other mode bits set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<i32>,
    /// path is the relative path of the file to map the key to. May not be an absolute path. May not contain the path element '..'. May not start with the string '..'.
    #[serde(default)]
    pub path: String,
}

/// A label selector is a label query over a set of resources. The result of matchLabels and matchExpressions are ANDed. An empty label selector matches all objects. A null label selector matches no objects.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LabelSelector {
    /// matchExpressions is a list of label selector requirements. The requirements are ANDed.
    #[serde(default, rename = "matchExpressions", skip_serializing_if = "Vec::is_empty")]
    pub match_expressions: Vec<LabelSelectorRequirement>,
    /// matchLabels is a map of {key,value} pairs. A single {key,value} in the matchLabels map is equivalent to an element of matchExpressions, whose key field is "key", the operator is "In", and the values array contains only "value". The requirements are ANDed.
    #[serde(default, rename = "matchLabels", skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub match_labels: std::collections::BTreeMap<String, String>,
}

/// A label selector requirement is a selector that contains values, a key, and an operator that relates the key and values.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LabelSelectorRequirement {
    /// key is the label key that the selector applies to.
    #[serde(default)]
    pub key: String,
    /// operator represents a key's relationship to a set of values. Valid operators are In, NotIn, Exists and DoesNotExist.
    #[serde(default)]
    pub operator: String,
    /// values is an array of string values. If the operator is In or NotIn, the values array must be non-empty. If the operator is Exists or DoesNotExist, the values array must be empty. This array is replaced during a strategic merge patch.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<String>,
}

/// Lifecycle describes actions that the management system should take in response to container lifecycle events. For the PostStart and PreStop lifecycle handlers, management of the container blocks until the action is complete, unless the container process fails, in which case the handler is aborted.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Lifecycle {
    /// PostStart is called immediately after a container is created. If the handler fails, the container is terminated and restarted according to its restart policy. Other management of the container blocks until the hook completes. More info: https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/#container-hooks
    #[serde(default, rename = "postStart", skip_serializing_if = "Option::is_none")]
    pub post_start: Option<LifecycleHandler>,
    /// PreStop is called immediately before a container is terminated due to an API request or management event such as liveness/startup probe failure, preemption, resource contention, etc. The handler is not called if the container crashes or exits. The Pod's termination grace period countdown begins before the PreStop hook is executed. Regardless of the outcome of the handler, the container will eventually terminate within the Pod's termination grace period (unless delayed by finalizers). Other management of the container blocks until the hook completes or until the termination grace period is reached. More info: https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/#container-hooks
    #[serde(default, rename = "preStop", skip_serializing_if = "Option::is_none")]
    pub pre_stop: Option<LifecycleHandler>,
    /// StopSignal defines which signal will be sent to a container when it is being stopped. If not specified, the default is defined by the container runtime in use. StopSignal can only be set for Pods with a non-empty .spec.os.name
    #[serde(default, rename = "stopSignal", skip_serializing_if = "Option::is_none")]
    pub stop_signal: Option<String>,
}

/// LifecycleHandler defines a specific action that should be taken in a lifecycle hook. One and only one of the fields, except TCPSocket must be specified.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LifecycleHandler {
    /// Exec specifies a command to execute in the container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec: Option<ExecAction>,
    /// HTTPGet specifies an HTTP GET request to perform.
    #[serde(default, rename = "httpGet", skip_serializing_if = "Option::is_none")]
    pub http_get: Option<HTTPGetAction>,
    /// Sleep represents a duration that the container should sleep.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep: Option<SleepAction>,
    /// Deprecated. TCPSocket is NOT supported as a LifecycleHandler and kept for backward compatibility. There is no validation of this field and lifecycle hooks will fail at runtime when it is specified.
    #[serde(default, rename = "tcpSocket", skip_serializing_if = "Option::is_none")]
    pub tcp_socket: Option<TCPSocketAction>,
}

/// LinuxContainerUser represents user identity information in Linux containers
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LinuxContainerUser {
    /// GID is the primary gid initially attached to the first process in the container
    #[serde(default)]
    pub gid: i64,
    /// SupplementalGroups are the supplemental groups initially attached to the first process in the container
    #[serde(default, rename = "supplementalGroups", skip_serializing_if = "Vec::is_empty")]
    pub supplemental_groups: Vec<i64>,
    /// UID is the primary uid initially attached to the first process in the container
    #[serde(default)]
    pub uid: i64,
}

/// LoadBalancerIngress represents the status of a load-balancer ingress point: traffic intended for the service should be sent to an ingress point.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LoadBalancerIngress {
    /// Hostname is set for load-balancer ingress points that are DNS based (typically AWS load-balancers)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    /// IP is set for load-balancer ingress points that are IP based (typically GCE or OpenStack load-balancers)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    /// IPMode specifies how the load-balancer IP behaves, and may only be specified when the ip field is specified. Setting this to "VIP" indicates that traffic is delivered to the node with the destination set to the load-balancer's IP and port. Setting this to "Proxy" indicates that traffic is delivered to the node or pod with the destination set to the node's IP and node port or the pod's IP and port. Service implementations may use this information to adjust traffic routing.
    #[serde(default, rename = "ipMode", skip_serializing_if = "Option::is_none")]
    pub ip_mode: Option<String>,
    /// Ports is a list of records of service ports If used, every port defined in the service should have an entry in it
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<PortStatus>,
}

/// LoadBalancerStatus represents the status of a load-balancer.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LoadBalancerStatus {
    /// Ingress is a list containing ingress points for the load-balancer. Traffic intended for the service should be sent to these ingress points.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ingress: Vec<LoadBalancerIngress>,
}

/// LocalObjectReference contains enough information to let you locate the referenced object inside the same namespace.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LocalObjectReference {
    /// Name of the referent. This field is effectively required, but due to backwards compatibility is allowed to be empty. Instances of this type with an empty value here are almost certainly wrong. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Local represents directly-attached storage with node affinity
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LocalVolumeSource {
    /// fsType is the filesystem type to mount. It applies only when the Path is a block device. Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs", "ntfs". The default value is to auto-select a filesystem if unspecified.
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// path of the full path to the volume on the node. It can be either a directory or block device (disk, partition, ...).
    #[serde(default)]
    pub path: String,
}

/// ModifyVolumeStatus represents the status object of ControllerModifyVolume operation
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ModifyVolumeStatus {
    /// status is the status of the ControllerModifyVolume operation. It can be in any of following states:
    /// - Pending
    /// Pending indicates that the PersistentVolumeClaim cannot be modified due to unmet requirements, such as
    /// the specified VolumeAttributesClass not existing.
    /// - InProgress
    /// InProgress indicates that the volume is being modified.
    /// - Infeasible
    /// Infeasible indicates that the request has been rejected as invalid by the CSI driver. To
    /// resolve the error, a valid VolumeAttributesClass needs to be specified.
    /// Note: New statuses can be added in the future. Consumers should check for unknown statuses and fail appropriately.
    #[serde(default)]
    pub status: String,
    /// targetVolumeAttributesClassName is the name of the VolumeAttributesClass the PVC currently being reconciled
    #[serde(default, rename = "targetVolumeAttributesClassName", skip_serializing_if = "Option::is_none")]
    pub target_volume_attributes_class_name: Option<String>,
}

/// Represents an NFS mount that lasts the lifetime of a pod. NFS volumes do not support ownership management or SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NFSVolumeSource {
    /// path that is exported by the NFS server. More info: https://kubernetes.io/docs/concepts/storage/volumes#nfs
    #[serde(default)]
    pub path: String,
    /// readOnly here will force the NFS export to be mounted with read-only permissions. Defaults to false. More info: https://kubernetes.io/docs/concepts/storage/volumes#nfs
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// server is the hostname or IP address of the NFS server. More info: https://kubernetes.io/docs/concepts/storage/volumes#nfs
    #[serde(default)]
    pub server: String,
}

/// NamespaceCondition contains details about state of namespace.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NamespaceCondition {
    /// Last time the condition transitioned from one status to another.
    #[serde(default, rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<Time>,
    /// Human-readable message indicating details about last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Unique, one-word, CamelCase reason for the condition's last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Status of the condition, one of True, False, Unknown.
    #[serde(default)]
    pub status: String,
    /// Type of namespace controller condition.
    #[serde(default, rename = "type")]
    pub r#type: String,
}

/// NamespaceSpec describes the attributes on a Namespace.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NamespaceSpec {
    /// Finalizers is an opaque list of values that must be empty to permanently remove object from storage. More info: https://kubernetes.io/docs/tasks/administer-cluster/namespaces/
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub finalizers: Vec<String>,
}

/// NamespaceStatus is information about the current status of a Namespace.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NamespaceStatus {
    /// Represents the latest available observations of a namespace's current state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<NamespaceCondition>,
    /// Phase is the current lifecycle phase of the namespace. More info: https://kubernetes.io/docs/tasks/administer-cluster/namespaces/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
}

/// NodeAddress contains information for the node's address.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeAddress {
    /// The node address.
    #[serde(default)]
    pub address: String,
    /// Node address type, one of Hostname, ExternalIP or InternalIP.
    #[serde(default, rename = "type")]
    pub r#type: String,
}

/// Node affinity is a group of node affinity scheduling rules.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeAffinity {
    /// The scheduler will prefer to schedule pods to nodes that satisfy the affinity expressions specified by this field, but it may choose a node that violates one or more of the expressions. The node that is most preferred is the one with the greatest sum of weights, i.e. for each node that meets all of the scheduling requirements (resource request, requiredDuringScheduling affinity expressions, etc.), compute a sum by iterating through the elements of this field and adding "weight" to the sum if the node matches the corresponding matchExpressions; the node(s) with the highest sum are the most preferred.
    #[serde(default, rename = "preferredDuringSchedulingIgnoredDuringExecution", skip_serializing_if = "Vec::is_empty")]
    pub preferred_during_scheduling_ignored_during_execution: Vec<PreferredSchedulingTerm>,
    /// If the affinity requirements specified by this field are not met at scheduling time, the pod will not be scheduled onto the node. If the affinity requirements specified by this field cease to be met at some point during pod execution (e.g. due to an update), the system may or may not try to eventually evict the pod from its node.
    #[serde(default, rename = "requiredDuringSchedulingIgnoredDuringExecution", skip_serializing_if = "Option::is_none")]
    pub required_during_scheduling_ignored_during_execution: Option<NodeSelector>,
}

/// NodeCondition contains condition information for a node.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeCondition {
    /// Last time we got an update on a given condition.
    #[serde(default, rename = "lastHeartbeatTime", skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_time: Option<Time>,
    /// Last time the condition transit from one status to another.
    #[serde(default, rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<Time>,
    /// Human readable message indicating details about last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// (brief) reason for the condition's last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Status of the condition, one of True, False, Unknown.
    #[serde(default)]
    pub status: String,
    /// Type of node condition.
    #[serde(default, rename = "type")]
    pub r#type: String,
}

/// NodeConfigSource specifies a source of node configuration. Exactly one subfield (excluding metadata) must be non-nil. This API is deprecated since 1.22
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeConfigSource {
    /// ConfigMap is a reference to a Node's ConfigMap
    #[serde(default, rename = "configMap", skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapNodeConfigSource>,
}

/// NodeConfigStatus describes the status of the config assigned by Node.Spec.ConfigSource.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeConfigStatus {
    /// Active reports the checkpointed config the node is actively using. Active will represent either the current version of the Assigned config, or the current LastKnownGood config, depending on whether attempting to use the Assigned config results in an error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<NodeConfigSource>,
    /// Assigned reports the checkpointed config the node will try to use. When Node.Spec.ConfigSource is updated, the node checkpoints the associated config payload to local disk, along with a record indicating intended config. The node refers to this record to choose its config checkpoint, and reports this record in Assigned. Assigned only updates in the status after the record has been checkpointed to disk. When the Kubelet is restarted, it tries to make the Assigned config the Active config by loading and validating the checkpointed payload identified by Assigned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assigned: Option<NodeConfigSource>,
    /// Error describes any problems reconciling the Spec.ConfigSource to the Active config. Errors may occur, for example, attempting to checkpoint Spec.ConfigSource to the local Assigned record, attempting to checkpoint the payload associated with Spec.ConfigSource, attempting to load or validate the Assigned config, etc. Errors may occur at different points while syncing config. Earlier errors (e.g. download or checkpointing errors) will not result in a rollback to LastKnownGood, and may resolve across Kubelet retries. Later errors (e.g. loading or validating a checkpointed config) will result in a rollback to LastKnownGood. In the latter case, it is usually possible to resolve the error by fixing the config assigned in Spec.ConfigSource. You can find additional information for debugging by searching the error message in the Kubelet log. Error is a human-readable description of the error state; machines can check whether or not Error is empty, but should not rely on the stability of the Error text across Kubelet versions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// LastKnownGood reports the checkpointed config the node will fall back to when it encounters an error attempting to use the Assigned config. The Assigned config becomes the LastKnownGood config when the node determines that the Assigned config is stable and correct. This is currently implemented as a 10-minute soak period starting when the local record of Assigned config is updated. If the Assigned config is Active at the end of this period, it becomes the LastKnownGood. Note that if Spec.ConfigSource is reset to nil (use local defaults), the LastKnownGood is also immediately reset to nil, because the local default config is always assumed good. You should not make assumptions about the node's method of determining config stability and correctness, as this may change or become configurable in the future.
    #[serde(default, rename = "lastKnownGood", skip_serializing_if = "Option::is_none")]
    pub last_known_good: Option<NodeConfigSource>,
}

/// NodeDaemonEndpoints lists ports opened by daemons running on the Node.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeDaemonEndpoints {
    /// Endpoint on which Kubelet is listening.
    #[serde(default, rename = "kubeletEndpoint", skip_serializing_if = "Option::is_none")]
    pub kubelet_endpoint: Option<DaemonEndpoint>,
}

/// NodeFeatures describes the set of features implemented by the CRI implementation. The features contained in the NodeFeatures should depend only on the cri implementation independent of runtime handlers.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeFeatures {
    /// SupplementalGroupsPolicy is set to true if the runtime supports SupplementalGroupsPolicy and ContainerUser.
    #[serde(default, rename = "supplementalGroupsPolicy", skip_serializing_if = "Option::is_none")]
    pub supplemental_groups_policy: Option<bool>,
}

/// NodeRuntimeHandler is a set of runtime handler information.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeRuntimeHandler {
    /// Supported features.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub features: Option<NodeRuntimeHandlerFeatures>,
    /// Runtime handler name. Empty for the default runtime handler.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// NodeRuntimeHandlerFeatures is a set of features implemented by the runtime handler.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeRuntimeHandlerFeatures {
    /// RecursiveReadOnlyMounts is set to true if the runtime handler supports RecursiveReadOnlyMounts.
    #[serde(default, rename = "recursiveReadOnlyMounts", skip_serializing_if = "Option::is_none")]
    pub recursive_read_only_mounts: Option<bool>,
    /// UserNamespaces is set to true if the runtime handler supports UserNamespaces, including for volumes.
    #[serde(default, rename = "userNamespaces", skip_serializing_if = "Option::is_none")]
    pub user_namespaces: Option<bool>,
}

/// A node selector represents the union of the results of one or more label queries over a set of nodes; that is, it represents the OR of the selectors represented by the node selector terms.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeSelector {
    /// Required. A list of node selector terms. The terms are ORed.
    #[serde(default, rename = "nodeSelectorTerms", skip_serializing_if = "Vec::is_empty")]
    pub node_selector_terms: Vec<NodeSelectorTerm>,
}

/// A node selector requirement is a selector that contains values, a key, and an operator that relates the key and values.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeSelectorRequirement {
    /// The label key that the selector applies to.
    #[serde(default)]
    pub key: String,
    /// Represents a key's relationship to a set of values. Valid operators are In, NotIn, Exists, DoesNotExist. Gt, and Lt.
    #[serde(default)]
    pub operator: String,
    /// An array of string values. If the operator is In or NotIn, the values array must be non-empty. If the operator is Exists or DoesNotExist, the values array must be empty. If the operator is Gt or Lt, the values array must have a single element, which will be interpreted as an integer. This array is replaced during a strategic merge patch.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<String>,
}

/// A null or empty node selector term matches no objects. The requirements of them are ANDed. The TopologySelectorTerm type implements a subset of the NodeSelectorTerm.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeSelectorTerm {
    /// A list of node selector requirements by node's labels.
    #[serde(default, rename = "matchExpressions", skip_serializing_if = "Vec::is_empty")]
    pub match_expressions: Vec<NodeSelectorRequirement>,
    /// A list of node selector requirements by node's fields.
    #[serde(default, rename = "matchFields", skip_serializing_if = "Vec::is_empty")]
    pub match_fields: Vec<NodeSelectorRequirement>,
}

/// NodeSpec describes the attributes that a node is created with.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeSpec {
    /// Deprecated: Previously used to specify the source of the node's configuration for the DynamicKubeletConfig feature. This feature is removed.
    #[serde(default, rename = "configSource", skip_serializing_if = "Option::is_none")]
    pub config_source: Option<NodeConfigSource>,
    /// Deprecated. Not all kubelets will set this field. Remove field after 1.13. see: https://issues.k8s.io/61966
    #[serde(default, rename = "externalID", skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    /// PodCIDR represents the pod IP range assigned to the node.
    #[serde(default, rename = "podCIDR", skip_serializing_if = "Option::is_none")]
    pub pod_cidr: Option<String>,
    /// podCIDRs represents the IP ranges assigned to the node for usage by Pods on that node. If this field is specified, the 0th entry must match the podCIDR field. It may contain at most 1 value for each of IPv4 and IPv6.
    #[serde(default, rename = "podCIDRs", skip_serializing_if = "Vec::is_empty")]
    pub pod_cid_rs: Vec<String>,
    /// ID of the node assigned by the cloud provider in the format: <ProviderName>://<ProviderSpecificNodeID>
    #[serde(default, rename = "providerID", skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    /// If specified, the node's taints.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub taints: Vec<Taint>,
    /// Unschedulable controls node schedulability of new pods. By default, node is schedulable. More info: https://kubernetes.io/docs/concepts/nodes/node/#manual-node-administration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unschedulable: Option<bool>,
}

/// NodeStatus is information about the current status of a node.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeStatus {
    /// List of addresses reachable to the node. Queried from cloud provider, if available. More info: https://kubernetes.io/docs/reference/node/node-status/#addresses Note: This field is declared as mergeable, but the merge key is not sufficiently unique, which can cause data corruption when it is merged. Callers should instead use a full-replacement patch. See https://pr.k8s.io/79391 for an example. Consumers should assume that addresses can change during the lifetime of a Node. However, there are some exceptions where this may not be possible, such as Pods that inherit a Node's address in its own status or consumers of the downward API (status.hostIP).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<NodeAddress>,
    /// Allocatable represents the resources of a node that are available for scheduling. Defaults to Capacity.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub allocatable: std::collections::BTreeMap<String, Quantity>,
    /// Capacity represents the total resources of a node. More info: https://kubernetes.io/docs/reference/node/node-status/#capacity
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub capacity: std::collections::BTreeMap<String, Quantity>,
    /// Conditions is an array of current observed node conditions. More info: https://kubernetes.io/docs/reference/node/node-status/#condition
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<NodeCondition>,
    /// Status of the config assigned to the node via the dynamic Kubelet config feature.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<NodeConfigStatus>,
    /// Endpoints of daemons running on the Node.
    #[serde(default, rename = "daemonEndpoints", skip_serializing_if = "Option::is_none")]
    pub daemon_endpoints: Option<NodeDaemonEndpoints>,
    /// Features describes the set of features implemented by the CRI implementation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub features: Option<NodeFeatures>,
    /// List of container images on this node
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<ContainerImage>,
    /// Set of ids/uuids to uniquely identify the node. More info: https://kubernetes.io/docs/reference/node/node-status/#info
    #[serde(default, rename = "nodeInfo", skip_serializing_if = "Option::is_none")]
    pub node_info: Option<NodeSystemInfo>,
    /// NodePhase is the recently observed lifecycle phase of the node. More info: https://kubernetes.io/docs/concepts/nodes/node/#phase The field is never populated, and now is deprecated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// The available runtime handlers.
    #[serde(default, rename = "runtimeHandlers", skip_serializing_if = "Vec::is_empty")]
    pub runtime_handlers: Vec<NodeRuntimeHandler>,
    /// List of volumes that are attached to the node.
    #[serde(default, rename = "volumesAttached", skip_serializing_if = "Vec::is_empty")]
    pub volumes_attached: Vec<AttachedVolume>,
    /// List of attachable volumes in use (mounted) by the node.
    #[serde(default, rename = "volumesInUse", skip_serializing_if = "Vec::is_empty")]
    pub volumes_in_use: Vec<String>,
}

/// NodeSwapStatus represents swap memory information.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeSwapStatus {
    /// Total amount of swap memory in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<i64>,
}

/// NodeSystemInfo is a set of ids/uuids to uniquely identify the node.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeSystemInfo {
    /// The Architecture reported by the node
    #[serde(default)]
    pub architecture: String,
    /// Boot ID reported by the node.
    #[serde(default, rename = "bootID")]
    pub boot_id: String,
    /// ContainerRuntime Version reported by the node through runtime remote API (e.g. containerd://1.4.2).
    #[serde(default, rename = "containerRuntimeVersion")]
    pub container_runtime_version: String,
    /// Kernel Version reported by the node from 'uname -r' (e.g. 3.16.0-0.bpo.4-amd64).
    #[serde(default, rename = "kernelVersion")]
    pub kernel_version: String,
    /// Deprecated: KubeProxy Version reported by the node.
    #[serde(default, rename = "kubeProxyVersion")]
    pub kube_proxy_version: String,
    /// Kubelet Version reported by the node.
    #[serde(default, rename = "kubeletVersion")]
    pub kubelet_version: String,
    /// MachineID reported by the node. For unique machine identification in the cluster this field is preferred. Learn more from man(5) machine-id: http://man7.org/linux/man-pages/man5/machine-id.5.html
    #[serde(default, rename = "machineID")]
    pub machine_id: String,
    /// The Operating System reported by the node
    #[serde(default, rename = "operatingSystem")]
    pub operating_system: String,
    /// OS Image reported by the node from /etc/os-release (e.g. Debian GNU/Linux 7 (wheezy)).
    #[serde(default, rename = "osImage")]
    pub os_image: String,
    /// Swap Info reported by the node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swap: Option<NodeSwapStatus>,
    /// SystemUUID reported by the node. For unique machine identification MachineID is preferred. This field is specific to Red Hat hosts https://access.redhat.com/documentation/en-us/red_hat_subscription_management/1/html/rhsm/uuid
    #[serde(default, rename = "systemUUID")]
    pub system_uuid: String,
}

/// ObjectFieldSelector selects an APIVersioned field of an object.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ObjectFieldSelector {
    /// Path of the field to select in the specified API version.
    #[serde(default, rename = "fieldPath")]
    pub field_path: String,
}

/// ObjectReference contains enough information to let you inspect or modify the referred object.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ObjectReference {
    /// If referring to a piece of an object instead of an entire object, this string should contain a valid JSON/Go field access statement, such as desiredState.manifest.containers[2]. For example, if the object reference is to a container within a pod, this would take on a value like: "spec.containers{name}" (where "name" refers to the name of the container that triggered the event) or if no container name is specified "spec.containers[2]" (container with index 2 in this pod). This syntax is chosen only to have some well-defined way of referencing a part of an object.
    #[serde(default, rename = "fieldPath", skip_serializing_if = "Option::is_none")]
    pub field_path: Option<String>,
    /// Name of the referent. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Namespace of the referent. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Specific resourceVersion to which this reference is made, if any. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#concurrency-control-and-consistency
    #[serde(default, rename = "resourceVersion", skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    /// UID of the referent. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#uids
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}

/// PersistentVolumeClaimCondition contains details about state of pvc
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolumeClaimCondition {
    /// lastProbeTime is the time we probed the condition.
    #[serde(default, rename = "lastProbeTime", skip_serializing_if = "Option::is_none")]
    pub last_probe_time: Option<Time>,
    /// lastTransitionTime is the time the condition transitioned from one status to another.
    #[serde(default, rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<Time>,
    /// message is the human-readable message indicating details about last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// reason is a unique, this should be a short, machine understandable string that gives the reason for condition's last transition. If it reports "Resizing" that means the underlying persistent volume is being resized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Status is the status of the condition. Can be True, False, Unknown. More info: https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/persistent-volume-claim-v1/#:~:text=state%20of%20pvc-,conditions.status,-(string)%2C%20required
    #[serde(default)]
    pub status: String,
    /// Type is the type of the condition. More info: https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/persistent-volume-claim-v1/#:~:text=set%20to%20%27ResizeStarted%27.-,PersistentVolumeClaimCondition,-contains%20details%20about
    #[serde(default, rename = "type")]
    pub r#type: String,
}

/// PersistentVolumeClaimSpec describes the common attributes of storage devices and allows a Source for provider-specific attributes
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolumeClaimSpec {
    /// accessModes contains the desired access modes the volume should have. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#access-modes-1
    #[serde(default, rename = "accessModes", skip_serializing_if = "Vec::is_empty")]
    pub access_modes: Vec<String>,
    /// dataSource field can be used to specify either: * An existing VolumeSnapshot object (snapshot.storage.k8s.io/VolumeSnapshot) * An existing PVC (PersistentVolumeClaim) If the provisioner or an external controller can support the specified data source, it will create a new volume based on the contents of the specified data source. When the AnyVolumeDataSource feature gate is enabled, dataSource contents will be copied to dataSourceRef, and dataSourceRef contents will be copied to dataSource when dataSourceRef.namespace is not specified. If the namespace is specified, then dataSourceRef will not be copied to dataSource.
    #[serde(default, rename = "dataSource", skip_serializing_if = "Option::is_none")]
    pub data_source: Option<TypedLocalObjectReference>,
    /// dataSourceRef specifies the object from which to populate the volume with data, if a non-empty volume is desired. This may be any object from a non-empty API group (non core object) or a PersistentVolumeClaim object. When this field is specified, volume binding will only succeed if the type of the specified object matches some installed volume populator or dynamic provisioner. This field will replace the functionality of the dataSource field and as such if both fields are non-empty, they must have the same value. For backwards compatibility, when namespace isn't specified in dataSourceRef, both fields (dataSource and dataSourceRef) will be set to the same value automatically if one of them is empty and the other is non-empty. When namespace is specified in dataSourceRef, dataSource isn't set to the same value and must be empty. There are three important differences between dataSource and dataSourceRef: * While dataSource only allows two specific types of objects, dataSourceRef
    /// allows any non-core object, as well as PersistentVolumeClaim objects.
    /// * While dataSource ignores disallowed values (dropping them), dataSourceRef
    /// preserves all values, and generates an error if a disallowed value is
    /// specified.
    /// * While dataSource only allows local objects, dataSourceRef allows objects
    /// in any namespaces.
    /// (Beta) Using this field requires the AnyVolumeDataSource feature gate to be enabled. (Alpha) Using the namespace field of dataSourceRef requires the CrossNamespaceVolumeDataSource feature gate to be enabled.
    #[serde(default, rename = "dataSourceRef", skip_serializing_if = "Option::is_none")]
    pub data_source_ref: Option<TypedObjectReference>,
    /// resources represents the minimum resources the volume should have. If RecoverVolumeExpansionFailure feature is enabled users are allowed to specify resource requirements that are lower than previous value but must still be higher than capacity recorded in the status field of the claim. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#resources
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<VolumeResourceRequirements>,
    /// selector is a label query over volumes to consider for binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,
    /// storageClassName is the name of the StorageClass required by the claim. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#class-1
    #[serde(default, rename = "storageClassName", skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,
    /// volumeAttributesClassName may be used to set the VolumeAttributesClass used by this claim. If specified, the CSI driver will create or update the volume with the attributes defined in the corresponding VolumeAttributesClass. This has a different purpose than storageClassName, it can be changed after the claim is created. An empty string or nil value indicates that no VolumeAttributesClass will be applied to the claim. If the claim enters an Infeasible error state, this field can be reset to its previous value (including nil) to cancel the modification. If the resource referred to by volumeAttributesClass does not exist, this PersistentVolumeClaim will be set to a Pending state, as reflected by the modifyVolumeStatus field, until such as a resource exists. More info: https://kubernetes.io/docs/concepts/storage/volume-attributes-classes/
    #[serde(default, rename = "volumeAttributesClassName", skip_serializing_if = "Option::is_none")]
    pub volume_attributes_class_name: Option<String>,
    /// volumeMode defines what type of volume is required by the claim. Value of Filesystem is implied when not included in claim spec.
    #[serde(default, rename = "volumeMode", skip_serializing_if = "Option::is_none")]
    pub volume_mode: Option<String>,
    /// volumeName is the binding reference to the PersistentVolume backing this claim.
    #[serde(default, rename = "volumeName", skip_serializing_if = "Option::is_none")]
    pub volume_name: Option<String>,
}

/// PersistentVolumeClaimStatus is the current status of a persistent volume claim.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolumeClaimStatus {
    /// accessModes contains the actual access modes the volume backing the PVC has. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#access-modes-1
    #[serde(default, rename = "accessModes", skip_serializing_if = "Vec::is_empty")]
    pub access_modes: Vec<String>,
    /// allocatedResourceStatuses stores status of resource being resized for the given PVC. Key names follow standard Kubernetes label syntax. Valid values are either:
    /// * Un-prefixed keys:
    /// - storage - the capacity of the volume.
    /// * Custom resources must use implementation-defined prefixed names such as "example.com/my-custom-resource"
    /// Apart from above values - keys that are unprefixed or have kubernetes.io prefix are considered reserved and hence may not be used.
    #[serde(default, rename = "allocatedResourceStatuses", skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub allocated_resource_statuses: std::collections::BTreeMap<String, String>,
    /// allocatedResources tracks the resources allocated to a PVC including its capacity. Key names follow standard Kubernetes label syntax. Valid values are either:
    /// * Un-prefixed keys:
    /// - storage - the capacity of the volume.
    /// * Custom resources must use implementation-defined prefixed names such as "example.com/my-custom-resource"
    /// Apart from above values - keys that are unprefixed or have kubernetes.io prefix are considered reserved and hence may not be used.
    #[serde(default, rename = "allocatedResources", skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub allocated_resources: std::collections::BTreeMap<String, Quantity>,
    /// capacity represents the actual resources of the underlying volume.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub capacity: std::collections::BTreeMap<String, Quantity>,
    /// conditions is the current Condition of persistent volume claim. If underlying persistent volume is being resized then the Condition will be set to 'Resizing'.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<PersistentVolumeClaimCondition>,
    /// currentVolumeAttributesClassName is the current name of the VolumeAttributesClass the PVC is using. When unset, there is no VolumeAttributeClass applied to this PersistentVolumeClaim
    #[serde(default, rename = "currentVolumeAttributesClassName", skip_serializing_if = "Option::is_none")]
    pub current_volume_attributes_class_name: Option<String>,
    /// ModifyVolumeStatus represents the status object of ControllerModifyVolume operation. When this is unset, there is no ModifyVolume operation being attempted.
    #[serde(default, rename = "modifyVolumeStatus", skip_serializing_if = "Option::is_none")]
    pub modify_volume_status: Option<ModifyVolumeStatus>,
    /// phase represents the current phase of PersistentVolumeClaim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
}

/// PersistentVolumeClaimTemplate is used to produce PersistentVolumeClaim objects as part of an EphemeralVolumeSource.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolumeClaimTemplate {
    /// May contain labels and annotations that will be copied into the PVC when creating it. No other fields are allowed and will be rejected during validation.
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// The specification for the PersistentVolumeClaim. The entire content is copied unchanged into the PVC that gets created from this template. The same fields as in a PersistentVolumeClaim are also valid here.
    #[serde(default)]
    pub spec: PersistentVolumeClaimSpec,
}

/// PersistentVolumeClaimVolumeSource references the user's PVC in the same namespace. This volume finds the bound PV and mounts that volume for the pod. A PersistentVolumeClaimVolumeSource is, essentially, a wrapper around another type of volume that is owned by someone else (the system).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolumeClaimVolumeSource {
    /// claimName is the name of a PersistentVolumeClaim in the same namespace as the pod using this volume. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
    #[serde(default, rename = "claimName")]
    pub claim_name: String,
    /// readOnly Will force the ReadOnly setting in VolumeMounts. Default false.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

/// PersistentVolumeSpec is the specification of a persistent volume.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolumeSpec {
    /// accessModes contains all ways the volume can be mounted. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#access-modes
    #[serde(default, rename = "accessModes", skip_serializing_if = "Vec::is_empty")]
    pub access_modes: Vec<String>,
    /// awsElasticBlockStore represents an AWS Disk resource that is attached to a kubelet's host machine and then exposed to the pod. Deprecated: AWSElasticBlockStore is deprecated. All operations for the in-tree awsElasticBlockStore type are redirected to the ebs.csi.aws.com CSI driver. More info: https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore
    #[serde(default, rename = "awsElasticBlockStore", skip_serializing_if = "Option::is_none")]
    pub aws_elastic_block_store: Option<AWSElasticBlockStoreVolumeSource>,
    /// azureDisk represents an Azure Data Disk mount on the host and bind mount to the pod. Deprecated: AzureDisk is deprecated. All operations for the in-tree azureDisk type are redirected to the disk.csi.azure.com CSI driver.
    #[serde(default, rename = "azureDisk", skip_serializing_if = "Option::is_none")]
    pub azure_disk: Option<AzureDiskVolumeSource>,
    /// azureFile represents an Azure File Service mount on the host and bind mount to the pod. Deprecated: AzureFile is deprecated. All operations for the in-tree azureFile type are redirected to the file.csi.azure.com CSI driver.
    #[serde(default, rename = "azureFile", skip_serializing_if = "Option::is_none")]
    pub azure_file: Option<AzureFilePersistentVolumeSource>,
    /// capacity is the description of the persistent volume's resources and capacity. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#capacity
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub capacity: std::collections::BTreeMap<String, Quantity>,
    /// cephFS represents a Ceph FS mount on the host that shares a pod's lifetime. Deprecated: CephFS is deprecated and the in-tree cephfs type is no longer supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cephfs: Option<CephFSPersistentVolumeSource>,
    /// cinder represents a cinder volume attached and mounted on kubelets host machine. Deprecated: Cinder is deprecated. All operations for the in-tree cinder type are redirected to the cinder.csi.openstack.org CSI driver. More info: https://examples.k8s.io/mysql-cinder-pd/README.md
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cinder: Option<CinderPersistentVolumeSource>,
    /// claimRef is part of a bi-directional binding between PersistentVolume and PersistentVolumeClaim. Expected to be non-nil when bound. claim.VolumeName is the authoritative bind between PV and PVC. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#binding
    #[serde(default, rename = "claimRef", skip_serializing_if = "Option::is_none")]
    pub claim_ref: Option<ObjectReference>,
    /// csi represents storage that is handled by an external CSI driver.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub csi: Option<CSIPersistentVolumeSource>,
    /// fc represents a Fibre Channel resource that is attached to a kubelet's host machine and then exposed to the pod.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fc: Option<FCVolumeSource>,
    /// flexVolume represents a generic volume resource that is provisioned/attached using an exec based plugin. Deprecated: FlexVolume is deprecated. Consider using a CSIDriver instead.
    #[serde(default, rename = "flexVolume", skip_serializing_if = "Option::is_none")]
    pub flex_volume: Option<FlexPersistentVolumeSource>,
    /// flocker represents a Flocker volume attached to a kubelet's host machine and exposed to the pod for its usage. This depends on the Flocker control service being running. Deprecated: Flocker is deprecated and the in-tree flocker type is no longer supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flocker: Option<FlockerVolumeSource>,
    /// gcePersistentDisk represents a GCE Disk resource that is attached to a kubelet's host machine and then exposed to the pod. Provisioned by an admin. Deprecated: GCEPersistentDisk is deprecated. All operations for the in-tree gcePersistentDisk type are redirected to the pd.csi.storage.gke.io CSI driver. More info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk
    #[serde(default, rename = "gcePersistentDisk", skip_serializing_if = "Option::is_none")]
    pub gce_persistent_disk: Option<GCEPersistentDiskVolumeSource>,
    /// glusterfs represents a Glusterfs volume that is attached to a host and exposed to the pod. Provisioned by an admin. Deprecated: Glusterfs is deprecated and the in-tree glusterfs type is no longer supported. More info: https://examples.k8s.io/volumes/glusterfs/README.md
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub glusterfs: Option<GlusterfsPersistentVolumeSource>,
    /// hostPath represents a directory on the host. Provisioned by a developer or tester. This is useful for single-node development and testing only! On-host storage is not supported in any way and WILL NOT WORK in a multi-node cluster. More info: https://kubernetes.io/docs/concepts/storage/volumes#hostpath
    #[serde(default, rename = "hostPath", skip_serializing_if = "Option::is_none")]
    pub host_path: Option<HostPathVolumeSource>,
    /// iscsi represents an ISCSI Disk resource that is attached to a kubelet's host machine and then exposed to the pod. Provisioned by an admin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iscsi: Option<ISCSIPersistentVolumeSource>,
    /// local represents directly-attached storage with node affinity
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<LocalVolumeSource>,
    /// mountOptions is the list of mount options, e.g. ["ro", "soft"]. Not validated - mount will simply fail if one is invalid. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes/#mount-options
    #[serde(default, rename = "mountOptions", skip_serializing_if = "Vec::is_empty")]
    pub mount_options: Vec<String>,
    /// nfs represents an NFS mount on the host. Provisioned by an admin. More info: https://kubernetes.io/docs/concepts/storage/volumes#nfs
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nfs: Option<NFSVolumeSource>,
    /// nodeAffinity defines constraints that limit what nodes this volume can be accessed from. This field influences the scheduling of pods that use this volume.
    #[serde(default, rename = "nodeAffinity", skip_serializing_if = "Option::is_none")]
    pub node_affinity: Option<VolumeNodeAffinity>,
    /// persistentVolumeReclaimPolicy defines what happens to a persistent volume when released from its claim. Valid options are Retain (default for manually created PersistentVolumes), Delete (default for dynamically provisioned PersistentVolumes), and Recycle (deprecated). Recycle must be supported by the volume plugin underlying this PersistentVolume. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#reclaiming
    #[serde(default, rename = "persistentVolumeReclaimPolicy", skip_serializing_if = "Option::is_none")]
    pub persistent_volume_reclaim_policy: Option<String>,
    /// photonPersistentDisk represents a PhotonController persistent disk attached and mounted on kubelets host machine. Deprecated: PhotonPersistentDisk is deprecated and the in-tree photonPersistentDisk type is no longer supported.
    #[serde(default, rename = "photonPersistentDisk", skip_serializing_if = "Option::is_none")]
    pub photon_persistent_disk: Option<PhotonPersistentDiskVolumeSource>,
    /// portworxVolume represents a portworx volume attached and mounted on kubelets host machine. Deprecated: PortworxVolume is deprecated. All operations for the in-tree portworxVolume type are redirected to the pxd.portworx.com CSI driver when the CSIMigrationPortworx feature-gate is on.
    #[serde(default, rename = "portworxVolume", skip_serializing_if = "Option::is_none")]
    pub portworx_volume: Option<PortworxVolumeSource>,
    /// quobyte represents a Quobyte mount on the host that shares a pod's lifetime. Deprecated: Quobyte is deprecated and the in-tree quobyte type is no longer supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quobyte: Option<QuobyteVolumeSource>,
    /// rbd represents a Rados Block Device mount on the host that shares a pod's lifetime. Deprecated: RBD is deprecated and the in-tree rbd type is no longer supported. More info: https://examples.k8s.io/volumes/rbd/README.md
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rbd: Option<RBDPersistentVolumeSource>,
    /// scaleIO represents a ScaleIO persistent volume attached and mounted on Kubernetes nodes. Deprecated: ScaleIO is deprecated and the in-tree scaleIO type is no longer supported.
    #[serde(default, rename = "scaleIO", skip_serializing_if = "Option::is_none")]
    pub scale_io: Option<ScaleIOPersistentVolumeSource>,
    /// storageClassName is the name of StorageClass to which this persistent volume belongs. Empty value means that this volume does not belong to any StorageClass.
    #[serde(default, rename = "storageClassName", skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,
    /// storageOS represents a StorageOS volume that is attached to the kubelet's host machine and mounted into the pod. Deprecated: StorageOS is deprecated and the in-tree storageos type is no longer supported. More info: https://examples.k8s.io/volumes/storageos/README.md
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storageos: Option<StorageOSPersistentVolumeSource>,
    /// Name of VolumeAttributesClass to which this persistent volume belongs. Empty value is not allowed. When this field is not set, it indicates that this volume does not belong to any VolumeAttributesClass. This field is mutable and can be changed by the CSI driver after a volume has been updated successfully to a new class. For an unbound PersistentVolume, the volumeAttributesClassName will be matched with unbound PersistentVolumeClaims during the binding process.
    #[serde(default, rename = "volumeAttributesClassName", skip_serializing_if = "Option::is_none")]
    pub volume_attributes_class_name: Option<String>,
    /// volumeMode defines if a volume is intended to be used with a formatted filesystem or to remain in raw block state. Value of Filesystem is implied when not included in spec.
    #[serde(default, rename = "volumeMode", skip_serializing_if = "Option::is_none")]
    pub volume_mode: Option<String>,
    /// vsphereVolume represents a vSphere volume attached and mounted on kubelets host machine. Deprecated: VsphereVolume is deprecated. All operations for the in-tree vsphereVolume type are redirected to the csi.vsphere.vmware.com CSI driver.
    #[serde(default, rename = "vsphereVolume", skip_serializing_if = "Option::is_none")]
    pub vsphere_volume: Option<VsphereVirtualDiskVolumeSource>,
}

/// PersistentVolumeStatus is the current status of a persistent volume.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolumeStatus {
    /// lastPhaseTransitionTime is the time the phase transitioned from one to another and automatically resets to current time everytime a volume phase transitions.
    #[serde(default, rename = "lastPhaseTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_phase_transition_time: Option<Time>,
    /// message is a human-readable message indicating details about why the volume is in this state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// phase indicates if a volume is available, bound to a claim, or released by a claim. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#phase
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// reason is a brief CamelCase string that describes any failure and is meant for machine parsing and tidy display in the CLI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Represents a Photon Controller persistent disk resource.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PhotonPersistentDiskVolumeSource {
    /// fsType is the filesystem type to mount. Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// pdID is the ID that identifies Photon Controller persistent disk
    #[serde(default, rename = "pdID")]
    pub pd_id: String,
}

/// Pod affinity is a group of inter pod affinity scheduling rules.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodAffinity {
    /// The scheduler will prefer to schedule pods to nodes that satisfy the affinity expressions specified by this field, but it may choose a node that violates one or more of the expressions. The node that is most preferred is the one with the greatest sum of weights, i.e. for each node that meets all of the scheduling requirements (resource request, requiredDuringScheduling affinity expressions, etc.), compute a sum by iterating through the elements of this field and adding "weight" to the sum if the node has pods which matches the corresponding podAffinityTerm; the node(s) with the highest sum are the most preferred.
    #[serde(default, rename = "preferredDuringSchedulingIgnoredDuringExecution", skip_serializing_if = "Vec::is_empty")]
    pub preferred_during_scheduling_ignored_during_execution: Vec<WeightedPodAffinityTerm>,
    /// If the affinity requirements specified by this field are not met at scheduling time, the pod will not be scheduled onto the node. If the affinity requirements specified by this field cease to be met at some point during pod execution (e.g. due to a pod label update), the system may or may not try to eventually evict the pod from its node. When there are multiple elements, the lists of nodes corresponding to each podAffinityTerm are intersected, i.e. all terms must be satisfied.
    #[serde(default, rename = "requiredDuringSchedulingIgnoredDuringExecution", skip_serializing_if = "Vec::is_empty")]
    pub required_during_scheduling_ignored_during_execution: Vec<PodAffinityTerm>,
}

/// Defines a set of pods (namely those matching the labelSelector relative to the given namespace(s)) that this pod should be co-located (affinity) or not co-located (anti-affinity) with, where co-located is defined as running on a node whose value of the label with key <topologyKey> matches that of any node on which a pod of the set of pods is running
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodAffinityTerm {
    /// A label query over a set of resources, in this case pods. If it's null, this PodAffinityTerm matches with no Pods.
    #[serde(default, rename = "labelSelector", skip_serializing_if = "Option::is_none")]
    pub label_selector: Option<LabelSelector>,
    /// MatchLabelKeys is a set of pod label keys to select which pods will be taken into consideration. The keys are used to lookup values from the incoming pod labels, those key-value labels are merged with `labelSelector` as `key in (value)` to select the group of existing pods which pods will be taken into consideration for the incoming pod's pod (anti) affinity. Keys that don't exist in the incoming pod labels will be ignored. The default value is empty. The same key is forbidden to exist in both matchLabelKeys and labelSelector. Also, matchLabelKeys cannot be set when labelSelector isn't set.
    #[serde(default, rename = "matchLabelKeys", skip_serializing_if = "Vec::is_empty")]
    pub match_label_keys: Vec<String>,
    /// MismatchLabelKeys is a set of pod label keys to select which pods will be taken into consideration. The keys are used to lookup values from the incoming pod labels, those key-value labels are merged with `labelSelector` as `key notin (value)` to select the group of existing pods which pods will be taken into consideration for the incoming pod's pod (anti) affinity. Keys that don't exist in the incoming pod labels will be ignored. The default value is empty. The same key is forbidden to exist in both mismatchLabelKeys and labelSelector. Also, mismatchLabelKeys cannot be set when labelSelector isn't set.
    #[serde(default, rename = "mismatchLabelKeys", skip_serializing_if = "Vec::is_empty")]
    pub mismatch_label_keys: Vec<String>,
    /// A label query over the set of namespaces that the term applies to. The term is applied to the union of the namespaces selected by this field and the ones listed in the namespaces field. null selector and null or empty namespaces list means "this pod's namespace". An empty selector ({}) matches all namespaces.
    #[serde(default, rename = "namespaceSelector", skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<LabelSelector>,
    /// namespaces specifies a static list of namespace names that the term applies to. The term is applied to the union of the namespaces listed in this field and the ones selected by namespaceSelector. null or empty namespaces list and null namespaceSelector means "this pod's namespace".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespaces: Vec<String>,
    /// This pod should be co-located (affinity) or not co-located (anti-affinity) with the pods matching the labelSelector in the specified namespaces, where co-located is defined as running on a node whose value of the label with key topologyKey matches that of any node on which any of the selected pods is running. Empty topologyKey is not allowed.
    #[serde(default, rename = "topologyKey")]
    pub topology_key: String,
}

/// Pod anti affinity is a group of inter pod anti affinity scheduling rules.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodAntiAffinity {
    /// The scheduler will prefer to schedule pods to nodes that satisfy the anti-affinity expressions specified by this field, but it may choose a node that violates one or more of the expressions. The node that is most preferred is the one with the greatest sum of weights, i.e. for each node that meets all of the scheduling requirements (resource request, requiredDuringScheduling anti-affinity expressions, etc.), compute a sum by iterating through the elements of this field and subtracting "weight" from the sum if the node has pods which matches the corresponding podAffinityTerm; the node(s) with the highest sum are the most preferred.
    #[serde(default, rename = "preferredDuringSchedulingIgnoredDuringExecution", skip_serializing_if = "Vec::is_empty")]
    pub preferred_during_scheduling_ignored_during_execution: Vec<WeightedPodAffinityTerm>,
    /// If the anti-affinity requirements specified by this field are not met at scheduling time, the pod will not be scheduled onto the node. If the anti-affinity requirements specified by this field cease to be met at some point during pod execution (e.g. due to a pod label update), the system may or may not try to eventually evict the pod from its node. When there are multiple elements, the lists of nodes corresponding to each podAffinityTerm are intersected, i.e. all terms must be satisfied.
    #[serde(default, rename = "requiredDuringSchedulingIgnoredDuringExecution", skip_serializing_if = "Vec::is_empty")]
    pub required_during_scheduling_ignored_during_execution: Vec<PodAffinityTerm>,
}

/// PodCertificateProjection provides a private key and X.509 certificate in the pod filesystem.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodCertificateProjection {
    /// Write the certificate chain at this path in the projected volume.
    #[serde(default, rename = "certificateChainPath", skip_serializing_if = "Option::is_none")]
    pub certificate_chain_path: Option<String>,
    /// Write the credential bundle at this path in the projected volume.
    #[serde(default, rename = "credentialBundlePath", skip_serializing_if = "Option::is_none")]
    pub credential_bundle_path: Option<String>,
    /// Write the key at this path in the projected volume.
    #[serde(default, rename = "keyPath", skip_serializing_if = "Option::is_none")]
    pub key_path: Option<String>,
    /// The type of keypair Kubelet will generate for the pod.
    #[serde(default, rename = "keyType")]
    pub key_type: String,
    /// maxExpirationSeconds is the maximum lifetime permitted for the certificate.
    #[serde(default, rename = "maxExpirationSeconds", skip_serializing_if = "Option::is_none")]
    pub max_expiration_seconds: Option<i32>,
    /// Kubelet's generated CSRs will be addressed to this signer.
    #[serde(default, rename = "signerName")]
    pub signer_name: String,
}

/// PodCondition contains details for the current condition of this pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodCondition {
    /// Last time we probed the condition.
    #[serde(default, rename = "lastProbeTime", skip_serializing_if = "Option::is_none")]
    pub last_probe_time: Option<Time>,
    /// Last time the condition transitioned from one status to another.
    #[serde(default, rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<Time>,
    /// Human-readable message indicating details about last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// If set, this represents the .metadata.generation that the pod condition was set based upon. This is an alpha field. Enable PodObservedGenerationTracking to be able to use this field.
    #[serde(default, rename = "observedGeneration", skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Unique, one-word, CamelCase reason for the condition's last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Status is the status of the condition. Can be True, False, Unknown. More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-conditions
    #[serde(default)]
    pub status: String,
    /// Type is the type of the condition. More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-conditions
    #[serde(default, rename = "type")]
    pub r#type: String,
}

/// PodDNSConfig defines the DNS parameters of a pod in addition to those generated from DNSPolicy.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodDNSConfig {
    /// A list of DNS name server IP addresses. This will be appended to the base nameservers generated from DNSPolicy. Duplicated nameservers will be removed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nameservers: Vec<String>,
    /// A list of DNS resolver options. This will be merged with the base options generated from DNSPolicy. Duplicated entries will be removed. Resolution options given in Options will override those that appear in the base DNSPolicy.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<PodDNSConfigOption>,
    /// A list of DNS search domains for host-name lookup. This will be appended to the base search paths generated from DNSPolicy. Duplicated search paths will be removed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub searches: Vec<String>,
}

/// PodDNSConfigOption defines DNS resolver options of a pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodDNSConfigOption {
    /// Name is this DNS resolver option's name. Required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Value is this DNS resolver option's value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

/// PodExtendedResourceClaimStatus is stored in the PodStatus for the extended resource requests backed by DRA. It stores the generated name for the corresponding special ResourceClaim created by the scheduler.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodExtendedResourceClaimStatus {
    /// RequestMappings identifies the mapping of <container, extended resource backed by DRA> to  device request in the generated ResourceClaim.
    #[serde(default, rename = "requestMappings", skip_serializing_if = "Vec::is_empty")]
    pub request_mappings: Vec<ContainerExtendedResourceRequest>,
    /// ResourceClaimName is the name of the ResourceClaim that was generated for the Pod in the namespace of the Pod.
    #[serde(default, rename = "resourceClaimName")]
    pub resource_claim_name: String,
}

/// PodIP represents a single IP address allocated to the pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodIP {
    /// IP is the IP address assigned to the pod
    #[serde(default)]
    pub ip: String,
}

/// PodOS defines the OS parameters of a pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodOS {
    /// Name is the name of the operating system. The currently supported values are linux and windows. Additional value may be defined in future and can be one of: https://github.com/opencontainers/runtime-spec/blob/master/config.md#platform-specific-configuration Clients should expect to handle additional values and treat unrecognized values in this field as os: null
    #[serde(default)]
    pub name: String,
}

/// PodReadinessGate contains the reference to a pod condition
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodReadinessGate {
    /// ConditionType refers to a condition in the pod's condition list with matching type.
    #[serde(default, rename = "conditionType")]
    pub condition_type: String,
}

/// PodResourceClaim references exactly one ResourceClaim, either directly or by naming a ResourceClaimTemplate which is then turned into a ResourceClaim for the pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodResourceClaim {
    /// Name uniquely identifies this resource claim inside the pod. This must be a DNS_LABEL.
    #[serde(default)]
    pub name: String,
    /// ResourceClaimName is the name of a ResourceClaim object in the same namespace as this pod.
    #[serde(default, rename = "resourceClaimName", skip_serializing_if = "Option::is_none")]
    pub resource_claim_name: Option<String>,
    /// ResourceClaimTemplateName is the name of a ResourceClaimTemplate object in the same namespace as this pod.
    #[serde(default, rename = "resourceClaimTemplateName", skip_serializing_if = "Option::is_none")]
    pub resource_claim_template_name: Option<String>,
}

/// PodResourceClaimStatus is stored in the PodStatus for each PodResourceClaim which references a ResourceClaimTemplate. It stores the generated name for the corresponding ResourceClaim.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodResourceClaimStatus {
    /// Name uniquely identifies this resource claim inside the pod. This must match the name of an entry in pod.spec.resourceClaims, which implies that the string must be a DNS_LABEL.
    #[serde(default)]
    pub name: String,
    /// ResourceClaimName is the name of the ResourceClaim that was generated for the Pod in the namespace of the Pod. If this is unset, then generating a ResourceClaim was not necessary. The pod.spec.resourceClaims entry can be ignored in this case.
    #[serde(default, rename = "resourceClaimName", skip_serializing_if = "Option::is_none")]
    pub resource_claim_name: Option<String>,
}

/// PodSchedulingGate is associated to a Pod to guard its scheduling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodSchedulingGate {
    /// Name of the scheduling gate. Each scheduling gate must have a unique name field.
    #[serde(default)]
    pub name: String,
}

/// PodSecurityContext holds pod-level security attributes and common container settings. Some fields are also present in container.securityContext.  Field values of container.securityContext take precedence over field values of PodSecurityContext.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodSecurityContext {
    /// appArmorProfile is the AppArmor options to use by the containers in this pod. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "appArmorProfile", skip_serializing_if = "Option::is_none")]
    pub app_armor_profile: Option<AppArmorProfile>,
    /// A special supplemental group that applies to all containers in a pod. Some volume types allow the Kubelet to change the ownership of that volume to be owned by the pod:
    #[serde(default, rename = "fsGroup", skip_serializing_if = "Option::is_none")]
    pub fs_group: Option<i64>,
    /// fsGroupChangePolicy defines behavior of changing ownership and permission of the volume before being exposed inside Pod. This field will only apply to volume types which support fsGroup based ownership(and permissions). It will have no effect on ephemeral volume types such as: secret, configmaps and emptydir. Valid values are "OnRootMismatch" and "Always". If not specified, "Always" is used. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "fsGroupChangePolicy", skip_serializing_if = "Option::is_none")]
    pub fs_group_change_policy: Option<String>,
    /// The GID to run the entrypoint of the container process. Uses runtime default if unset. May also be set in SecurityContext.  If set in both SecurityContext and PodSecurityContext, the value specified in SecurityContext takes precedence for that container. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "runAsGroup", skip_serializing_if = "Option::is_none")]
    pub run_as_group: Option<i64>,
    /// Indicates that the container must run as a non-root user. If true, the Kubelet will validate the image at runtime to ensure that it does not run as UID 0 (root) and fail to start the container if it does. If unset or false, no such validation will be performed. May also be set in SecurityContext.  If set in both SecurityContext and PodSecurityContext, the value specified in SecurityContext takes precedence.
    #[serde(default, rename = "runAsNonRoot", skip_serializing_if = "Option::is_none")]
    pub run_as_non_root: Option<bool>,
    /// The UID to run the entrypoint of the container process. Defaults to user specified in image metadata if unspecified. May also be set in SecurityContext.  If set in both SecurityContext and PodSecurityContext, the value specified in SecurityContext takes precedence for that container. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "runAsUser", skip_serializing_if = "Option::is_none")]
    pub run_as_user: Option<i64>,
    /// seLinuxChangePolicy defines how the container's SELinux label is applied to all volumes used by the Pod. It has no effect on nodes that do not support SELinux or to volumes does not support SELinux. Valid values are "MountOption" and "Recursive".
    #[serde(default, rename = "seLinuxChangePolicy", skip_serializing_if = "Option::is_none")]
    pub se_linux_change_policy: Option<String>,
    /// The SELinux context to be applied to all containers. If unspecified, the container runtime will allocate a random SELinux context for each container.  May also be set in SecurityContext.  If set in both SecurityContext and PodSecurityContext, the value specified in SecurityContext takes precedence for that container. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "seLinuxOptions", skip_serializing_if = "Option::is_none")]
    pub se_linux_options: Option<SELinuxOptions>,
    /// The seccomp options to use by the containers in this pod. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "seccompProfile", skip_serializing_if = "Option::is_none")]
    pub seccomp_profile: Option<SeccompProfile>,
    /// A list of groups applied to the first process run in each container, in addition to the container's primary GID and fsGroup (if specified).  If the SupplementalGroupsPolicy feature is enabled, the supplementalGroupsPolicy field determines whether these are in addition to or instead of any group memberships defined in the container image. If unspecified, no additional groups are added, though group memberships defined in the container image may still be used, depending on the supplementalGroupsPolicy field. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "supplementalGroups", skip_serializing_if = "Vec::is_empty")]
    pub supplemental_groups: Vec<i64>,
    /// Defines how supplemental groups of the first container processes are calculated. Valid values are "Merge" and "Strict". If not specified, "Merge" is used. (Alpha) Using the field requires the SupplementalGroupsPolicy feature gate to be enabled and the container runtime must implement support for this feature. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "supplementalGroupsPolicy", skip_serializing_if = "Option::is_none")]
    pub supplemental_groups_policy: Option<String>,
    /// Sysctls hold a list of namespaced sysctls used for the pod. Pods with unsupported sysctls (by the container runtime) might fail to launch. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sysctls: Vec<Sysctl>,
    /// The Windows specific settings applied to all containers. If unspecified, the options within a container's SecurityContext will be used. If set in both SecurityContext and PodSecurityContext, the value specified in SecurityContext takes precedence. Note that this field cannot be set when spec.os.name is linux.
    #[serde(default, rename = "windowsOptions", skip_serializing_if = "Option::is_none")]
    pub windows_options: Option<WindowsSecurityContextOptions>,
}

/// PodSpec is a description of a pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodSpec {
    /// Optional duration in seconds the pod may be active on the node relative to StartTime before the system will actively try to mark it failed and kill associated containers. Value must be a positive integer.
    #[serde(default, rename = "activeDeadlineSeconds", skip_serializing_if = "Option::is_none")]
    pub active_deadline_seconds: Option<i64>,
    /// If specified, the pod's scheduling constraints
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<Affinity>,
    /// AutomountServiceAccountToken indicates whether a service account token should be automatically mounted.
    #[serde(default, rename = "automountServiceAccountToken", skip_serializing_if = "Option::is_none")]
    pub automount_service_account_token: Option<bool>,
    /// List of containers belonging to the pod. Containers cannot currently be added or removed. There must be at least one container in a Pod. Cannot be updated.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub containers: Vec<Container>,
    /// Specifies the DNS parameters of a pod. Parameters specified here will be merged to the generated DNS configuration based on DNSPolicy.
    #[serde(default, rename = "dnsConfig", skip_serializing_if = "Option::is_none")]
    pub dns_config: Option<PodDNSConfig>,
    /// Set DNS policy for the pod. Defaults to "ClusterFirst". Valid values are 'ClusterFirstWithHostNet', 'ClusterFirst', 'Default' or 'None'. DNS parameters given in DNSConfig will be merged with the policy selected with DNSPolicy. To have DNS options set along with hostNetwork, you have to specify DNS policy explicitly to 'ClusterFirstWithHostNet'.
    #[serde(default, rename = "dnsPolicy", skip_serializing_if = "Option::is_none")]
    pub dns_policy: Option<String>,
    /// EnableServiceLinks indicates whether information about services should be injected into pod's environment variables, matching the syntax of Docker links. Optional: Defaults to true.
    #[serde(default, rename = "enableServiceLinks", skip_serializing_if = "Option::is_none")]
    pub enable_service_links: Option<bool>,
    /// List of ephemeral containers run in this pod. Ephemeral containers may be run in an existing pod to perform user-initiated actions such as debugging. This list cannot be specified when creating a pod, and it cannot be modified by updating the pod spec. In order to add an ephemeral container to an existing pod, use the pod's ephemeralcontainers subresource.
    #[serde(default, rename = "ephemeralContainers", skip_serializing_if = "Vec::is_empty")]
    pub ephemeral_containers: Vec<EphemeralContainer>,
    /// HostAliases is an optional list of hosts and IPs that will be injected into the pod's hosts file if specified.
    #[serde(default, rename = "hostAliases", skip_serializing_if = "Vec::is_empty")]
    pub host_aliases: Vec<HostAlias>,
    /// Use the host's ipc namespace. Optional: Default to false.
    #[serde(default, rename = "hostIPC", skip_serializing_if = "Option::is_none")]
    pub host_ipc: Option<bool>,
    /// Host networking requested for this pod. Use the host's network namespace. When using HostNetwork you should specify ports so the scheduler is aware. When `hostNetwork` is true, specified `hostPort` fields in port definitions must match `containerPort`, and unspecified `hostPort` fields in port definitions are defaulted to match `containerPort`. Default to false.
    #[serde(default, rename = "hostNetwork", skip_serializing_if = "Option::is_none")]
    pub host_network: Option<bool>,
    /// Use the host's pid namespace. Optional: Default to false.
    #[serde(default, rename = "hostPID", skip_serializing_if = "Option::is_none")]
    pub host_pid: Option<bool>,
    /// Use the host's user namespace. Optional: Default to true. If set to true or not present, the pod will be run in the host user namespace, useful for when the pod needs a feature only available to the host user namespace, such as loading a kernel module with CAP_SYS_MODULE. When set to false, a new userns is created for the pod. Setting false is useful for mitigating container breakout vulnerabilities even allowing users to run their containers as root without actually having root privileges on the host. This field is alpha-level and is only honored by servers that enable the UserNamespacesSupport feature.
    #[serde(default, rename = "hostUsers", skip_serializing_if = "Option::is_none")]
    pub host_users: Option<bool>,
    /// Specifies the hostname of the Pod If not specified, the pod's hostname will be set to a system-defined value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    /// HostnameOverride specifies an explicit override for the pod's hostname as perceived by the pod. This field only specifies the pod's hostname and does not affect its DNS records. When this field is set to a non-empty string: - It takes precedence over the values set in `hostname` and `subdomain`. - The Pod's hostname will be set to this value. - `setHostnameAsFQDN` must be nil or set to false. - `hostNetwork` must be set to false.
    #[serde(default, rename = "hostnameOverride", skip_serializing_if = "Option::is_none")]
    pub hostname_override: Option<String>,
    /// ImagePullSecrets is an optional list of references to secrets in the same namespace to use for pulling any of the images used by this PodSpec. If specified, these secrets will be passed to individual puller implementations for them to use. More info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod
    #[serde(default, rename = "imagePullSecrets", skip_serializing_if = "Vec::is_empty")]
    pub image_pull_secrets: Vec<LocalObjectReference>,
    /// List of initialization containers belonging to the pod. Init containers are executed in order prior to containers being started. If any init container fails, the pod is considered to have failed and is handled according to its restartPolicy. The name for an init container or normal container must be unique among all containers. Init containers may not have Lifecycle actions, Readiness probes, Liveness probes, or Startup probes. The resourceRequirements of an init container are taken into account during scheduling by finding the highest request/limit for each resource type, and then using the max of that value or the sum of the normal containers. Limits are applied to init containers in a similar fashion. Init containers cannot currently be added or removed. Cannot be updated. More info: https://kubernetes.io/docs/concepts/workloads/pods/init-containers/
    #[serde(default, rename = "initContainers", skip_serializing_if = "Vec::is_empty")]
    pub init_containers: Vec<Container>,
    /// NodeName indicates in which node this pod is scheduled. If empty, this pod is a candidate for scheduling by the scheduler defined in schedulerName. Once this field is set, the kubelet for this node becomes responsible for the lifecycle of this pod. This field should not be used to express a desire for the pod to be scheduled on a specific node. https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodename
    #[serde(default, rename = "nodeName", skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    /// NodeSelector is a selector which must be true for the pod to fit on a node. Selector which must match a node's labels for the pod to be scheduled on that node. More info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/
    #[serde(default, rename = "nodeSelector", skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub node_selector: std::collections::BTreeMap<String, String>,
    /// Specifies the OS of the containers in the pod. Some pod and container fields are restricted if this is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub os: Option<PodOS>,
    /// Overhead represents the resource overhead associated with running a pod for a given RuntimeClass. This field will be autopopulated at admission time by the RuntimeClass admission controller. If the RuntimeClass admission controller is enabled, overhead must not be set in Pod create requests. The RuntimeClass admission controller will reject Pod create requests which have the overhead already set. If RuntimeClass is configured and selected in the PodSpec, Overhead will be set to the value defined in the corresponding RuntimeClass, otherwise it will remain unset and treated as zero. More info: https://git.k8s.io/enhancements/keps/sig-node/688-pod-overhead/README.md
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub overhead: std::collections::BTreeMap<String, Quantity>,
    /// PreemptionPolicy is the Policy for preempting pods with lower priority. One of Never, PreemptLowerPriority. Defaults to PreemptLowerPriority if unset.
    #[serde(default, rename = "preemptionPolicy", skip_serializing_if = "Option::is_none")]
    pub preemption_policy: Option<String>,
    /// The priority value. Various system components use this field to find the priority of the pod. When Priority Admission Controller is enabled, it prevents users from setting this field. The admission controller populates this field from PriorityClassName. The higher the value, the higher the priority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    /// If specified, indicates the pod's priority. "system-node-critical" and "system-cluster-critical" are two special keywords which indicate the highest priorities with the former being the highest priority. Any other name must be defined by creating a PriorityClass object with that name. If not specified, the pod priority will be default or zero if there is no default.
    #[serde(default, rename = "priorityClassName", skip_serializing_if = "Option::is_none")]
    pub priority_class_name: Option<String>,
    /// If specified, all readiness gates will be evaluated for pod readiness. A pod is ready when all its containers are ready AND all conditions specified in the readiness gates have status equal to "True" More info: https://git.k8s.io/enhancements/keps/sig-network/580-pod-readiness-gates
    #[serde(default, rename = "readinessGates", skip_serializing_if = "Vec::is_empty")]
    pub readiness_gates: Vec<PodReadinessGate>,
    /// ResourceClaims defines which ResourceClaims must be allocated and reserved before the Pod is allowed to start. The resources will be made available to those containers which consume them by name.
    #[serde(default, rename = "resourceClaims", skip_serializing_if = "Vec::is_empty")]
    pub resource_claims: Vec<PodResourceClaim>,
    /// Resources is the total amount of CPU and Memory resources required by all containers in the pod. It supports specifying Requests and Limits for "cpu", "memory" and "hugepages-" resource names only. ResourceClaims are not supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
    /// Restart policy for all containers within the pod. One of Always, OnFailure, Never. In some contexts, only a subset of those values may be permitted. Default to Always. More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-policy
    #[serde(default, rename = "restartPolicy", skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>,
    /// RuntimeClassName refers to a RuntimeClass object in the node.k8s.io group, which should be used to run this pod.  If no RuntimeClass resource matches the named class, the pod will not be run. If unset or empty, the "legacy" RuntimeClass will be used, which is an implicit class with an empty definition that uses the default runtime handler. More info: https://git.k8s.io/enhancements/keps/sig-node/585-runtime-class
    #[serde(default, rename = "runtimeClassName", skip_serializing_if = "Option::is_none")]
    pub runtime_class_name: Option<String>,
    /// If specified, the pod will be dispatched by specified scheduler. If not specified, the pod will be dispatched by default scheduler.
    #[serde(default, rename = "schedulerName", skip_serializing_if = "Option::is_none")]
    pub scheduler_name: Option<String>,
    /// SchedulingGates is an opaque list of values that if specified will block scheduling the pod. If schedulingGates is not empty, the pod will stay in the SchedulingGated state and the scheduler will not attempt to schedule the pod.
    #[serde(default, rename = "schedulingGates", skip_serializing_if = "Vec::is_empty")]
    pub scheduling_gates: Vec<PodSchedulingGate>,
    /// SecurityContext holds pod-level security attributes and common container settings. Optional: Defaults to empty.  See type description for default values of each field.
    #[serde(default, rename = "securityContext", skip_serializing_if = "Option::is_none")]
    pub security_context: Option<PodSecurityContext>,
    /// DeprecatedServiceAccount is a deprecated alias for ServiceAccountName. Deprecated: Use serviceAccountName instead.
    #[serde(default, rename = "serviceAccount", skip_serializing_if = "Option::is_none")]
    pub service_account: Option<String>,
    /// ServiceAccountName is the name of the ServiceAccount to use to run this pod. More info: https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/
    #[serde(default, rename = "serviceAccountName", skip_serializing_if = "Option::is_none")]
    pub service_account_name: Option<String>,
    /// If true the pod's hostname will be configured as the pod's FQDN, rather than the leaf name (the default). In Linux containers, this means setting the FQDN in the hostname field of the kernel (the nodename field of struct utsname). In Windows containers, this means setting the registry value of hostname for the registry key HKEY_LOCAL_MACHINE\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters to FQDN. If a pod does not have FQDN, this has no effect. Default to false.
    #[serde(default, rename = "setHostnameAsFQDN", skip_serializing_if = "Option::is_none")]
    pub set_hostname_as_fqdn: Option<bool>,
    /// Share a single process namespace between all of the containers in a pod. When this is set containers will be able to view and signal processes from other containers in the same pod, and the first process in each container will not be assigned PID 1. HostPID and ShareProcessNamespace cannot both be set. Optional: Default to false.
    #[serde(default, rename = "shareProcessNamespace", skip_serializing_if = "Option::is_none")]
    pub share_process_namespace: Option<bool>,
    /// If specified, the fully qualified Pod hostname will be "<hostname>.<subdomain>.<pod namespace>.svc.<cluster domain>". If not specified, the pod will not have a domainname at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subdomain: Option<String>,
    /// Optional duration in seconds the pod needs to terminate gracefully. May be decreased in delete request. Value must be non-negative integer. The value zero indicates stop immediately via the kill signal (no opportunity to shut down). If this value is nil, the default grace period will be used instead. The grace period is the duration in seconds after the processes running in the pod are sent a termination signal and the time when the processes are forcibly halted with a kill signal. Set this value longer than the expected cleanup time for your process. Defaults to 30 seconds.
    #[serde(default, rename = "terminationGracePeriodSeconds", skip_serializing_if = "Option::is_none")]
    pub termination_grace_period_seconds: Option<i64>,
    /// If specified, the pod's tolerations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<Toleration>,
    /// TopologySpreadConstraints describes how a group of pods ought to spread across topology domains. Scheduler will schedule pods in a way which abides by the constraints. All topologySpreadConstraints are ANDed.
    #[serde(default, rename = "topologySpreadConstraints", skip_serializing_if = "Vec::is_empty")]
    pub topology_spread_constraints: Vec<TopologySpreadConstraint>,
    /// List of volumes that can be mounted by containers belonging to the pod. More info: https://kubernetes.io/docs/concepts/storage/volumes
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<Volume>,
}

/// PodStatus represents information about the status of a pod. Status may trail the actual state of a system, especially if the node that hosts the pod cannot contact the control plane.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodStatus {
    /// Current service state of pod. More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-conditions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<PodCondition>,
    /// Statuses of containers in this pod. Each container in the pod should have at most one status in this list, and all statuses should be for containers in the pod. However this is not enforced. If a status for a non-existent container is present in the list, or the list has duplicate names, the behavior of various Kubernetes components is not defined and those statuses might be ignored. More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-and-container-status
    #[serde(default, rename = "containerStatuses", skip_serializing_if = "Vec::is_empty")]
    pub container_statuses: Vec<ContainerStatus>,
    /// Statuses for any ephemeral containers that have run in this pod. Each ephemeral container in the pod should have at most one status in this list, and all statuses should be for containers in the pod. However this is not enforced. If a status for a non-existent container is present in the list, or the list has duplicate names, the behavior of various Kubernetes components is not defined and those statuses might be ignored. More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-and-container-status
    #[serde(default, rename = "ephemeralContainerStatuses", skip_serializing_if = "Vec::is_empty")]
    pub ephemeral_container_statuses: Vec<ContainerStatus>,
    /// Status of extended resource claim backed by DRA.
    #[serde(default, rename = "extendedResourceClaimStatus", skip_serializing_if = "Option::is_none")]
    pub extended_resource_claim_status: Option<PodExtendedResourceClaimStatus>,
    /// hostIP holds the IP address of the host to which the pod is assigned. Empty if the pod has not started yet. A pod can be assigned to a node that has a problem in kubelet which in turns mean that HostIP will not be updated even if there is a node is assigned to pod
    #[serde(default, rename = "hostIP", skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,
    /// hostIPs holds the IP addresses allocated to the host. If this field is specified, the first entry must match the hostIP field. This list is empty if the pod has not started yet. A pod can be assigned to a node that has a problem in kubelet which in turns means that HostIPs will not be updated even if there is a node is assigned to this pod.
    #[serde(default, rename = "hostIPs", skip_serializing_if = "Vec::is_empty")]
    pub host_i_ps: Vec<HostIP>,
    /// Statuses of init containers in this pod. The most recent successful non-restartable init container will have ready = true, the most recently started container will have startTime set. Each init container in the pod should have at most one status in this list, and all statuses should be for containers in the pod. However this is not enforced. If a status for a non-existent container is present in the list, or the list has duplicate names, the behavior of various Kubernetes components is not defined and those statuses might be ignored. More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-and-container-status
    #[serde(default, rename = "initContainerStatuses", skip_serializing_if = "Vec::is_empty")]
    pub init_container_statuses: Vec<ContainerStatus>,
    /// A human readable message indicating details about why the pod is in this condition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// nominatedNodeName is set only when this pod preempts other pods on the node, but it cannot be scheduled right away as preemption victims receive their graceful termination periods. This field does not guarantee that the pod will be scheduled on this node. Scheduler may decide to place the pod elsewhere if other nodes become available sooner. Scheduler may also decide to give the resources on this node to a higher priority pod that is created after preemption. As a result, this field may be different than PodSpec.nodeName when the pod is scheduled.
    #[serde(default, rename = "nominatedNodeName", skip_serializing_if = "Option::is_none")]
    pub nominated_node_name: Option<String>,
    /// If set, this represents the .metadata.generation that the pod status was set based upon. This is an alpha field. Enable PodObservedGenerationTracking to be able to use this field.
    #[serde(default, rename = "observedGeneration", skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The phase of a Pod is a simple, high-level summary of where the Pod is in its lifecycle. The conditions array, the reason and message fields, and the individual container status arrays contain more detail about the pod's status. There are five possible phase values:
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<crate::curated_enums::PodPhase>,
    /// podIP address allocated to the pod. Routable at least within the cluster. Empty if not yet allocated.
    #[serde(default, rename = "podIP", skip_serializing_if = "Option::is_none")]
    pub pod_ip: Option<String>,
    /// podIPs holds the IP addresses allocated to the pod. If this field is specified, the 0th entry must match the podIP field. Pods may be allocated at most 1 value for each of IPv4 and IPv6. This list is empty if no IPs have been allocated yet.
    #[serde(default, rename = "podIPs", skip_serializing_if = "Vec::is_empty")]
    pub pod_i_ps: Vec<PodIP>,
    /// The Quality of Service (QOS) classification assigned to the pod based on resource requirements See PodQOSClass type for available QOS classes More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/#quality-of-service-classes
    #[serde(default, rename = "qosClass", skip_serializing_if = "Option::is_none")]
    pub qos_class: Option<String>,
    /// A brief CamelCase message indicating details about why the pod is in this state. e.g. 'Evicted'
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Status of resources resize desired for pod's containers. It is empty if no resources resize is pending. Any changes to container resources will automatically set this to "Proposed" Deprecated: Resize status is moved to two pod conditions PodResizePending and PodResizeInProgress. PodResizePending will track states where the spec has been resized, but the Kubelet has not yet allocated the resources. PodResizeInProgress will track in-progress resizes, and should be present whenever allocated resources != acknowledged resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resize: Option<String>,
    /// Status of resource claims.
    #[serde(default, rename = "resourceClaimStatuses", skip_serializing_if = "Vec::is_empty")]
    pub resource_claim_statuses: Vec<PodResourceClaimStatus>,
    /// RFC 3339 date and time at which the object was acknowledged by the Kubelet. This is before the Kubelet pulled the container image(s) for the pod.
    #[serde(default, rename = "startTime", skip_serializing_if = "Option::is_none")]
    pub start_time: Option<Time>,
}

/// PodTemplateSpec describes the data a pod should have when created from a template
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PodTemplateSpec {
    /// Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
    #[serde(default, skip_serializing_if = "is_empty_meta")]
    pub metadata: crate::meta::ObjectMeta,
    /// Specification of the desired behavior of the pod. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<PodSpec>,
}

/// PolicyRule holds information that describes a policy rule, but does not contain information about who the rule applies to or which namespace the rule applies to.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PolicyRule {
    /// APIGroups is the name of the APIGroup that contains the resources.  If multiple API groups are specified, any action requested against one of the enumerated resources in any API group will be allowed. "" represents the core API group and "*" represents all API groups.
    #[serde(default, rename = "apiGroups", skip_serializing_if = "Vec::is_empty")]
    pub api_groups: Vec<String>,
    /// NonResourceURLs is a set of partial urls that a user should have access to.  *s are allowed, but only as the full, final step in the path Since non-resource URLs are not namespaced, this field is only applicable for ClusterRoles referenced from a ClusterRoleBinding. Rules can either apply to API resources (such as "pods" or "secrets") or non-resource URL paths (such as "/api"),  but not both.
    #[serde(default, rename = "nonResourceURLs", skip_serializing_if = "Vec::is_empty")]
    pub non_resource_ur_ls: Vec<String>,
    /// ResourceNames is an optional white list of names that the rule applies to.  An empty set means that everything is allowed.
    #[serde(default, rename = "resourceNames", skip_serializing_if = "Vec::is_empty")]
    pub resource_names: Vec<String>,
    /// Resources is a list of resources this rule applies to. '*' represents all resources.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<String>,
    /// Verbs is a list of Verbs that apply to ALL the ResourceKinds contained in this rule. '*' represents all verbs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verbs: Vec<String>,
}

/// PortStatus represents the error condition of a service port
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PortStatus {
    /// Error is to record the problem with the service port The format of the error shall comply with the following rules: - built-in error values shall be specified in this file and those shall use
    /// CamelCase names
    /// - cloud provider specific error values must have names that comply with the
    /// format foo.example.com/CamelCase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Port is the port number of the service port of which status is recorded here
    #[serde(default)]
    pub port: i32,
    /// Protocol is the protocol of the service port of which status is recorded here The supported values are: "TCP", "UDP", "SCTP"
    #[serde(default)]
    pub protocol: String,
}

/// PortworxVolumeSource represents a Portworx volume resource.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PortworxVolumeSource {
    /// fSType represents the filesystem type to mount Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs". Implicitly inferred to be "ext4" if unspecified.
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// readOnly defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// volumeID uniquely identifies a Portworx volume
    #[serde(default, rename = "volumeID")]
    pub volume_id: String,
}

/// An empty preferred scheduling term matches all objects with implicit weight 0 (i.e. it's a no-op). A null preferred scheduling term matches no objects (i.e. is also a no-op).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PreferredSchedulingTerm {
    /// A node selector term, associated with the corresponding weight.
    #[serde(default)]
    pub preference: NodeSelectorTerm,
    /// Weight associated with matching the corresponding nodeSelectorTerm, in the range 1-100.
    #[serde(default)]
    pub weight: i32,
}

/// Probe describes a health check to be performed against a container to determine whether it is alive or ready to receive traffic.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Probe {
    /// Exec specifies a command to execute in the container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec: Option<ExecAction>,
    /// Minimum consecutive failures for the probe to be considered failed after having succeeded. Defaults to 3. Minimum value is 1.
    #[serde(default, rename = "failureThreshold", skip_serializing_if = "Option::is_none")]
    pub failure_threshold: Option<i32>,
    /// GRPC specifies a GRPC HealthCheckRequest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc: Option<GRPCAction>,
    /// HTTPGet specifies an HTTP GET request to perform.
    #[serde(default, rename = "httpGet", skip_serializing_if = "Option::is_none")]
    pub http_get: Option<HTTPGetAction>,
    /// Number of seconds after the container has started before liveness probes are initiated. More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
    #[serde(default, rename = "initialDelaySeconds", skip_serializing_if = "Option::is_none")]
    pub initial_delay_seconds: Option<i32>,
    /// How often (in seconds) to perform the probe. Default to 10 seconds. Minimum value is 1.
    #[serde(default, rename = "periodSeconds", skip_serializing_if = "Option::is_none")]
    pub period_seconds: Option<i32>,
    /// Minimum consecutive successes for the probe to be considered successful after having failed. Defaults to 1. Must be 1 for liveness and startup. Minimum value is 1.
    #[serde(default, rename = "successThreshold", skip_serializing_if = "Option::is_none")]
    pub success_threshold: Option<i32>,
    /// TCPSocket specifies a connection to a TCP port.
    #[serde(default, rename = "tcpSocket", skip_serializing_if = "Option::is_none")]
    pub tcp_socket: Option<TCPSocketAction>,
    /// Optional duration in seconds the pod needs to terminate gracefully upon probe failure. The grace period is the duration in seconds after the processes running in the pod are sent a termination signal and the time when the processes are forcibly halted with a kill signal. Set this value longer than the expected cleanup time for your process. If this value is nil, the pod's terminationGracePeriodSeconds will be used. Otherwise, this value overrides the value provided by the pod spec. Value must be non-negative integer. The value zero indicates stop immediately via the kill signal (no opportunity to shut down). This is a beta field and requires enabling ProbeTerminationGracePeriod feature gate. Minimum value is 1. spec.terminationGracePeriodSeconds is used if unset.
    #[serde(default, rename = "terminationGracePeriodSeconds", skip_serializing_if = "Option::is_none")]
    pub termination_grace_period_seconds: Option<i64>,
    /// Number of seconds after which the probe times out. Defaults to 1 second. Minimum value is 1. More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
    #[serde(default, rename = "timeoutSeconds", skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,
}

/// Represents a projected volume source
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectedVolumeSource {
    /// defaultMode are the mode bits used to set permissions on created files by default. Must be an octal value between 0000 and 0777 or a decimal value between 0 and 511. YAML accepts both octal and decimal values, JSON requires decimal values for mode bits. Directories within the path are not affected by this setting. This might be in conflict with other options that affect the file mode, like fsGroup, and the result can be other mode bits set.
    #[serde(default, rename = "defaultMode", skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<i32>,
    /// sources is the list of volume projections. Each entry in this list handles one source.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<VolumeProjection>,
}

/// Quantity is a fixed-point representation of a number. It provides convenient marshaling/unmarshaling in JSON and YAML, in addition to String() and AsInt64() accessors.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Quantity {
}

/// Represents a Quobyte mount that lasts the lifetime of a pod. Quobyte volumes do not support ownership management or SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct QuobyteVolumeSource {
    /// group to map volume access to Default is no group
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// readOnly here will force the Quobyte volume to be mounted with read-only permissions. Defaults to false.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// registry represents a single or multiple Quobyte Registry services specified as a string as host:port pair (multiple entries are separated with commas) which acts as the central registry for volumes
    #[serde(default)]
    pub registry: String,
    /// tenant owning the given Quobyte volume in the Backend Used with dynamically provisioned Quobyte volumes, value is set by the plugin
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// user to map volume access to Defaults to serivceaccount user
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// volume is a string that references an already created Quobyte volume by name.
    #[serde(default)]
    pub volume: String,
}

/// Represents a Rados Block Device mount that lasts the lifetime of a pod. RBD volumes support ownership management and SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RBDPersistentVolumeSource {
    /// fsType is the filesystem type of the volume that you want to mount. Tip: Ensure that the filesystem type is supported by the host operating system. Examples: "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified. More info: https://kubernetes.io/docs/concepts/storage/volumes#rbd
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// image is the rados image name. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default)]
    pub image: String,
    /// keyring is the path to key ring for RBDUser. Default is /etc/ceph/keyring. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keyring: Option<String>,
    /// monitors is a collection of Ceph monitors. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub monitors: Vec<String>,
    /// pool is the rados pool name. Default is rbd. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// readOnly here will force the ReadOnly setting in VolumeMounts. Defaults to false. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretRef is name of the authentication secret for RBDUser. If provided overrides keyring. Default is nil. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretReference>,
    /// user is the rados user name. Default is admin. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// Represents a Rados Block Device mount that lasts the lifetime of a pod. RBD volumes support ownership management and SELinux relabeling.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RBDVolumeSource {
    /// fsType is the filesystem type of the volume that you want to mount. Tip: Ensure that the filesystem type is supported by the host operating system. Examples: "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified. More info: https://kubernetes.io/docs/concepts/storage/volumes#rbd
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// image is the rados image name. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default)]
    pub image: String,
    /// keyring is the path to key ring for RBDUser. Default is /etc/ceph/keyring. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keyring: Option<String>,
    /// monitors is a collection of Ceph monitors. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub monitors: Vec<String>,
    /// pool is the rados pool name. Default is rbd. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// readOnly here will force the ReadOnly setting in VolumeMounts. Defaults to false. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretRef is name of the authentication secret for RBDUser. If provided overrides keyring. Default is nil. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<LocalObjectReference>,
    /// user is the rados user name. Default is admin. More info: https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// ReplicaSetCondition describes the state of a replica set at a certain point.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaSetCondition {
    /// The last time the condition transitioned from one status to another.
    #[serde(default, rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<Time>,
    /// A human readable message indicating details about the transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The reason for the condition's last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Status of the condition, one of True, False, Unknown.
    #[serde(default)]
    pub status: String,
    /// Type of replica set condition.
    #[serde(default, rename = "type")]
    pub r#type: String,
}

/// ReplicaSetSpec is the specification of a ReplicaSet.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaSetSpec {
    /// Minimum number of seconds for which a newly created pod should be ready without any of its container crashing, for it to be considered available. Defaults to 0 (pod will be considered available as soon as it is ready)
    #[serde(default, rename = "minReadySeconds", skip_serializing_if = "Option::is_none")]
    pub min_ready_seconds: Option<i32>,
    /// Replicas is the number of desired pods. This is a pointer to distinguish between explicit zero and unspecified. Defaults to 1. More info: https://kubernetes.io/docs/concepts/workloads/controllers/replicaset
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// Selector is a label query over pods that should match the replica count. Label keys and values that must match in order to be controlled by this replica set. It must match the pod template's labels. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
    #[serde(default)]
    pub selector: LabelSelector,
    /// Template is the object that describes the pod that will be created if insufficient replicas are detected. More info: https://kubernetes.io/docs/concepts/workloads/controllers/replicaset/#pod-template
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<PodTemplateSpec>,
}

/// ReplicaSetStatus represents the current status of a ReplicaSet.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaSetStatus {
    /// The number of available non-terminating pods (ready for at least minReadySeconds) for this replica set.
    #[serde(default, rename = "availableReplicas", skip_serializing_if = "Option::is_none")]
    pub available_replicas: Option<i32>,
    /// Represents the latest available observations of a replica set's current state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ReplicaSetCondition>,
    /// The number of non-terminating pods that have labels matching the labels of the pod template of the replicaset.
    #[serde(default, rename = "fullyLabeledReplicas", skip_serializing_if = "Option::is_none")]
    pub fully_labeled_replicas: Option<i32>,
    /// ObservedGeneration reflects the generation of the most recently observed ReplicaSet.
    #[serde(default, rename = "observedGeneration", skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The number of non-terminating pods targeted by this ReplicaSet with a Ready Condition.
    #[serde(default, rename = "readyReplicas", skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    /// Replicas is the most recently observed number of non-terminating pods. More info: https://kubernetes.io/docs/concepts/workloads/controllers/replicaset
    #[serde(default)]
    pub replicas: i32,
    /// The number of terminating pods for this replica set. Terminating pods have a non-null .metadata.deletionTimestamp and have not yet reached the Failed or Succeeded .status.phase.
    #[serde(default, rename = "terminatingReplicas", skip_serializing_if = "Option::is_none")]
    pub terminating_replicas: Option<i32>,
}

/// ResourceClaim references one entry in PodSpec.ResourceClaims.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceClaim {
    /// Name must match the name of one entry in pod.spec.resourceClaims of the Pod where this field is used. It makes that resource available inside a container.
    #[serde(default)]
    pub name: String,
    /// Request is the name chosen for a request in the referenced claim. If empty, everything from the claim is made available, otherwise only the result of this request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<String>,
}

/// ResourceFieldSelector represents container resources (cpu, memory) and their output format
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceFieldSelector {
    /// Container name: required for volumes, optional for env vars
    #[serde(default, rename = "containerName", skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,
    /// Specifies the output format of the exposed resources, defaults to "1"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub divisor: Option<Quantity>,
    /// Required: resource to select
    #[serde(default)]
    pub resource: String,
}

/// ResourceHealth represents the health of a resource. It has the latest device health information. This is a part of KEP https://kep.k8s.io/4680.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceHealth {
    /// Health of the resource. can be one of:
    /// - Healthy: operates as normal
    /// - Unhealthy: reported unhealthy. We consider this a temporary health issue
    /// since we do not have a mechanism today to distinguish
    /// temporary and permanent issues.
    /// - Unknown: The status cannot be determined.
    /// For example, Device Plugin got unregistered and hasn't been re-registered since.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
    /// ResourceID is the unique identifier of the resource. See the ResourceID type for more information.
    #[serde(default, rename = "resourceID")]
    pub resource_id: String,
}

/// ResourceRequirements describes the compute resource requirements.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceRequirements {
    /// Claims lists the names of resources, defined in spec.resourceClaims, that are used by this container.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub claims: Vec<ResourceClaim>,
    /// Limits describes the maximum amount of compute resources allowed. More info: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub limits: std::collections::BTreeMap<String, Quantity>,
    /// Requests describes the minimum amount of compute resources required. If Requests is omitted for a container, it defaults to Limits if that is explicitly specified, otherwise to an implementation-defined value. Requests cannot exceed Limits. More info: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub requests: std::collections::BTreeMap<String, Quantity>,
}

/// ResourceStatus represents the status of a single resource allocated to a Pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceStatus {
    /// Name of the resource. Must be unique within the pod and in case of non-DRA resource, match one of the resources from the pod spec. For DRA resources, the value must be "claim:<claim_name>/<request>". When this status is reported about a container, the "claim_name" and "request" must match one of the claims of this container.
    #[serde(default)]
    pub name: String,
    /// List of unique resources health. Each element in the list contains an unique resource ID and its health. At a minimum, for the lifetime of a Pod, resource ID must uniquely identify the resource allocated to the Pod on the Node. If other Pod on the same Node reports the status with the same resource ID, it must be the same resource they share. See ResourceID type definition for a specific format it has in various use cases.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<ResourceHealth>,
}

/// RoleRef contains information that points to the role being used
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RoleRef {
    /// APIGroup is the group for the resource being referenced
    #[serde(default, rename = "apiGroup")]
    pub api_group: String,
    /// Name is the name of resource being referenced
    #[serde(default)]
    pub name: String,
}

/// Spec to control the desired behavior of daemon set rolling update.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RollingUpdateDaemonSet {
    /// The maximum number of nodes with an existing available DaemonSet pod that can have an updated DaemonSet pod during during an update. Value can be an absolute number (ex: 5) or a percentage of desired pods (ex: 10%). This can not be 0 if MaxUnavailable is 0. Absolute number is calculated from percentage by rounding up to a minimum of 1. Default value is 0. Example: when this is set to 30%, at most 30% of the total number of nodes that should be running the daemon pod (i.e. status.desiredNumberScheduled) can have their a new pod created before the old pod is marked as deleted. The update starts by launching new pods on 30% of nodes. Once an updated pod is available (Ready for at least minReadySeconds) the old DaemonSet pod on that node is marked deleted. If the old pod becomes unavailable for any reason (Ready transitions to false, is evicted, or is drained) an updated pod is immediately created on that node without considering surge limits. Allowing surge implies the possibility that the resources consumed by the daemonset on any given node can double if the readiness check fails, and so resource intensive daemonsets should take into account that they may cause evictions during disruption.
    #[serde(default, rename = "maxSurge", skip_serializing_if = "Option::is_none")]
    pub max_surge: Option<IntOrString>,
    /// The maximum number of DaemonSet pods that can be unavailable during the update. Value can be an absolute number (ex: 5) or a percentage of total number of DaemonSet pods at the start of the update (ex: 10%). Absolute number is calculated from percentage by rounding up. This cannot be 0 if MaxSurge is 0 Default value is 1. Example: when this is set to 30%, at most 30% of the total number of nodes that should be running the daemon pod (i.e. status.desiredNumberScheduled) can have their pods stopped for an update at any given time. The update starts by stopping at most 30% of those DaemonSet pods and then brings up new DaemonSet pods in their place. Once the new pods are available, it then proceeds onto other DaemonSet pods, thus ensuring that at least 70% of original number of DaemonSet pods are available at all times during the update.
    #[serde(default, rename = "maxUnavailable", skip_serializing_if = "Option::is_none")]
    pub max_unavailable: Option<IntOrString>,
}

/// Spec to control the desired behavior of rolling update.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RollingUpdateDeployment {
    /// The maximum number of pods that can be scheduled above the desired number of pods. Value can be an absolute number (ex: 5) or a percentage of desired pods (ex: 10%). This can not be 0 if MaxUnavailable is 0. Absolute number is calculated from percentage by rounding up. Defaults to 25%. Example: when this is set to 30%, the new ReplicaSet can be scaled up immediately when the rolling update starts, such that the total number of old and new pods do not exceed 130% of desired pods. Once old pods have been killed, new ReplicaSet can be scaled up further, ensuring that total number of pods running at any time during the update is at most 130% of desired pods.
    #[serde(default, rename = "maxSurge", skip_serializing_if = "Option::is_none")]
    pub max_surge: Option<IntOrString>,
    /// The maximum number of pods that can be unavailable during the update. Value can be an absolute number (ex: 5) or a percentage of desired pods (ex: 10%). Absolute number is calculated from percentage by rounding down. This can not be 0 if MaxSurge is 0. Defaults to 25%. Example: when this is set to 30%, the old ReplicaSet can be scaled down to 70% of desired pods immediately when the rolling update starts. Once new pods are ready, old ReplicaSet can be scaled down further, followed by scaling up the new ReplicaSet, ensuring that the total number of pods available at all times during the update is at least 70% of desired pods.
    #[serde(default, rename = "maxUnavailable", skip_serializing_if = "Option::is_none")]
    pub max_unavailable: Option<IntOrString>,
}

/// RollingUpdateStatefulSetStrategy is used to communicate parameter for RollingUpdateStatefulSetStrategyType.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RollingUpdateStatefulSetStrategy {
    /// The maximum number of pods that can be unavailable during the update. Value can be an absolute number (ex: 5) or a percentage of desired pods (ex: 10%). Absolute number is calculated from percentage by rounding up. This can not be 0. Defaults to 1. This field is alpha-level and is only honored by servers that enable the MaxUnavailableStatefulSet feature. The field applies to all pods in the range 0 to Replicas-1. That means if there is any unavailable pod in the range 0 to Replicas-1, it will be counted towards MaxUnavailable.
    #[serde(default, rename = "maxUnavailable", skip_serializing_if = "Option::is_none")]
    pub max_unavailable: Option<IntOrString>,
    /// Partition indicates the ordinal at which the StatefulSet should be partitioned for updates. During a rolling update, all pods from ordinal Replicas-1 to Partition are updated. All pods from ordinal Partition-1 to 0 remain untouched. This is helpful in being able to do a canary based deployment. The default value is 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<i32>,
}

/// SELinuxOptions are the labels to be applied to the container
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SELinuxOptions {
    /// Level is SELinux level label that applies to the container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
    /// Role is a SELinux role label that applies to the container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Type is a SELinux type label that applies to the container.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// User is a SELinux user label that applies to the container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// ScaleIOPersistentVolumeSource represents a persistent ScaleIO volume
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ScaleIOPersistentVolumeSource {
    /// fsType is the filesystem type to mount. Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs", "ntfs". Default is "xfs"
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// gateway is the host address of the ScaleIO API Gateway.
    #[serde(default)]
    pub gateway: String,
    /// protectionDomain is the name of the ScaleIO Protection Domain for the configured storage.
    #[serde(default, rename = "protectionDomain", skip_serializing_if = "Option::is_none")]
    pub protection_domain: Option<String>,
    /// readOnly defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretRef references to the secret for ScaleIO user and other sensitive information. If this is not provided, Login operation will fail.
    #[serde(default, rename = "secretRef")]
    pub secret_ref: SecretReference,
    /// sslEnabled is the flag to enable/disable SSL communication with Gateway, default false
    #[serde(default, rename = "sslEnabled", skip_serializing_if = "Option::is_none")]
    pub ssl_enabled: Option<bool>,
    /// storageMode indicates whether the storage for a volume should be ThickProvisioned or ThinProvisioned. Default is ThinProvisioned.
    #[serde(default, rename = "storageMode", skip_serializing_if = "Option::is_none")]
    pub storage_mode: Option<String>,
    /// storagePool is the ScaleIO Storage Pool associated with the protection domain.
    #[serde(default, rename = "storagePool", skip_serializing_if = "Option::is_none")]
    pub storage_pool: Option<String>,
    /// system is the name of the storage system as configured in ScaleIO.
    #[serde(default)]
    pub system: String,
    /// volumeName is the name of a volume already created in the ScaleIO system that is associated with this volume source.
    #[serde(default, rename = "volumeName", skip_serializing_if = "Option::is_none")]
    pub volume_name: Option<String>,
}

/// ScaleIOVolumeSource represents a persistent ScaleIO volume
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ScaleIOVolumeSource {
    /// fsType is the filesystem type to mount. Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs", "ntfs". Default is "xfs".
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// gateway is the host address of the ScaleIO API Gateway.
    #[serde(default)]
    pub gateway: String,
    /// protectionDomain is the name of the ScaleIO Protection Domain for the configured storage.
    #[serde(default, rename = "protectionDomain", skip_serializing_if = "Option::is_none")]
    pub protection_domain: Option<String>,
    /// readOnly Defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretRef references to the secret for ScaleIO user and other sensitive information. If this is not provided, Login operation will fail.
    #[serde(default, rename = "secretRef")]
    pub secret_ref: LocalObjectReference,
    /// sslEnabled Flag enable/disable SSL communication with Gateway, default false
    #[serde(default, rename = "sslEnabled", skip_serializing_if = "Option::is_none")]
    pub ssl_enabled: Option<bool>,
    /// storageMode indicates whether the storage for a volume should be ThickProvisioned or ThinProvisioned. Default is ThinProvisioned.
    #[serde(default, rename = "storageMode", skip_serializing_if = "Option::is_none")]
    pub storage_mode: Option<String>,
    /// storagePool is the ScaleIO Storage Pool associated with the protection domain.
    #[serde(default, rename = "storagePool", skip_serializing_if = "Option::is_none")]
    pub storage_pool: Option<String>,
    /// system is the name of the storage system as configured in ScaleIO.
    #[serde(default)]
    pub system: String,
    /// volumeName is the name of a volume already created in the ScaleIO system that is associated with this volume source.
    #[serde(default, rename = "volumeName", skip_serializing_if = "Option::is_none")]
    pub volume_name: Option<String>,
}

/// SeccompProfile defines a pod/container's seccomp profile settings. Only one profile source may be set.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SeccompProfile {
    /// localhostProfile indicates a profile defined in a file on the node should be used. The profile must be preconfigured on the node to work. Must be a descending path, relative to the kubelet's configured seccomp profile location. Must be set if type is "Localhost". Must NOT be set for any other type.
    #[serde(default, rename = "localhostProfile", skip_serializing_if = "Option::is_none")]
    pub localhost_profile: Option<String>,
    /// type indicates which kind of seccomp profile will be applied. Valid options are:
    #[serde(default, rename = "type")]
    pub r#type: String,
}

/// SecretEnvSource selects a Secret to populate the environment variables with.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SecretEnvSource {
    /// Name of the referent. This field is effectively required, but due to backwards compatibility is allowed to be empty. Instances of this type with an empty value here are almost certainly wrong. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Specify whether the Secret must be defined
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// SecretKeySelector selects a key of a Secret.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SecretKeySelector {
    /// The key of the secret to select from.  Must be a valid secret key.
    #[serde(default)]
    pub key: String,
    /// Name of the referent. This field is effectively required, but due to backwards compatibility is allowed to be empty. Instances of this type with an empty value here are almost certainly wrong. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Specify whether the Secret or its key must be defined
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// Adapts a secret into a projected volume.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SecretProjection {
    /// items if unspecified, each key-value pair in the Data field of the referenced Secret will be projected into the volume as a file whose name is the key and content is the value. If specified, the listed keys will be projected into the specified paths, and unlisted keys will not be present. If a key is specified which is not present in the Secret, the volume setup will error unless it is marked optional. Paths must be relative and may not contain the '..' path or start with '..'.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<KeyToPath>,
    /// Name of the referent. This field is effectively required, but due to backwards compatibility is allowed to be empty. Instances of this type with an empty value here are almost certainly wrong. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// optional field specify whether the Secret or its key must be defined
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// SecretReference represents a Secret Reference. It has enough information to retrieve secret in any namespace
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SecretReference {
    /// name is unique within a namespace to reference a secret resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// namespace defines the space within which the secret name must be unique.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Adapts a Secret into a volume.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SecretVolumeSource {
    /// defaultMode is Optional: mode bits used to set permissions on created files by default. Must be an octal value between 0000 and 0777 or a decimal value between 0 and 511. YAML accepts both octal and decimal values, JSON requires decimal values for mode bits. Defaults to 0644. Directories within the path are not affected by this setting. This might be in conflict with other options that affect the file mode, like fsGroup, and the result can be other mode bits set.
    #[serde(default, rename = "defaultMode", skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<i32>,
    /// items If unspecified, each key-value pair in the Data field of the referenced Secret will be projected into the volume as a file whose name is the key and content is the value. If specified, the listed keys will be projected into the specified paths, and unlisted keys will not be present. If a key is specified which is not present in the Secret, the volume setup will error unless it is marked optional. Paths must be relative and may not contain the '..' path or start with '..'.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<KeyToPath>,
    /// optional field specify whether the Secret or its keys must be defined
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
    /// secretName is the name of the secret in the pod's namespace to use. More info: https://kubernetes.io/docs/concepts/storage/volumes#secret
    #[serde(default, rename = "secretName", skip_serializing_if = "Option::is_none")]
    pub secret_name: Option<String>,
}

/// SecurityContext holds security configuration that will be applied to a container. Some fields are present in both SecurityContext and PodSecurityContext.  When both are set, the values in SecurityContext take precedence.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SecurityContext {
    /// AllowPrivilegeEscalation controls whether a process can gain more privileges than its parent process. This bool directly controls if the no_new_privs flag will be set on the container process. AllowPrivilegeEscalation is true always when the container is: 1) run as Privileged 2) has CAP_SYS_ADMIN Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "allowPrivilegeEscalation", skip_serializing_if = "Option::is_none")]
    pub allow_privilege_escalation: Option<bool>,
    /// appArmorProfile is the AppArmor options to use by this container. If set, this profile overrides the pod's appArmorProfile. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "appArmorProfile", skip_serializing_if = "Option::is_none")]
    pub app_armor_profile: Option<AppArmorProfile>,
    /// The capabilities to add/drop when running containers. Defaults to the default set of capabilities granted by the container runtime. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Capabilities>,
    /// Run container in privileged mode. Processes in privileged containers are essentially equivalent to root on the host. Defaults to false. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privileged: Option<bool>,
    /// procMount denotes the type of proc mount to use for the containers. The default value is Default which uses the container runtime defaults for readonly paths and masked paths. This requires the ProcMountType feature flag to be enabled. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "procMount", skip_serializing_if = "Option::is_none")]
    pub proc_mount: Option<String>,
    /// Whether this container has a read-only root filesystem. Default is false. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "readOnlyRootFilesystem", skip_serializing_if = "Option::is_none")]
    pub read_only_root_filesystem: Option<bool>,
    /// The GID to run the entrypoint of the container process. Uses runtime default if unset. May also be set in PodSecurityContext.  If set in both SecurityContext and PodSecurityContext, the value specified in SecurityContext takes precedence. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "runAsGroup", skip_serializing_if = "Option::is_none")]
    pub run_as_group: Option<i64>,
    /// Indicates that the container must run as a non-root user. If true, the Kubelet will validate the image at runtime to ensure that it does not run as UID 0 (root) and fail to start the container if it does. If unset or false, no such validation will be performed. May also be set in PodSecurityContext.  If set in both SecurityContext and PodSecurityContext, the value specified in SecurityContext takes precedence.
    #[serde(default, rename = "runAsNonRoot", skip_serializing_if = "Option::is_none")]
    pub run_as_non_root: Option<bool>,
    /// The UID to run the entrypoint of the container process. Defaults to user specified in image metadata if unspecified. May also be set in PodSecurityContext.  If set in both SecurityContext and PodSecurityContext, the value specified in SecurityContext takes precedence. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "runAsUser", skip_serializing_if = "Option::is_none")]
    pub run_as_user: Option<i64>,
    /// The SELinux context to be applied to the container. If unspecified, the container runtime will allocate a random SELinux context for each container.  May also be set in PodSecurityContext.  If set in both SecurityContext and PodSecurityContext, the value specified in SecurityContext takes precedence. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "seLinuxOptions", skip_serializing_if = "Option::is_none")]
    pub se_linux_options: Option<SELinuxOptions>,
    /// The seccomp options to use by this container. If seccomp options are provided at both the pod & container level, the container options override the pod options. Note that this field cannot be set when spec.os.name is windows.
    #[serde(default, rename = "seccompProfile", skip_serializing_if = "Option::is_none")]
    pub seccomp_profile: Option<SeccompProfile>,
    /// The Windows specific settings applied to all containers. If unspecified, the options from the PodSecurityContext will be used. If set in both SecurityContext and PodSecurityContext, the value specified in SecurityContext takes precedence. Note that this field cannot be set when spec.os.name is linux.
    #[serde(default, rename = "windowsOptions", skip_serializing_if = "Option::is_none")]
    pub windows_options: Option<WindowsSecurityContextOptions>,
}

/// ServiceAccountTokenProjection represents a projected service account token volume. This projection can be used to insert a service account token into the pods runtime filesystem for use against APIs (Kubernetes API Server or otherwise).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceAccountTokenProjection {
    /// audience is the intended audience of the token. A recipient of a token must identify itself with an identifier specified in the audience of the token, and otherwise should reject the token. The audience defaults to the identifier of the apiserver.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    /// expirationSeconds is the requested duration of validity of the service account token. As the token approaches expiration, the kubelet volume plugin will proactively rotate the service account token. The kubelet will start trying to rotate the token if the token is older than 80 percent of its time to live or if the token is older than 24 hours.Defaults to 1 hour and must be at least 10 minutes.
    #[serde(default, rename = "expirationSeconds", skip_serializing_if = "Option::is_none")]
    pub expiration_seconds: Option<i64>,
    /// path is the path relative to the mount point of the file to project the token into.
    #[serde(default)]
    pub path: String,
}

/// ServicePort contains information on service's port.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ServicePort {
    /// The application protocol for this port. This is used as a hint for implementations to offer richer behavior for protocols that they understand. This field follows standard Kubernetes label syntax. Valid values are either:
    #[serde(default, rename = "appProtocol", skip_serializing_if = "Option::is_none")]
    pub app_protocol: Option<String>,
    /// The name of this port within the service. This must be a DNS_LABEL. All ports within a ServiceSpec must have unique names. When considering the endpoints for a Service, this must match the 'name' field in the EndpointPort. Optional if only one ServicePort is defined on this service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The port on each node on which this service is exposed when type is NodePort or LoadBalancer.  Usually assigned by the system. If a value is specified, in-range, and not in use it will be used, otherwise the operation will fail.  If not specified, a port will be allocated if this Service requires one.  If this field is specified when creating a Service which does not need it, creation will fail. This field will be wiped when updating a Service to no longer need it (e.g. changing type from NodePort to ClusterIP). More info: https://kubernetes.io/docs/concepts/services-networking/service/#type-nodeport
    #[serde(default, rename = "nodePort", skip_serializing_if = "Option::is_none")]
    pub node_port: Option<i32>,
    /// The port that will be exposed by this service.
    #[serde(default)]
    pub port: i32,
    /// The IP protocol for this port. Supports "TCP", "UDP", and "SCTP". Default is TCP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    /// Number or name of the port to access on the pods targeted by the service. Number must be in the range 1 to 65535. Name must be an IANA_SVC_NAME. If this is a string, it will be looked up as a named port in the target Pod's container ports. If this is not specified, the value of the 'port' field is used (an identity map). This field is ignored for services with clusterIP=None, and should be omitted or set equal to the 'port' field. More info: https://kubernetes.io/docs/concepts/services-networking/service/#defining-a-service
    #[serde(default, rename = "targetPort", skip_serializing_if = "Option::is_none")]
    pub target_port: Option<IntOrString>,
}

/// ServiceSpec describes the attributes that a user creates on a service.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceSpec {
    /// allocateLoadBalancerNodePorts defines if NodePorts will be automatically allocated for services with type LoadBalancer.  Default is "true". It may be set to "false" if the cluster load-balancer does not rely on NodePorts.  If the caller requests specific NodePorts (by specifying a value), those requests will be respected, regardless of this field. This field may only be set for services with type LoadBalancer and will be cleared if the type is changed to any other type.
    #[serde(default, rename = "allocateLoadBalancerNodePorts", skip_serializing_if = "Option::is_none")]
    pub allocate_load_balancer_node_ports: Option<bool>,
    /// clusterIP is the IP address of the service and is usually assigned randomly. If an address is specified manually, is in-range (as per system configuration), and is not in use, it will be allocated to the service; otherwise creation of the service will fail. This field may not be changed through updates unless the type field is also being changed to ExternalName (which requires this field to be blank) or the type field is being changed from ExternalName (in which case this field may optionally be specified, as describe above).  Valid values are "None", empty string (""), or a valid IP address. Setting this to "None" makes a "headless service" (no virtual IP), which is useful when direct endpoint connections are preferred and proxying is not required.  Only applies to types ClusterIP, NodePort, and LoadBalancer. If this field is specified when creating a Service of type ExternalName, creation will fail. This field will be wiped when updating a Service to type ExternalName. More info: https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
    #[serde(default, rename = "clusterIP", skip_serializing_if = "Option::is_none")]
    pub cluster_ip: Option<String>,
    /// ClusterIPs is a list of IP addresses assigned to this service, and are usually assigned randomly.  If an address is specified manually, is in-range (as per system configuration), and is not in use, it will be allocated to the service; otherwise creation of the service will fail. This field may not be changed through updates unless the type field is also being changed to ExternalName (which requires this field to be empty) or the type field is being changed from ExternalName (in which case this field may optionally be specified, as describe above).  Valid values are "None", empty string (""), or a valid IP address.  Setting this to "None" makes a "headless service" (no virtual IP), which is useful when direct endpoint connections are preferred and proxying is not required.  Only applies to types ClusterIP, NodePort, and LoadBalancer. If this field is specified when creating a Service of type ExternalName, creation will fail. This field will be wiped when updating a Service to type ExternalName.  If this field is not specified, it will be initialized from the clusterIP field.  If this field is specified, clients must ensure that clusterIPs[0] and clusterIP have the same value.
    #[serde(default, rename = "clusterIPs", skip_serializing_if = "Vec::is_empty")]
    pub cluster_i_ps: Vec<String>,
    /// externalIPs is a list of IP addresses for which nodes in the cluster will also accept traffic for this service.  These IPs are not managed by Kubernetes.  The user is responsible for ensuring that traffic arrives at a node with this IP.  A common example is external load-balancers that are not part of the Kubernetes system.
    #[serde(default, rename = "externalIPs", skip_serializing_if = "Vec::is_empty")]
    pub external_i_ps: Vec<String>,
    /// externalName is the external reference that discovery mechanisms will return as an alias for this service (e.g. a DNS CNAME record). No proxying will be involved.  Must be a lowercase RFC-1123 hostname (https://tools.ietf.org/html/rfc1123) and requires `type` to be "ExternalName".
    #[serde(default, rename = "externalName", skip_serializing_if = "Option::is_none")]
    pub external_name: Option<String>,
    /// externalTrafficPolicy describes how nodes distribute service traffic they receive on one of the Service's "externally-facing" addresses (NodePorts, ExternalIPs, and LoadBalancer IPs). If set to "Local", the proxy will configure the service in a way that assumes that external load balancers will take care of balancing the service traffic between nodes, and so each node will deliver traffic only to the node-local endpoints of the service, without masquerading the client source IP. (Traffic mistakenly sent to a node with no endpoints will be dropped.) The default value, "Cluster", uses the standard behavior of routing to all endpoints evenly (possibly modified by topology and other features). Note that traffic sent to an External IP or LoadBalancer IP from within the cluster will always get "Cluster" semantics, but clients sending to a NodePort from within the cluster may need to take traffic policy into account when picking a node.
    #[serde(default, rename = "externalTrafficPolicy", skip_serializing_if = "Option::is_none")]
    pub external_traffic_policy: Option<String>,
    /// healthCheckNodePort specifies the healthcheck nodePort for the service. This only applies when type is set to LoadBalancer and externalTrafficPolicy is set to Local. If a value is specified, is in-range, and is not in use, it will be used.  If not specified, a value will be automatically allocated.  External systems (e.g. load-balancers) can use this port to determine if a given node holds endpoints for this service or not.  If this field is specified when creating a Service which does not need it, creation will fail. This field will be wiped when updating a Service to no longer need it (e.g. changing type). This field cannot be updated once set.
    #[serde(default, rename = "healthCheckNodePort", skip_serializing_if = "Option::is_none")]
    pub health_check_node_port: Option<i32>,
    /// InternalTrafficPolicy describes how nodes distribute service traffic they receive on the ClusterIP. If set to "Local", the proxy will assume that pods only want to talk to endpoints of the service on the same node as the pod, dropping the traffic if there are no local endpoints. The default value, "Cluster", uses the standard behavior of routing to all endpoints evenly (possibly modified by topology and other features).
    #[serde(default, rename = "internalTrafficPolicy", skip_serializing_if = "Option::is_none")]
    pub internal_traffic_policy: Option<String>,
    /// IPFamilies is a list of IP families (e.g. IPv4, IPv6) assigned to this service. This field is usually assigned automatically based on cluster configuration and the ipFamilyPolicy field. If this field is specified manually, the requested family is available in the cluster, and ipFamilyPolicy allows it, it will be used; otherwise creation of the service will fail. This field is conditionally mutable: it allows for adding or removing a secondary IP family, but it does not allow changing the primary IP family of the Service. Valid values are "IPv4" and "IPv6".  This field only applies to Services of types ClusterIP, NodePort, and LoadBalancer, and does apply to "headless" services. This field will be wiped when updating a Service to type ExternalName.
    #[serde(default, rename = "ipFamilies", skip_serializing_if = "Vec::is_empty")]
    pub ip_families: Vec<String>,
    /// IPFamilyPolicy represents the dual-stack-ness requested or required by this Service. If there is no value provided, then this field will be set to SingleStack. Services can be "SingleStack" (a single IP family), "PreferDualStack" (two IP families on dual-stack configured clusters or a single IP family on single-stack clusters), or "RequireDualStack" (two IP families on dual-stack configured clusters, otherwise fail). The ipFamilies and clusterIPs fields depend on the value of this field. This field will be wiped when updating a service to type ExternalName.
    #[serde(default, rename = "ipFamilyPolicy", skip_serializing_if = "Option::is_none")]
    pub ip_family_policy: Option<String>,
    /// loadBalancerClass is the class of the load balancer implementation this Service belongs to. If specified, the value of this field must be a label-style identifier, with an optional prefix, e.g. "internal-vip" or "example.com/internal-vip". Unprefixed names are reserved for end-users. This field can only be set when the Service type is 'LoadBalancer'. If not set, the default load balancer implementation is used, today this is typically done through the cloud provider integration, but should apply for any default implementation. If set, it is assumed that a load balancer implementation is watching for Services with a matching class. Any default load balancer implementation (e.g. cloud providers) should ignore Services that set this field. This field can only be set when creating or updating a Service to type 'LoadBalancer'. Once set, it can not be changed. This field will be wiped when a service is updated to a non 'LoadBalancer' type.
    #[serde(default, rename = "loadBalancerClass", skip_serializing_if = "Option::is_none")]
    pub load_balancer_class: Option<String>,
    /// Only applies to Service Type: LoadBalancer. This feature depends on whether the underlying cloud-provider supports specifying the loadBalancerIP when a load balancer is created. This field will be ignored if the cloud-provider does not support the feature. Deprecated: This field was under-specified and its meaning varies across implementations. Using it is non-portable and it may not support dual-stack. Users are encouraged to use implementation-specific annotations when available.
    #[serde(default, rename = "loadBalancerIP", skip_serializing_if = "Option::is_none")]
    pub load_balancer_ip: Option<String>,
    /// If specified and supported by the platform, this will restrict traffic through the cloud-provider load-balancer will be restricted to the specified client IPs. This field will be ignored if the cloud-provider does not support the feature." More info: https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/
    #[serde(default, rename = "loadBalancerSourceRanges", skip_serializing_if = "Vec::is_empty")]
    pub load_balancer_source_ranges: Vec<String>,
    /// The list of ports that are exposed by this service. More info: https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<ServicePort>,
    /// publishNotReadyAddresses indicates that any agent which deals with endpoints for this Service should disregard any indications of ready/not-ready. The primary use case for setting this field is for a StatefulSet's Headless Service to propagate SRV DNS records for its Pods for the purpose of peer discovery. The Kubernetes controllers that generate Endpoints and EndpointSlice resources for Services interpret this to mean that all endpoints are considered "ready" even if the Pods themselves are not. Agents which consume only Kubernetes generated endpoints through the Endpoints or EndpointSlice resources can safely assume this behavior.
    #[serde(default, rename = "publishNotReadyAddresses", skip_serializing_if = "Option::is_none")]
    pub publish_not_ready_addresses: Option<bool>,
    /// Route service traffic to pods with label keys and values matching this selector. If empty or not present, the service is assumed to have an external process managing its endpoints, which Kubernetes will not modify. Only applies to types ClusterIP, NodePort, and LoadBalancer. Ignored if type is ExternalName. More info: https://kubernetes.io/docs/concepts/services-networking/service/
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub selector: std::collections::BTreeMap<String, String>,
    /// Supports "ClientIP" and "None". Used to maintain session affinity. Enable client IP based session affinity. Must be ClientIP or None. Defaults to None. More info: https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
    #[serde(default, rename = "sessionAffinity", skip_serializing_if = "Option::is_none")]
    pub session_affinity: Option<String>,
    /// sessionAffinityConfig contains the configurations of session affinity.
    #[serde(default, rename = "sessionAffinityConfig", skip_serializing_if = "Option::is_none")]
    pub session_affinity_config: Option<SessionAffinityConfig>,
    /// TrafficDistribution offers a way to express preferences for how traffic is distributed to Service endpoints. Implementations can use this field as a hint, but are not required to guarantee strict adherence. If the field is not set, the implementation will apply its default routing strategy. If set to "PreferClose", implementations should prioritize endpoints that are in the same zone.
    #[serde(default, rename = "trafficDistribution", skip_serializing_if = "Option::is_none")]
    pub traffic_distribution: Option<String>,
    /// type determines how the Service is exposed. Defaults to ClusterIP. Valid options are ExternalName, ClusterIP, NodePort, and LoadBalancer. "ClusterIP" allocates a cluster-internal IP address for load-balancing to endpoints. Endpoints are determined by the selector or if that is not specified, by manual construction of an Endpoints object or EndpointSlice objects. If clusterIP is "None", no virtual IP is allocated and the endpoints are published as a set of endpoints rather than a virtual IP. "NodePort" builds on ClusterIP and allocates a port on every node which routes to the same endpoints as the clusterIP. "LoadBalancer" builds on NodePort and creates an external load-balancer (if supported in the current cloud) which routes to the same endpoints as the clusterIP. "ExternalName" aliases this service to the specified externalName. Several other fields do not apply to ExternalName services. More info: https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

/// ServiceStatus represents the current status of a service.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceStatus {
    /// Current service state
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    /// LoadBalancer contains the current status of the load-balancer, if one is present.
    #[serde(default, rename = "loadBalancer", skip_serializing_if = "Option::is_none")]
    pub load_balancer: Option<LoadBalancerStatus>,
}

/// SessionAffinityConfig represents the configurations of session affinity.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionAffinityConfig {
    /// clientIP contains the configurations of Client IP based session affinity.
    #[serde(default, rename = "clientIP", skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<ClientIPConfig>,
}

/// SleepAction describes a "sleep" action.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SleepAction {
    /// Seconds is the number of seconds to sleep.
    #[serde(default)]
    pub seconds: i64,
}

/// StatefulSetCondition describes the state of a statefulset at a certain point.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StatefulSetCondition {
    /// Last time the condition transitioned from one status to another.
    #[serde(default, rename = "lastTransitionTime", skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<Time>,
    /// A human readable message indicating details about the transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The reason for the condition's last transition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Status of the condition, one of True, False, Unknown.
    #[serde(default)]
    pub status: String,
    /// Type of statefulset condition.
    #[serde(default, rename = "type")]
    pub r#type: String,
}

/// StatefulSetOrdinals describes the policy used for replica ordinal assignment in this StatefulSet.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StatefulSetOrdinals {
    /// start is the number representing the first replica's index. It may be used to number replicas from an alternate index (eg: 1-indexed) over the default 0-indexed names, or to orchestrate progressive movement of replicas from one StatefulSet to another. If set, replica indices will be in the range:
    /// [.spec.ordinals.start, .spec.ordinals.start + .spec.replicas).
    /// If unset, defaults to 0. Replica indices will be in the range:
    /// [0, .spec.replicas).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<i32>,
}

/// StatefulSetPersistentVolumeClaimRetentionPolicy describes the policy used for PVCs created from the StatefulSet VolumeClaimTemplates.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StatefulSetPersistentVolumeClaimRetentionPolicy {
    /// WhenDeleted specifies what happens to PVCs created from StatefulSet VolumeClaimTemplates when the StatefulSet is deleted. The default policy of `Retain` causes PVCs to not be affected by StatefulSet deletion. The `Delete` policy causes those PVCs to be deleted.
    #[serde(default, rename = "whenDeleted", skip_serializing_if = "Option::is_none")]
    pub when_deleted: Option<String>,
    /// WhenScaled specifies what happens to PVCs created from StatefulSet VolumeClaimTemplates when the StatefulSet is scaled down. The default policy of `Retain` causes PVCs to not be affected by a scaledown. The `Delete` policy causes the associated PVCs for any excess pods above the replica count to be deleted.
    #[serde(default, rename = "whenScaled", skip_serializing_if = "Option::is_none")]
    pub when_scaled: Option<String>,
}

/// A StatefulSetSpec is the specification of a StatefulSet.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StatefulSetSpec {
    /// Minimum number of seconds for which a newly created pod should be ready without any of its container crashing for it to be considered available. Defaults to 0 (pod will be considered available as soon as it is ready)
    #[serde(default, rename = "minReadySeconds", skip_serializing_if = "Option::is_none")]
    pub min_ready_seconds: Option<i32>,
    /// ordinals controls the numbering of replica indices in a StatefulSet. The default ordinals behavior assigns a "0" index to the first replica and increments the index by one for each additional replica requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordinals: Option<StatefulSetOrdinals>,
    /// persistentVolumeClaimRetentionPolicy describes the lifecycle of persistent volume claims created from volumeClaimTemplates. By default, all persistent volume claims are created as needed and retained until manually deleted. This policy allows the lifecycle to be altered, for example by deleting persistent volume claims when their stateful set is deleted, or when their pod is scaled down.
    #[serde(default, rename = "persistentVolumeClaimRetentionPolicy", skip_serializing_if = "Option::is_none")]
    pub persistent_volume_claim_retention_policy: Option<StatefulSetPersistentVolumeClaimRetentionPolicy>,
    /// podManagementPolicy controls how pods are created during initial scale up, when replacing pods on nodes, or when scaling down. The default policy is `OrderedReady`, where pods are created in increasing order (pod-0, then pod-1, etc) and the controller will wait until each pod is ready before continuing. When scaling down, the pods are removed in the opposite order. The alternative policy is `Parallel` which will create pods in parallel to match the desired scale without waiting, and on scale down will delete all pods at once.
    #[serde(default, rename = "podManagementPolicy", skip_serializing_if = "Option::is_none")]
    pub pod_management_policy: Option<String>,
    /// replicas is the desired number of replicas of the given Template. These are replicas in the sense that they are instantiations of the same Template, but individual replicas also have a consistent identity. If unspecified, defaults to 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// revisionHistoryLimit is the maximum number of revisions that will be maintained in the StatefulSet's revision history. The revision history consists of all revisions not represented by a currently applied StatefulSetSpec version. The default value is 10.
    #[serde(default, rename = "revisionHistoryLimit", skip_serializing_if = "Option::is_none")]
    pub revision_history_limit: Option<i32>,
    /// selector is a label query over pods that should match the replica count. It must match the pod template's labels. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
    #[serde(default)]
    pub selector: LabelSelector,
    /// serviceName is the name of the service that governs this StatefulSet. This service must exist before the StatefulSet, and is responsible for the network identity of the set. Pods get DNS/hostnames that follow the pattern: pod-specific-string.serviceName.default.svc.cluster.local where "pod-specific-string" is managed by the StatefulSet controller.
    #[serde(default, rename = "serviceName", skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// template is the object that describes the pod that will be created if insufficient replicas are detected. Each pod stamped out by the StatefulSet will fulfill this Template, but have a unique identity from the rest of the StatefulSet. Each pod will be named with the format <statefulsetname>-<podindex>. For example, a pod in a StatefulSet named "web" with index number "3" would be named "web-3". The only allowed template.spec.restartPolicy value is "Always".
    #[serde(default)]
    pub template: PodTemplateSpec,
    /// updateStrategy indicates the StatefulSetUpdateStrategy that will be employed to update Pods in the StatefulSet when a revision is made to Template.
    #[serde(default, rename = "updateStrategy", skip_serializing_if = "Option::is_none")]
    pub update_strategy: Option<StatefulSetUpdateStrategy>,
    /// volumeClaimTemplates is a list of claims that pods are allowed to reference. The StatefulSet controller is responsible for mapping network identities to claims in a way that maintains the identity of a pod. Every claim in this list must have at least one matching (by name) volumeMount in one container in the template. A claim in this list takes precedence over any volumes in the template, with the same name.
    #[serde(default, rename = "volumeClaimTemplates", skip_serializing_if = "Vec::is_empty")]
    pub volume_claim_templates: Vec<PersistentVolumeClaim>,
}

/// StatefulSetStatus represents the current state of a StatefulSet.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StatefulSetStatus {
    /// Total number of available pods (ready for at least minReadySeconds) targeted by this statefulset.
    #[serde(default, rename = "availableReplicas", skip_serializing_if = "Option::is_none")]
    pub available_replicas: Option<i32>,
    /// collisionCount is the count of hash collisions for the StatefulSet. The StatefulSet controller uses this field as a collision avoidance mechanism when it needs to create the name for the newest ControllerRevision.
    #[serde(default, rename = "collisionCount", skip_serializing_if = "Option::is_none")]
    pub collision_count: Option<i32>,
    /// Represents the latest available observations of a statefulset's current state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<StatefulSetCondition>,
    /// currentReplicas is the number of Pods created by the StatefulSet controller from the StatefulSet version indicated by currentRevision.
    #[serde(default, rename = "currentReplicas", skip_serializing_if = "Option::is_none")]
    pub current_replicas: Option<i32>,
    /// currentRevision, if not empty, indicates the version of the StatefulSet used to generate Pods in the sequence [0,currentReplicas).
    #[serde(default, rename = "currentRevision", skip_serializing_if = "Option::is_none")]
    pub current_revision: Option<String>,
    /// observedGeneration is the most recent generation observed for this StatefulSet. It corresponds to the StatefulSet's generation, which is updated on mutation by the API Server.
    #[serde(default, rename = "observedGeneration", skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// readyReplicas is the number of pods created for this StatefulSet with a Ready Condition.
    #[serde(default, rename = "readyReplicas", skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    /// replicas is the number of Pods created by the StatefulSet controller.
    #[serde(default)]
    pub replicas: i32,
    /// updateRevision, if not empty, indicates the version of the StatefulSet used to generate Pods in the sequence [replicas-updatedReplicas,replicas)
    #[serde(default, rename = "updateRevision", skip_serializing_if = "Option::is_none")]
    pub update_revision: Option<String>,
    /// updatedReplicas is the number of Pods created by the StatefulSet controller from the StatefulSet version indicated by updateRevision.
    #[serde(default, rename = "updatedReplicas", skip_serializing_if = "Option::is_none")]
    pub updated_replicas: Option<i32>,
}

/// StatefulSetUpdateStrategy indicates the strategy that the StatefulSet controller will use to perform updates. It includes any additional parameters necessary to perform the update for the indicated strategy.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StatefulSetUpdateStrategy {
    /// RollingUpdate is used to communicate parameters when Type is RollingUpdateStatefulSetStrategyType.
    #[serde(default, rename = "rollingUpdate", skip_serializing_if = "Option::is_none")]
    pub rolling_update: Option<RollingUpdateStatefulSetStrategy>,
    /// Type indicates the type of the StatefulSetUpdateStrategy. Default is RollingUpdate.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

/// Represents a StorageOS persistent volume resource.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StorageOSPersistentVolumeSource {
    /// fsType is the filesystem type to mount. Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// readOnly defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretRef specifies the secret to use for obtaining the StorageOS API credentials.  If not specified, default values will be attempted.
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<ObjectReference>,
    /// volumeName is the human-readable name of the StorageOS volume.  Volume names are only unique within a namespace.
    #[serde(default, rename = "volumeName", skip_serializing_if = "Option::is_none")]
    pub volume_name: Option<String>,
    /// volumeNamespace specifies the scope of the volume within StorageOS.  If no namespace is specified then the Pod's namespace will be used.  This allows the Kubernetes name scoping to be mirrored within StorageOS for tighter integration. Set VolumeName to any name to override the default behaviour. Set to "default" if you are not using namespaces within StorageOS. Namespaces that do not pre-exist within StorageOS will be created.
    #[serde(default, rename = "volumeNamespace", skip_serializing_if = "Option::is_none")]
    pub volume_namespace: Option<String>,
}

/// Represents a StorageOS persistent volume resource.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StorageOSVolumeSource {
    /// fsType is the filesystem type to mount. Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// readOnly defaults to false (read/write). ReadOnly here will force the ReadOnly setting in VolumeMounts.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// secretRef specifies the secret to use for obtaining the StorageOS API credentials.  If not specified, default values will be attempted.
    #[serde(default, rename = "secretRef", skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<LocalObjectReference>,
    /// volumeName is the human-readable name of the StorageOS volume.  Volume names are only unique within a namespace.
    #[serde(default, rename = "volumeName", skip_serializing_if = "Option::is_none")]
    pub volume_name: Option<String>,
    /// volumeNamespace specifies the scope of the volume within StorageOS.  If no namespace is specified then the Pod's namespace will be used.  This allows the Kubernetes name scoping to be mirrored within StorageOS for tighter integration. Set VolumeName to any name to override the default behaviour. Set to "default" if you are not using namespaces within StorageOS. Namespaces that do not pre-exist within StorageOS will be created.
    #[serde(default, rename = "volumeNamespace", skip_serializing_if = "Option::is_none")]
    pub volume_namespace: Option<String>,
}

/// Subject contains a reference to the object or user identities a role binding applies to.  This can either hold a direct API object reference, or a value for non-objects such as user and group names.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Subject {
    /// APIGroup holds the API group of the referenced subject. Defaults to "" for ServiceAccount subjects. Defaults to "rbac.authorization.k8s.io" for User and Group subjects.
    #[serde(default, rename = "apiGroup", skip_serializing_if = "Option::is_none")]
    pub api_group: Option<String>,
    /// Name of the object being referenced.
    #[serde(default)]
    pub name: String,
    /// Namespace of the referenced object.  If the object kind is non-namespace, such as "User" or "Group", and this value is not empty the Authorizer should report an error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Sysctl defines a kernel parameter to be set
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Sysctl {
    /// Name of a property to set
    #[serde(default)]
    pub name: String,
    /// Value of a property to set
    #[serde(default)]
    pub value: String,
}

/// TCPSocketAction describes an action based on opening a socket
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TCPSocketAction {
    /// Optional: Host name to connect to, defaults to the pod IP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Number or name of the port to access on the container. Number must be in the range 1 to 65535. Name must be an IANA_SVC_NAME.
    #[serde(default)]
    pub port: IntOrString,
}

/// The node this Taint is attached to has the "effect" on any pod that does not tolerate the Taint.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Taint {
    /// Required. The effect of the taint on pods that do not tolerate the taint. Valid effects are NoSchedule, PreferNoSchedule and NoExecute.
    #[serde(default)]
    pub effect: String,
    /// Required. The taint key to be applied to a node.
    #[serde(default)]
    pub key: String,
    /// TimeAdded represents the time at which the taint was added.
    #[serde(default, rename = "timeAdded", skip_serializing_if = "Option::is_none")]
    pub time_added: Option<Time>,
    /// The taint value corresponding to the taint key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

/// Time is a wrapper around time.Time which supports correct marshaling to YAML and JSON.  Wrappers are provided for many of the factory methods that the time package offers.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Time {
}

/// The pod this Toleration is attached to tolerates any taint that matches the triple <key,value,effect> using the matching operator <operator>.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Toleration {
    /// Effect indicates the taint effect to match. Empty means match all taint effects. When specified, allowed values are NoSchedule, PreferNoSchedule and NoExecute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,
    /// Key is the taint key that the toleration applies to. Empty means match all taint keys. If the key is empty, operator must be Exists; this combination means to match all values and all keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Operator represents a key's relationship to the value. Valid operators are Exists and Equal. Defaults to Equal. Exists is equivalent to wildcard for value, so that a pod can tolerate all taints of a particular category.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    /// TolerationSeconds represents the period of time the toleration (which must be of effect NoExecute, otherwise this field is ignored) tolerates the taint. By default, it is not set, which means tolerate the taint forever (do not evict). Zero and negative values will be treated as 0 (evict immediately) by the system.
    #[serde(default, rename = "tolerationSeconds", skip_serializing_if = "Option::is_none")]
    pub toleration_seconds: Option<i64>,
    /// Value is the taint value the toleration matches to. If the operator is Exists, the value should be empty, otherwise just a regular string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

/// TopologySpreadConstraint specifies how to spread matching pods among the given topology.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TopologySpreadConstraint {
    /// LabelSelector is used to find matching pods. Pods that match this label selector are counted to determine the number of pods in their corresponding topology domain.
    #[serde(default, rename = "labelSelector", skip_serializing_if = "Option::is_none")]
    pub label_selector: Option<LabelSelector>,
    /// MatchLabelKeys is a set of pod label keys to select the pods over which spreading will be calculated. The keys are used to lookup values from the incoming pod labels, those key-value labels are ANDed with labelSelector to select the group of existing pods over which spreading will be calculated for the incoming pod. The same key is forbidden to exist in both MatchLabelKeys and LabelSelector. MatchLabelKeys cannot be set when LabelSelector isn't set. Keys that don't exist in the incoming pod labels will be ignored. A null or empty list means only match against labelSelector.
    #[serde(default, rename = "matchLabelKeys", skip_serializing_if = "Vec::is_empty")]
    pub match_label_keys: Vec<String>,
    /// MaxSkew describes the degree to which pods may be unevenly distributed. When `whenUnsatisfiable=DoNotSchedule`, it is the maximum permitted difference between the number of matching pods in the target topology and the global minimum. The global minimum is the minimum number of matching pods in an eligible domain or zero if the number of eligible domains is less than MinDomains. For example, in a 3-zone cluster, MaxSkew is set to 1, and pods with the same labelSelector spread as 2/2/1: In this case, the global minimum is 1. | zone1 | zone2 | zone3 | |  P P  |  P P  |   P   | - if MaxSkew is 1, incoming pod can only be scheduled to zone3 to become 2/2/2; scheduling it onto zone1(zone2) would make the ActualSkew(3-1) on zone1(zone2) violate MaxSkew(1). - if MaxSkew is 2, incoming pod can be scheduled onto any zone. When `whenUnsatisfiable=ScheduleAnyway`, it is used to give higher precedence to topologies that satisfy it. It's a required field. Default value is 1 and 0 is not allowed.
    #[serde(default, rename = "maxSkew")]
    pub max_skew: i32,
    /// MinDomains indicates a minimum number of eligible domains. When the number of eligible domains with matching topology keys is less than minDomains, Pod Topology Spread treats "global minimum" as 0, and then the calculation of Skew is performed. And when the number of eligible domains with matching topology keys equals or greater than minDomains, this value has no effect on scheduling. As a result, when the number of eligible domains is less than minDomains, scheduler won't schedule more than maxSkew Pods to those domains. If value is nil, the constraint behaves as if MinDomains is equal to 1. Valid values are integers greater than 0. When value is not nil, WhenUnsatisfiable must be DoNotSchedule.
    #[serde(default, rename = "minDomains", skip_serializing_if = "Option::is_none")]
    pub min_domains: Option<i32>,
    /// NodeAffinityPolicy indicates how we will treat Pod's nodeAffinity/nodeSelector when calculating pod topology spread skew. Options are: - Honor: only nodes matching nodeAffinity/nodeSelector are included in the calculations. - Ignore: nodeAffinity/nodeSelector are ignored. All nodes are included in the calculations.
    #[serde(default, rename = "nodeAffinityPolicy", skip_serializing_if = "Option::is_none")]
    pub node_affinity_policy: Option<String>,
    /// NodeTaintsPolicy indicates how we will treat node taints when calculating pod topology spread skew. Options are: - Honor: nodes without taints, along with tainted nodes for which the incoming pod has a toleration, are included. - Ignore: node taints are ignored. All nodes are included.
    #[serde(default, rename = "nodeTaintsPolicy", skip_serializing_if = "Option::is_none")]
    pub node_taints_policy: Option<String>,
    /// TopologyKey is the key of node labels. Nodes that have a label with this key and identical values are considered to be in the same topology. We consider each <key, value> as a "bucket", and try to put balanced number of pods into each bucket. We define a domain as a particular instance of a topology. Also, we define an eligible domain as a domain whose nodes meet the requirements of nodeAffinityPolicy and nodeTaintsPolicy. e.g. If TopologyKey is "kubernetes.io/hostname", each Node is a domain of that topology. And, if TopologyKey is "topology.kubernetes.io/zone", each zone is a domain of that topology. It's a required field.
    #[serde(default, rename = "topologyKey")]
    pub topology_key: String,
    /// WhenUnsatisfiable indicates how to deal with a pod if it doesn't satisfy the spread constraint. - DoNotSchedule (default) tells the scheduler not to schedule it. - ScheduleAnyway tells the scheduler to schedule the pod in any location,
    /// but giving higher precedence to topologies that would help reduce the
    /// skew.
    /// A constraint is considered "Unsatisfiable" for an incoming pod if and only if every possible node assignment for that pod would violate "MaxSkew" on some topology. For example, in a 3-zone cluster, MaxSkew is set to 1, and pods with the same labelSelector spread as 3/1/1: | zone1 | zone2 | zone3 | | P P P |   P   |   P   | If WhenUnsatisfiable is set to DoNotSchedule, incoming pod can only be scheduled to zone2(zone3) to become 3/2/1(3/1/2) as ActualSkew(2-1) on zone2(zone3) satisfies MaxSkew(1). In other words, the cluster can still be imbalanced, but scheduler won't make it *more* imbalanced. It's a required field.
    #[serde(default, rename = "whenUnsatisfiable")]
    pub when_unsatisfiable: String,
}

/// TypedLocalObjectReference contains enough information to let you locate the typed referenced object inside the same namespace.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TypedLocalObjectReference {
    /// APIGroup is the group for the resource being referenced. If APIGroup is not specified, the specified Kind must be in the core API group. For any other third-party types, APIGroup is required.
    #[serde(default, rename = "apiGroup", skip_serializing_if = "Option::is_none")]
    pub api_group: Option<String>,
    /// Name is the name of resource being referenced
    #[serde(default)]
    pub name: String,
}

/// TypedObjectReference contains enough information to let you locate the typed referenced object
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TypedObjectReference {
    /// APIGroup is the group for the resource being referenced. If APIGroup is not specified, the specified Kind must be in the core API group. For any other third-party types, APIGroup is required.
    #[serde(default, rename = "apiGroup", skip_serializing_if = "Option::is_none")]
    pub api_group: Option<String>,
    /// Name is the name of resource being referenced
    #[serde(default)]
    pub name: String,
    /// Namespace is the namespace of resource being referenced Note that when a namespace is specified, a gateway.networking.k8s.io/ReferenceGrant object is required in the referent namespace to allow that namespace's owner to accept the reference. See the ReferenceGrant documentation for details. (Alpha) This field requires the CrossNamespaceVolumeDataSource feature gate to be enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Volume represents a named volume in a pod that may be accessed by any container in the pod.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Volume {
    /// awsElasticBlockStore represents an AWS Disk resource that is attached to a kubelet's host machine and then exposed to the pod. Deprecated: AWSElasticBlockStore is deprecated. All operations for the in-tree awsElasticBlockStore type are redirected to the ebs.csi.aws.com CSI driver. More info: https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore
    #[serde(default, rename = "awsElasticBlockStore", skip_serializing_if = "Option::is_none")]
    pub aws_elastic_block_store: Option<AWSElasticBlockStoreVolumeSource>,
    /// azureDisk represents an Azure Data Disk mount on the host and bind mount to the pod. Deprecated: AzureDisk is deprecated. All operations for the in-tree azureDisk type are redirected to the disk.csi.azure.com CSI driver.
    #[serde(default, rename = "azureDisk", skip_serializing_if = "Option::is_none")]
    pub azure_disk: Option<AzureDiskVolumeSource>,
    /// azureFile represents an Azure File Service mount on the host and bind mount to the pod. Deprecated: AzureFile is deprecated. All operations for the in-tree azureFile type are redirected to the file.csi.azure.com CSI driver.
    #[serde(default, rename = "azureFile", skip_serializing_if = "Option::is_none")]
    pub azure_file: Option<AzureFileVolumeSource>,
    /// cephFS represents a Ceph FS mount on the host that shares a pod's lifetime. Deprecated: CephFS is deprecated and the in-tree cephfs type is no longer supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cephfs: Option<CephFSVolumeSource>,
    /// cinder represents a cinder volume attached and mounted on kubelets host machine. Deprecated: Cinder is deprecated. All operations for the in-tree cinder type are redirected to the cinder.csi.openstack.org CSI driver. More info: https://examples.k8s.io/mysql-cinder-pd/README.md
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cinder: Option<CinderVolumeSource>,
    /// configMap represents a configMap that should populate this volume
    #[serde(default, rename = "configMap", skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapVolumeSource>,
    /// csi (Container Storage Interface) represents ephemeral storage that is handled by certain external CSI drivers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub csi: Option<CSIVolumeSource>,
    /// downwardAPI represents downward API about the pod that should populate this volume
    #[serde(default, rename = "downwardAPI", skip_serializing_if = "Option::is_none")]
    pub downward_api: Option<DownwardAPIVolumeSource>,
    /// emptyDir represents a temporary directory that shares a pod's lifetime. More info: https://kubernetes.io/docs/concepts/storage/volumes#emptydir
    #[serde(default, rename = "emptyDir", skip_serializing_if = "Option::is_none")]
    pub empty_dir: Option<EmptyDirVolumeSource>,
    /// ephemeral represents a volume that is handled by a cluster storage driver. The volume's lifecycle is tied to the pod that defines it - it will be created before the pod starts, and deleted when the pod is removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<EphemeralVolumeSource>,
    /// fc represents a Fibre Channel resource that is attached to a kubelet's host machine and then exposed to the pod.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fc: Option<FCVolumeSource>,
    /// flexVolume represents a generic volume resource that is provisioned/attached using an exec based plugin. Deprecated: FlexVolume is deprecated. Consider using a CSIDriver instead.
    #[serde(default, rename = "flexVolume", skip_serializing_if = "Option::is_none")]
    pub flex_volume: Option<FlexVolumeSource>,
    /// flocker represents a Flocker volume attached to a kubelet's host machine. This depends on the Flocker control service being running. Deprecated: Flocker is deprecated and the in-tree flocker type is no longer supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flocker: Option<FlockerVolumeSource>,
    /// gcePersistentDisk represents a GCE Disk resource that is attached to a kubelet's host machine and then exposed to the pod. Deprecated: GCEPersistentDisk is deprecated. All operations for the in-tree gcePersistentDisk type are redirected to the pd.csi.storage.gke.io CSI driver. More info: https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk
    #[serde(default, rename = "gcePersistentDisk", skip_serializing_if = "Option::is_none")]
    pub gce_persistent_disk: Option<GCEPersistentDiskVolumeSource>,
    /// gitRepo represents a git repository at a particular revision. Deprecated: GitRepo is deprecated. To provision a container with a git repo, mount an EmptyDir into an InitContainer that clones the repo using git, then mount the EmptyDir into the Pod's container.
    #[serde(default, rename = "gitRepo", skip_serializing_if = "Option::is_none")]
    pub git_repo: Option<GitRepoVolumeSource>,
    /// glusterfs represents a Glusterfs mount on the host that shares a pod's lifetime. Deprecated: Glusterfs is deprecated and the in-tree glusterfs type is no longer supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub glusterfs: Option<GlusterfsVolumeSource>,
    /// hostPath represents a pre-existing file or directory on the host machine that is directly exposed to the container. This is generally used for system agents or other privileged things that are allowed to see the host machine. Most containers will NOT need this. More info: https://kubernetes.io/docs/concepts/storage/volumes#hostpath
    #[serde(default, rename = "hostPath", skip_serializing_if = "Option::is_none")]
    pub host_path: Option<HostPathVolumeSource>,
    /// image represents an OCI object (a container image or artifact) pulled and mounted on the kubelet's host machine. The volume is resolved at pod startup depending on which PullPolicy value is provided:
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<ImageVolumeSource>,
    /// iscsi represents an ISCSI Disk resource that is attached to a kubelet's host machine and then exposed to the pod. More info: https://kubernetes.io/docs/concepts/storage/volumes/#iscsi
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iscsi: Option<ISCSIVolumeSource>,
    /// name of the volume. Must be a DNS_LABEL and unique within the pod. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
    #[serde(default)]
    pub name: String,
    /// nfs represents an NFS mount on the host that shares a pod's lifetime More info: https://kubernetes.io/docs/concepts/storage/volumes#nfs
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nfs: Option<NFSVolumeSource>,
    /// persistentVolumeClaimVolumeSource represents a reference to a PersistentVolumeClaim in the same namespace. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
    #[serde(default, rename = "persistentVolumeClaim", skip_serializing_if = "Option::is_none")]
    pub persistent_volume_claim: Option<PersistentVolumeClaimVolumeSource>,
    /// photonPersistentDisk represents a PhotonController persistent disk attached and mounted on kubelets host machine. Deprecated: PhotonPersistentDisk is deprecated and the in-tree photonPersistentDisk type is no longer supported.
    #[serde(default, rename = "photonPersistentDisk", skip_serializing_if = "Option::is_none")]
    pub photon_persistent_disk: Option<PhotonPersistentDiskVolumeSource>,
    /// portworxVolume represents a portworx volume attached and mounted on kubelets host machine. Deprecated: PortworxVolume is deprecated. All operations for the in-tree portworxVolume type are redirected to the pxd.portworx.com CSI driver when the CSIMigrationPortworx feature-gate is on.
    #[serde(default, rename = "portworxVolume", skip_serializing_if = "Option::is_none")]
    pub portworx_volume: Option<PortworxVolumeSource>,
    /// projected items for all in one resources secrets, configmaps, and downward API
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projected: Option<ProjectedVolumeSource>,
    /// quobyte represents a Quobyte mount on the host that shares a pod's lifetime. Deprecated: Quobyte is deprecated and the in-tree quobyte type is no longer supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quobyte: Option<QuobyteVolumeSource>,
    /// rbd represents a Rados Block Device mount on the host that shares a pod's lifetime. Deprecated: RBD is deprecated and the in-tree rbd type is no longer supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rbd: Option<RBDVolumeSource>,
    /// scaleIO represents a ScaleIO persistent volume attached and mounted on Kubernetes nodes. Deprecated: ScaleIO is deprecated and the in-tree scaleIO type is no longer supported.
    #[serde(default, rename = "scaleIO", skip_serializing_if = "Option::is_none")]
    pub scale_io: Option<ScaleIOVolumeSource>,
    /// secret represents a secret that should populate this volume. More info: https://kubernetes.io/docs/concepts/storage/volumes#secret
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretVolumeSource>,
    /// storageOS represents a StorageOS volume attached and mounted on Kubernetes nodes. Deprecated: StorageOS is deprecated and the in-tree storageos type is no longer supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storageos: Option<StorageOSVolumeSource>,
    /// vsphereVolume represents a vSphere volume attached and mounted on kubelets host machine. Deprecated: VsphereVolume is deprecated. All operations for the in-tree vsphereVolume type are redirected to the csi.vsphere.vmware.com CSI driver.
    #[serde(default, rename = "vsphereVolume", skip_serializing_if = "Option::is_none")]
    pub vsphere_volume: Option<VsphereVirtualDiskVolumeSource>,
}

/// volumeDevice describes a mapping of a raw block device within a container.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VolumeDevice {
    /// devicePath is the path inside of the container that the device will be mapped to.
    #[serde(default, rename = "devicePath")]
    pub device_path: String,
    /// name must match the name of a persistentVolumeClaim in the pod
    #[serde(default)]
    pub name: String,
}

/// VolumeMount describes a mounting of a Volume within a container.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VolumeMount {
    /// Path within the container at which the volume should be mounted.  Must not contain ':'.
    #[serde(default, rename = "mountPath")]
    pub mount_path: String,
    /// mountPropagation determines how mounts are propagated from the host to container and the other way around. When not set, MountPropagationNone is used. This field is beta in 1.10. When RecursiveReadOnly is set to IfPossible or to Enabled, MountPropagation must be None or unspecified (which defaults to None).
    #[serde(default, rename = "mountPropagation", skip_serializing_if = "Option::is_none")]
    pub mount_propagation: Option<String>,
    /// This must match the Name of a Volume.
    #[serde(default)]
    pub name: String,
    /// Mounted read-only if true, read-write otherwise (false or unspecified). Defaults to false.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// RecursiveReadOnly specifies whether read-only mounts should be handled recursively.
    #[serde(default, rename = "recursiveReadOnly", skip_serializing_if = "Option::is_none")]
    pub recursive_read_only: Option<String>,
    /// Path within the volume from which the container's volume should be mounted. Defaults to "" (volume's root).
    #[serde(default, rename = "subPath", skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
    /// Expanded path within the volume from which the container's volume should be mounted. Behaves similarly to SubPath but environment variable references $(VAR_NAME) are expanded using the container's environment. Defaults to "" (volume's root). SubPathExpr and SubPath are mutually exclusive.
    #[serde(default, rename = "subPathExpr", skip_serializing_if = "Option::is_none")]
    pub sub_path_expr: Option<String>,
}

/// VolumeMountStatus shows status of volume mounts.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VolumeMountStatus {
    /// MountPath corresponds to the original VolumeMount.
    #[serde(default, rename = "mountPath")]
    pub mount_path: String,
    /// Name corresponds to the name of the original VolumeMount.
    #[serde(default)]
    pub name: String,
    /// ReadOnly corresponds to the original VolumeMount.
    #[serde(default, rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// RecursiveReadOnly must be set to Disabled, Enabled, or unspecified (for non-readonly mounts). An IfPossible value in the original VolumeMount must be translated to Disabled or Enabled, depending on the mount result.
    #[serde(default, rename = "recursiveReadOnly", skip_serializing_if = "Option::is_none")]
    pub recursive_read_only: Option<String>,
}

/// VolumeNodeAffinity defines constraints that limit what nodes this volume can be accessed from.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VolumeNodeAffinity {
    /// required specifies hard node constraints that must be met.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required: Option<NodeSelector>,
}

/// Projection that may be projected along with other supported volume types. Exactly one of these fields must be set.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VolumeProjection {
    /// ClusterTrustBundle allows a pod to access the `.spec.trustBundle` field of ClusterTrustBundle objects in an auto-updating file.
    #[serde(default, rename = "clusterTrustBundle", skip_serializing_if = "Option::is_none")]
    pub cluster_trust_bundle: Option<ClusterTrustBundleProjection>,
    /// configMap information about the configMap data to project
    #[serde(default, rename = "configMap", skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapProjection>,
    /// downwardAPI information about the downwardAPI data to project
    #[serde(default, rename = "downwardAPI", skip_serializing_if = "Option::is_none")]
    pub downward_api: Option<DownwardAPIProjection>,
    /// Projects an auto-rotating credential bundle (private key and certificate chain) that the pod can use either as a TLS client or server.
    #[serde(default, rename = "podCertificate", skip_serializing_if = "Option::is_none")]
    pub pod_certificate: Option<PodCertificateProjection>,
    /// secret information about the secret data to project
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretProjection>,
    /// serviceAccountToken is information about the serviceAccountToken data to project
    #[serde(default, rename = "serviceAccountToken", skip_serializing_if = "Option::is_none")]
    pub service_account_token: Option<ServiceAccountTokenProjection>,
}

/// VolumeResourceRequirements describes the storage resource requirements for a volume.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VolumeResourceRequirements {
    /// Limits describes the maximum amount of compute resources allowed. More info: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub limits: std::collections::BTreeMap<String, Quantity>,
    /// Requests describes the minimum amount of compute resources required. If Requests is omitted for a container, it defaults to Limits if that is explicitly specified, otherwise to an implementation-defined value. Requests cannot exceed Limits. More info: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub requests: std::collections::BTreeMap<String, Quantity>,
}

/// Represents a vSphere volume resource.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VsphereVirtualDiskVolumeSource {
    /// fsType is filesystem type to mount. Must be a filesystem type supported by the host operating system. Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
    #[serde(default, rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    /// storagePolicyID is the storage Policy Based Management (SPBM) profile ID associated with the StoragePolicyName.
    #[serde(default, rename = "storagePolicyID", skip_serializing_if = "Option::is_none")]
    pub storage_policy_id: Option<String>,
    /// storagePolicyName is the storage Policy Based Management (SPBM) profile name.
    #[serde(default, rename = "storagePolicyName", skip_serializing_if = "Option::is_none")]
    pub storage_policy_name: Option<String>,
    /// volumePath is the path that identifies vSphere volume vmdk
    #[serde(default, rename = "volumePath")]
    pub volume_path: String,
}

/// The weights of all of the matched WeightedPodAffinityTerm fields are added per-node to find the most preferred node(s)
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WeightedPodAffinityTerm {
    /// Required. A pod affinity term, associated with the corresponding weight.
    #[serde(default, rename = "podAffinityTerm")]
    pub pod_affinity_term: PodAffinityTerm,
    /// weight associated with matching the corresponding podAffinityTerm, in the range 1-100.
    #[serde(default)]
    pub weight: i32,
}

/// WindowsSecurityContextOptions contain Windows-specific options and credentials.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WindowsSecurityContextOptions {
    /// GMSACredentialSpec is where the GMSA admission webhook (https://github.com/kubernetes-sigs/windows-gmsa) inlines the contents of the GMSA credential spec named by the GMSACredentialSpecName field.
    #[serde(default, rename = "gmsaCredentialSpec", skip_serializing_if = "Option::is_none")]
    pub gmsa_credential_spec: Option<String>,
    /// GMSACredentialSpecName is the name of the GMSA credential spec to use.
    #[serde(default, rename = "gmsaCredentialSpecName", skip_serializing_if = "Option::is_none")]
    pub gmsa_credential_spec_name: Option<String>,
    /// HostProcess determines if a container should be run as a 'Host Process' container. All of a Pod's containers must have the same effective HostProcess value (it is not allowed to have a mix of HostProcess containers and non-HostProcess containers). In addition, if HostProcess is true then HostNetwork must also be set to true.
    #[serde(default, rename = "hostProcess", skip_serializing_if = "Option::is_none")]
    pub host_process: Option<bool>,
    /// The UserName in Windows to run the entrypoint of the container process. Defaults to the user specified in image metadata if unspecified. May also be set in PodSecurityContext. If set in both SecurityContext and PodSecurityContext, the value specified in SecurityContext takes precedence.
    #[serde(default, rename = "runAsUserName", skip_serializing_if = "Option::is_none")]
    pub run_as_user_name: Option<String>,
}

fn is_empty_meta(m: &crate::meta::ObjectMeta) -> bool { m == &crate::meta::ObjectMeta::default() }
