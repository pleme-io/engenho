use std::path::Path;

use engenho_config::EngenhoConfig;

pub fn place(cfg: &mut EngenhoConfig, data_dir: &Path) {
    cfg.runtime.data_dir = data_dir.to_path_buf();
    cfg.runtime.node_manifests_dir = data_dir.join("manifests.d");
}
