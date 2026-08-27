# engenho — the TYPE-STRICT configuration surface.
#
# ── WHY THIS EXISTS ALONGSIDE THE TRIO ────────────────────────────────────
# `substrate/lib/module-trio.nix` gives engenho its delivery: package install,
# launchd/systemd units, and a `settings` option deployed as YAML. But that
# option is `types.attrs` — a FREEFORM attrset. A typo in it (`data_dr`, or
# `kubelet_backend = "docker"`) evaluates perfectly, renders perfectly, and
# fails at engenho's own config parse on the node, at boot, in a log nobody is
# watching.
#
# This module makes those illegal states unrepresentable at EVAL time instead:
# every leaf engenho reads is a typed option, and the rendered YAML is a
# projection of them. `nixos-rebuild` refuses a bad value; the node never sees
# one.
#
# ── ONE MODULE, ALL THREE ARMS ────────────────────────────────────────────
# The trio is instantiated with `hmNamespace = "services"`, so the option path
# is `services.engenho.*` on NixOS, Darwin AND home-manager. That is what lets
# this single typed module serve all three rather than being written out three
# times with three chances to drift.
#
# ── WHAT IS DELIBERATELY NOT AN ENUM ──────────────────────────────────────
# `revoada.topology.strategy` is `types.str`, not `types.enum`, and that is a
# measured decision rather than laziness. Its Rust enum contains `Quorum3M` and
# `Cluster3MNW`; serde's `rename_all = "snake_case"` encoding of variants that
# mix digits and consecutive capitals is not obvious (`quorum3_m`?
# `cluster3_m_n_w`? `cluster3_mnw`?), and an attempt to verify it empirically
# produced a VACUOUS result — the probe returned the default for a deliberately
# bogus value too, so it could not distinguish right from wrong.
#
# Listing guessed variants in a `types.enum` would be worse than a plain
# string: it would REJECT the two spellings that are actually correct while
# looking authoritative. The four unambiguous ones are named in the
# description; when the wire encoding is measured, this becomes an enum.
{ lib, config, ... }:
let
  inherit (lib) mkOption mkEnableOption types;

  # A leaf whose value is only emitted when the operator sets it. Absent
  # options must not appear in the YAML at all — engenho's own tiered fold
  # supplies its prescribed defaults, and a null written into the file would
  # OVERRIDE that fold with an explicit null rather than deferring to it.
  optional = type: description: mkOption {
    inherit description;
    type = types.nullOr type;
    default = null;
  };

  controllerToggles = {
    replicaset = true; deployment = true; statefulset = true; daemonset = true;
    job = true; cronjob = true; endpoints = true; service_routing = true;
    gc = true; crd = true; namespace = true; pv_binder = true; pdb = true;
  };
in
{
  options.services.engenho.config = {
    cluster = {
      name = optional types.str ''
        Cluster identity. Defaults, in engenho itself, to a DERIVED per-node
        name (`engenho-<node>-<hash8>`) so every node has its own cluster
        without configuration and two nodes never collide.

        Leave unset unless you specifically want a shared name — setting it
        pins every node that imports this module to ONE identity, which is the
        collision the derivation exists to prevent.
      '';
      region = optional types.str "Cloud region / homelab location identifier.";
    };

    runtime = {
      listenAddr = optional types.str
        "apiserver bind address. `127.0.0.1:6443` keeps it node-local.";
      dataDir = optional types.path
        "Durable store root. The fjall store lives at `<dataDir>/store`.";
      durable = optional types.bool
        "`false` uses an ephemeral in-memory store — tests and dev only.";
      nodeName = optional types.str ''
        This node's name. The kubelet binds Pods whose `spec.nodeName` matches,
        and the Runtime self-registers a schedulable `Node/<nodeName>`.
        Defaults to the detected hostname.
      '';
      kubeconfigPublishPath = optional types.str ''
        Where the boot kubeconfig is published so kubectl / k9s / flux find it
        without being told. Defaults to `~/.kube/configs/engenho`, which the
        fleet's `pleme.kubeconfigs` list folds into `$KUBECONFIG`.

        Empty string disables publishing. `~/` is expanded by engenho at write
        time, not here, so a rendered config stays valid for any user.
      '';
      kubeletBackend = optional (types.enum [ "podman" "fake" ]) ''
        Container runtime the kubelet drives.

        `fake` runs NOTHING — it is the mock backend, correct for a
        control-plane-only node and wrong anywhere pods must actually run.
        On macOS `podman` is itself a Linux VM, so neither option yet gives
        real pods on baremetal.
      '';
      podmanBinary = optional types.path
        "Explicit podman path. Unset resolves it from `$PATH`.";
      leadershipTimeoutSeconds = optional types.ints.unsigned
        "Leadership lease timeout.";
      tls = {
        enabled = optional types.bool "Serve the apiserver over TLS.";
        autoGenerate = optional types.bool
          "Generate and persist a cluster CA at boot when absent.";
      };
    };

    scheduler = {
      strategy = optional (types.enum [ "round_robin" "bin_pack" "affinity" ])
        "Pod placement strategy.";
      namespace = optional types.str
        "Restrict scheduling to one namespace. Empty = all.";
      tickIntervalSeconds = optional types.ints.unsigned "Scheduler tick period.";
    };

    controllers = {
      enable = mkOption {
        type = types.attrsOf types.bool;
        default = { };
        example = { gc = false; pdb = false; };
        description = ''
          Per-controller on/off. Only the keys you set are emitted; the rest
          keep engenho's defaults (all on).

          Known controllers: ${lib.concatStringsSep ", "
            (lib.attrNames controllerToggles)}.
        '';
      };
      namespace = optional types.str
        "Restrict controllers to one namespace. Empty = all.";
      fallbackIntervalSeconds = optional types.ints.unsigned
        "Resync period when no watch event arrives.";
      debounceMilliseconds = optional types.ints.unsigned
        "Watch-event debounce window.";
    };

    consistency.defaultTier = optional
      (types.enum [ "strong" "eventual_gossip" "durable_stream" "content" ])
      "Default consistency tier for store reads.";

    networking = {
      serviceCidr = optional types.str "ClusterIP allocation range.";
      datapathMode = optional
        (types.enum [ "auto" "iptables" "ipvs" "compute_only" ])
        "Service routing datapath. `compute_only` disables service routing.";
    };

    revoada.topology = {
      strategy = optional types.str ''
        Multi-node topology strategy. Unambiguous values: `solo`, `pair`,
        `mesh_all_peers`, `phalanx`. Two further Rust variants (`Quorum3M`,
        `Cluster3MNW`) exist whose snake_case wire spelling is unverified —
        see this file's header for why they are not enumerated.
      '';
      minNodes = optional types.ints.unsigned "Minimum nodes before scheduling.";
      gracePeriodSeconds = optional types.ints.unsigned
        "Grace period before topology reacts to a membership change.";
    };

    teia = {
      servers = mkOption {
        type = types.listOf types.str;
        default = [ ];
        example = [ "nats://127.0.0.1:4222" ];
        description = "NATS servers for the teia mesh. Empty keeps the default.";
      };
      cluster = optional types.str ''
        teia subject namespace.

        NOTE: engenho currently defaults this to the literal `engenho-local`,
        INDEPENDENTLY of `cluster.name` — a second hardcoded copy of the
        cluster identity. Until that is derived upstream, set both together or
        neither.
      '';
      credentialsPath = optional types.path "NATS credentials file.";
      connectTimeoutSeconds = optional types.ints.unsigned "NATS connect timeout.";
    };
  };

  # ── The projection: typed options → the trio's YAML `settings` ──────────
  #
  # `mkDefault` so an operator can still drop to raw `services.engenho.settings`
  # for a key this surface does not model yet, without being blocked by a
  # conflict. Typed values win over engenho's internal defaults; raw settings
  # win over both.
  config.services.engenho.settings =
    let
      cfg = config.services.engenho.config;
      # Drop every unset leaf. An emitted `null` is NOT the same as absence:
      # engenho's progressive fold treats a present key as an explicit
      # operator opinion and stops deferring to its own prescribed default.
      prune = attrs:
        lib.filterAttrs (_: v: v != null && v != { } && v != [ ])
          (lib.mapAttrs (_: v: if lib.isAttrs v && !lib.isDerivation v then prune v else v) attrs);
    in
    lib.mkDefault (prune {
      cluster = { inherit (cfg.cluster) name region; };
      runtime = {
        listen_addr = cfg.runtime.listenAddr;
        data_dir = cfg.runtime.dataDir;
        durable = cfg.runtime.durable;
        node_name = cfg.runtime.nodeName;
        kubeconfig_publish_path = cfg.runtime.kubeconfigPublishPath;
        kubelet_backend = cfg.runtime.kubeletBackend;
        podman_binary = cfg.runtime.podmanBinary;
        leadership_timeout_seconds = cfg.runtime.leadershipTimeoutSeconds;
        tls = {
          enabled = cfg.runtime.tls.enabled;
          auto_generate = cfg.runtime.tls.autoGenerate;
        };
      };
      scheduler = {
        strategy = cfg.scheduler.strategy;
        namespace = cfg.scheduler.namespace;
        tick_interval_seconds = cfg.scheduler.tickIntervalSeconds;
      };
      controllers = {
        enable = cfg.controllers.enable;
        namespace = cfg.controllers.namespace;
        fallback_interval_seconds = cfg.controllers.fallbackIntervalSeconds;
        debounce_milliseconds = cfg.controllers.debounceMilliseconds;
      };
      consistency.default_tier = cfg.consistency.defaultTier;
      networking = {
        service_cidr = cfg.networking.serviceCidr;
        datapath_mode = cfg.networking.datapathMode;
      };
      revoada.topology = {
        strategy = cfg.revoada.topology.strategy;
        min_nodes = cfg.revoada.topology.minNodes;
        grace_period_seconds = cfg.revoada.topology.gracePeriodSeconds;
      };
      teia = {
        servers = cfg.teia.servers;
        cluster = cfg.teia.cluster;
        credentials_path = cfg.teia.credentialsPath;
        connect_timeout_seconds = cfg.teia.connectTimeoutSeconds;
      };
    });
}
