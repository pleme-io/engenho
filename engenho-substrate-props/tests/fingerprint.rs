//! Property: Fingerprint trait — generic determinism + divergence.

use engenho_substrate::{
    Fingerprint, Linhagem, NodeId, Placement, Plantio, SearchId, Stage, StageId, WorkloadShape,
    fingerprint_blake3,
};
use engenho_substrate_props::proptest_with_env;
use proptest::prelude::*;

proptest_with_env! {
    /// fingerprint_blake3() helper is deterministic for any serde value.
    #[test]
    fn helper_is_deterministic_for_serde_value(
        bytes in proptest::collection::vec(any::<u8>(), 0..512),
    ) {
        let f1 = fingerprint_blake3(&bytes);
        let f2 = fingerprint_blake3(&bytes);
        prop_assert_eq!(f1, f2);
    }

    /// fingerprint() on a Plantio is deterministic.
    #[test]
    fn plantio_fingerprint_deterministic(
        n_stages in 1usize..8,
    ) {
        let mut p1 = Plantio::new();
        let mut p2 = Plantio::new();
        let node = NodeId::new([0u8; 32]);
        for i in 0..n_stages {
            let s1 = Stage::pinned(
                format!("s-{i:02}"),
                WorkloadShape::OciImage,
                node,
            );
            let mut s2 = Stage::pinned(
                format!("s-{i:02}"),
                WorkloadShape::OciImage,
                node,
            );
            // Same content; insertion-ordered via BTreeMap so
            // fingerprints match regardless of insert order.
            s2.placement = Placement::Pinned { node };
            p1.add_stage(s1).unwrap();
            p2.add_stage(s2).unwrap();
        }
        prop_assert_eq!(
            <Plantio as Fingerprint>::fingerprint(&p1),
            <Plantio as Fingerprint>::fingerprint(&p2),
        );
    }

    /// Different Plantios produce different fingerprints (overwhelming).
    #[test]
    fn plantio_fingerprint_diverges_when_stage_added(
        n_stages in 1usize..6,
    ) {
        let mut p1 = Plantio::new();
        let node = NodeId::new([0u8; 32]);
        for i in 0..n_stages {
            p1.add_stage(Stage::pinned(
                format!("s-{i:02}"),
                WorkloadShape::OciImage,
                node,
            ))
            .unwrap();
        }
        let mut p2 = p1.clone();
        p2.add_stage(Stage::pinned("extra", WorkloadShape::Wasm, node))
            .unwrap();
        prop_assert_ne!(p1.fingerprint(), p2.fingerprint());
    }

    /// Linhagem trait impl agrees with inherent method.
    #[test]
    fn linhagem_trait_and_inherent_agree(
        search in "[a-z]{1,16}",
        gen_count in 0usize..16,
    ) {
        let mut l = Linhagem::new(SearchId::new(search));
        for i in 0..gen_count {
            l.extend(engenho_substrate::GeracaoId([i as u8; 32]));
        }
        let inherent = Linhagem::fingerprint(&l);
        let trait_dispatched = <Linhagem as Fingerprint>::fingerprint(&l);
        prop_assert_eq!(inherent, trait_dispatched);
    }

    /// Polymorphic dispatch through trait objects works for any
    /// substrate value implementing Fingerprint.
    #[test]
    fn polymorphic_dispatch_via_trait_object(
        n_stages in 1usize..8,
    ) {
        let mut p = Plantio::new();
        let node = NodeId::new([1u8; 32]);
        for i in 0..n_stages {
            p.add_stage(Stage::pinned(
                format!("s-{i:02}"),
                WorkloadShape::OciImage,
                node,
            ))
            .unwrap();
        }
        let l = Linhagem::new(SearchId::new("test"));
        let fps: Vec<&dyn Fingerprint> = vec![&p, &l];
        let collected: Vec<[u8; 32]> = fps.iter().map(|f| f.fingerprint()).collect();
        // Both produce 32-byte hashes; distinct types → distinct hashes.
        prop_assert_eq!(collected[0].len(), 32);
        prop_assert_eq!(collected[1].len(), 32);
        prop_assert_ne!(collected[0], collected[1]);
        // Suppress unused-binding lint:
        let _ = StageId::new("noop");
    }
}
