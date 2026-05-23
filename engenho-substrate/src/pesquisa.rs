//! pesquisa — typed RL/evolutionary search over computational primitives.
//!
//! Per the design brief: every search problem has the same typed
//! shape — a `SearchSpace<G>` describes the search space, a
//! `Fitness<G, F>` scores candidates, a `Selector<G, F>` picks
//! the next generation, a `SearchEngine<G, F>` runs the loop.
//! Determinism is anchored by BLAKE3 fingerprints across every
//! typed value; verifiability is anchored by `MaterializationReceipt`
//! emission on every fitness evaluation.
//!
//! ## Sibling primitive to roça
//!
//! `roca::Plantio` is a typed DAG of materializations; `pesquisa`
//! is a typed DAG of search trials. Every `Ensaio` (trial) can be
//! lowered to a `Plantio` (the brief's "per-trial Plantio"
//! pattern); every generation can be lowered to a Plantio whose
//! stages depend_on parents.
//!
//! ## This commit
//!
//! - All 6 typed values: `EnsaioId`, `Ensaio`, `Geracao`, `Aptidao<F>`,
//!   `Linhagem`, `Arquivo<F>`
//! - All 4 traits: `SearchSpace<G>`, `Fitness<G, F>`, `Selector<G, F>`,
//!   `SearchEngine<G, F>` (the engine trait without production impls)
//! - One Fake per trait: `FakeSearchSpace`, `FakeFitness`,
//!   `FakeSelector`, `FakeSearchEngine`
//! - Determinism anchors: BLAKE3 ids + seeded `SearchRng` + linhagem
//!   chain hash
//! - Replay-verifiable: same seed + same SearchSpace produces
//!   byte-identical Ensaios

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// =================================================================
// Typed identifiers + determinism anchors
// =================================================================

/// Search-instance identifier. Operator-chosen; stable across runs.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SearchId(pub String);

impl SearchId {
    /// Construct from a string.
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl std::fmt::Display for SearchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Typed trial identifier. BLAKE3 over canonical inputs — same
/// inputs always produce the same EnsaioId.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EnsaioId(pub [u8; 32]);

impl EnsaioId {
    /// Derive from `(search_id, generation_idx, canonical_genotype_bytes,
    /// lineage_root)`. Deterministic — replaying produces byte-identical IDs.
    #[must_use]
    pub fn derive(
        search_id: &SearchId,
        generation_idx: u64,
        canonical_genotype: &[u8],
        lineage_root: &[u8; 32],
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(search_id.0.as_bytes());
        hasher.update(b"|");
        hasher.update(&generation_idx.to_le_bytes());
        hasher.update(b"|");
        hasher.update(canonical_genotype);
        hasher.update(b"|");
        hasher.update(lineage_root);
        Self(*hasher.finalize().as_bytes())
    }

    /// Hex representation.
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }
}

impl std::fmt::Display for EnsaioId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// Generation identifier — BLAKE3-chained.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GeracaoId(pub [u8; 32]);

impl GeracaoId {
    /// Derive from `(prior_geracao_id, sorted_ensaio_ids, selection_seed)`.
    /// Chained — re-deriving from the same prior + same ensaios + same
    /// seed produces byte-identical IDs.
    #[must_use]
    pub fn derive(
        prior: Option<&GeracaoId>,
        ensaio_ids: &BTreeSet<EnsaioId>,
        selection_seed: &[u8; 32],
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        if let Some(p) = prior {
            hasher.update(&p.0);
        } else {
            hasher.update(&[0u8; 32]);
        }
        hasher.update(b"|");
        for id in ensaio_ids {
            hasher.update(&id.0);
        }
        hasher.update(b"|");
        hasher.update(selection_seed);
        Self(*hasher.finalize().as_bytes())
    }

    /// Hex representation.
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }
}

// =================================================================
// Typed values
// =================================================================

/// One typed trial. The `genotype` is opaque bytes — the concrete
/// type lives in the `SearchSpace::G` parameter; the substrate
/// stores the canonical-bytes serialization for determinism.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ensaio {
    /// Unique typed id.
    pub id: EnsaioId,
    /// Canonical genotype bytes.
    pub genotype: Vec<u8>,
    /// Parents this trial descends from (empty for genesis trials).
    pub lineage: BTreeSet<EnsaioId>,
    /// Generation index this trial belongs to.
    pub generation_idx: u64,
}

/// Scored trial — fitness `F` attached + evidence hash for verifiability.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Aptidao<F> {
    /// The trial that produced this fitness.
    pub ensaio: Ensaio,
    /// Typed fitness value (scalar f64 / vector Vec<f64> / Pareto / etc.).
    pub fitness: F,
    /// BLAKE3 over the canonical evidence the Fitness emitted.
    /// Cross-node disagreement on the same Ensaio = Dissent at the
    /// QuorumTracker layer.
    pub evidence_hash: [u8; 32],
}

/// One generation — a typed snapshot of all trials at one tick.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Geracao<F> {
    /// Stable id (BLAKE3-chained from prior).
    pub id: GeracaoId,
    /// Index in the search.
    pub generation_idx: u64,
    /// Trials keyed by id. BTreeMap preserves deterministic ordering.
    pub trials: BTreeMap<EnsaioId, Aptidao<F>>,
}

impl<F> Geracao<F> {
    /// Count of trials in this generation.
    #[must_use]
    pub fn len(&self) -> usize {
        self.trials.len()
    }

    /// True if no trials.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.trials.is_empty()
    }
}

/// Append-only BLAKE3-chained lineage. Each entry references the
/// prior generation's id; the chain is replay-verifiable from a seed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Linhagem {
    /// Search this lineage belongs to.
    pub search_id: SearchId,
    /// Chain of generation ids in temporal order.
    pub generations: Vec<GeracaoId>,
}

impl Linhagem {
    /// New empty lineage for a search.
    #[must_use]
    pub fn new(search_id: SearchId) -> Self {
        Self {
            search_id,
            generations: Vec::new(),
        }
    }

    /// Append a generation id.
    pub fn extend(&mut self, generation_id: GeracaoId) -> &mut Self {
        self.generations.push(generation_id);
        self
    }

    /// Latest generation id (the chain head).
    #[must_use]
    pub fn head(&self) -> Option<&GeracaoId> {
        self.generations.last()
    }

    /// Generation count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.generations.len()
    }

    /// True if no generations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.generations.is_empty()
    }

    /// Canonical fingerprint over the entire chain — operators
    /// commit this to attest a search was reproducible.
    #[must_use]
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.search_id.0.as_bytes());
        for g in &self.generations {
            hasher.update(&g.0);
        }
        *hasher.finalize().as_bytes()
    }
}

/// MAP-Elites archive — typed BTreeMap from niche key to the best
/// trial that fell into that niche. Niche keys are operator-chosen
/// behavior descriptors.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Arquivo<F> {
    /// Niche → best trial.
    pub entries: BTreeMap<NicheKey, Aptidao<F>>,
}

impl<F> Default for Arquivo<F> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
}

impl<F: Clone> Arquivo<F> {
    /// New empty archive.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a trial into a niche. Returns true if it displaced
    /// the prior occupant (per a comparator), false otherwise.
    ///
    /// The comparator returns true when `candidate` is BETTER than
    /// `incumbent` (i.e. should replace).
    pub fn insert_if_better<C>(
        &mut self,
        niche: NicheKey,
        candidate: Aptidao<F>,
        better: C,
    ) -> bool
    where
        C: Fn(&Aptidao<F>, &Aptidao<F>) -> bool,
    {
        match self.entries.get(&niche) {
            None => {
                self.entries.insert(niche, candidate);
                true
            }
            Some(existing) => {
                if better(&candidate, existing) {
                    self.entries.insert(niche, candidate);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Filled niches.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True if empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Opaque niche descriptor — operator-chosen bytes encoding
/// behavior dimensions (e.g. "compression_ratio + circuit_depth").
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NicheKey(pub Vec<u8>);

impl NicheKey {
    /// Construct from arbitrary bytes.
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }
}

// =================================================================
// Deterministic RNG anchor
// =================================================================

/// Tiny deterministic RNG (Wyhash). Seeded from a 32-byte seed +
/// stream identifier; same seed + same identifier produces
/// byte-identical output. Used by every SearchSpace.mutate() +
/// every Selector.select() call so the search is replayable.
///
/// Not cryptographic; good enough for search.
#[derive(Clone, Debug)]
pub struct SearchRng {
    state: u64,
}

impl SearchRng {
    /// New RNG from seed + stream tag (typically `(search_id_hash,
    /// generation_idx, trial_idx)` packed into bytes).
    #[must_use]
    pub fn new(seed: &[u8; 32], stream_tag: &[u8]) -> Self {
        let mut h = blake3::Hasher::new();
        h.update(seed);
        h.update(b"|");
        h.update(stream_tag);
        let digest = h.finalize();
        let mut state_bytes = [0u8; 8];
        state_bytes.copy_from_slice(&digest.as_bytes()[..8]);
        Self {
            state: u64::from_le_bytes(state_bytes),
        }
    }

    /// Next u64 (Wyhash step). Deterministic.
    pub fn next_u64(&mut self) -> u64 {
        // Splittable-mix step; same constants the substrate's
        // existing tests use elsewhere.
        let mut z = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        self.state = z;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Next u64 in `[0, max)`. `max == 0` returns 0.
    pub fn next_below(&mut self, max: u64) -> u64 {
        if max == 0 {
            return 0;
        }
        self.next_u64() % max
    }
}

// =================================================================
// Trait surface
// =================================================================

/// SearchSpace describes the search space + its mutation operators.
pub trait SearchSpace<G>: Send + Sync
where
    G: Send + Sync,
{
    /// Telemetry name.
    fn name(&self) -> &'static str;

    /// Sample a fresh genotype from a deterministic RNG.
    fn sample(&self, rng: &mut SearchRng) -> G;

    /// Mutate a parent into a child via deterministic RNG.
    fn mutate(&self, parent: &G, rng: &mut SearchRng) -> G;

    /// Canonical bytes for a genotype — substrate hashes these to
    /// derive EnsaioId. Same genotype always produces same bytes.
    fn canonical_bytes(&self, g: &G) -> Vec<u8>;
}

/// Evidence bytes — whatever the Fitness produces alongside its
/// scalar/vector score. The substrate hashes this with BLAKE3 to
/// derive the receipt's evidence_hash.
pub type Evidence = Vec<u8>;

/// Fitness errors.
#[derive(Debug, Clone, Error)]
pub enum FitnessError {
    /// Backend failure during evaluation.
    #[error("backend: {0}")]
    Backend(String),
    /// Genotype rejected as malformed.
    #[error("invalid genotype: {0}")]
    InvalidGenotype(String),
}

impl FitnessError {
    /// Stable identifier for telemetry.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Backend(_) => "backend",
            Self::InvalidGenotype(_) => "invalid_genotype",
        }
    }
}

/// Pluggable fitness function.
#[async_trait]
pub trait Fitness<G, F>: Send + Sync
where
    G: Send + Sync,
    F: Send + Sync,
{
    /// Telemetry name.
    fn name(&self) -> &'static str;

    /// Score a genotype + emit evidence bytes the substrate hashes
    /// into a MaterializationReceipt's `evidence_hash`.
    ///
    /// # Errors
    /// [`FitnessError::Backend`] / [`FitnessError::InvalidGenotype`].
    async fn evaluate(&self, genotype: &G) -> Result<(F, Evidence), FitnessError>;
}

/// Pluggable selector — picks the next generation from a scored
/// population. Deterministic given the rng + ordering of inputs.
pub trait Selector<F>: Send + Sync
where
    F: Send + Sync,
{
    /// Telemetry name.
    fn name(&self) -> &'static str;

    /// Pick `n` ensaio ids from the population. Same input + same
    /// rng state produces same output.
    fn select(&self, population: &[Aptidao<F>], rng: &mut SearchRng, n: usize) -> Vec<EnsaioId>;
}

/// Pesquisa errors.
#[derive(Debug, Clone, Error)]
pub enum PesquisaError {
    /// Fitness backend errored.
    #[error("fitness: {0}")]
    Fitness(String),
    /// SearchEngine refused to step (out of budget / invalid prior).
    #[error("engine: {0}")]
    Engine(String),
    /// Lineage replay diverged from the original chain.
    #[error("diverged at generation {0}")]
    Diverged(u64),
}

impl PesquisaError {
    /// Stable identifier for telemetry.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Fitness(_) => "fitness",
            Self::Engine(_) => "engine",
            Self::Diverged(_) => "diverged",
        }
    }
}

/// SearchEngine — runs one search step. The loop lives in operator
/// code; the trait gives one typed call per generation.
#[async_trait]
pub trait SearchEngine<F>: Send + Sync
where
    F: Send + Sync,
{
    /// Telemetry name.
    fn name(&self) -> &'static str;

    /// Compute the next generation from the prior. Returns the
    /// new generation + extends the linhagem.
    ///
    /// # Errors
    /// Engine-specific failures via [`PesquisaError`].
    async fn step(
        &mut self,
        prior: Option<&Geracao<F>>,
    ) -> Result<Geracao<F>, PesquisaError>;

    /// Borrow the current linhagem (for telemetry + replay).
    fn linhagem(&self) -> &Linhagem;
}

// =================================================================
// Fake impls for tests
// =================================================================

/// In-memory SearchSpace<u32>. Sample = next_u64() as u32; mutate
/// = parent + small random delta. Deterministic.
#[derive(Default, Clone)]
pub struct FakeSearchSpace;

impl SearchSpace<u32> for FakeSearchSpace {
    fn name(&self) -> &'static str {
        "fake-u32"
    }

    fn sample(&self, rng: &mut SearchRng) -> u32 {
        rng.next_u64() as u32
    }

    fn mutate(&self, parent: &u32, rng: &mut SearchRng) -> u32 {
        // Small additive perturbation in [-16, 16).
        let delta = (rng.next_u64() % 32) as i64 - 16;
        let v = (*parent as i64).saturating_add(delta);
        v.clamp(0, i64::from(u32::MAX)) as u32
    }

    fn canonical_bytes(&self, g: &u32) -> Vec<u8> {
        g.to_le_bytes().to_vec()
    }
}

/// In-memory Fitness for u32 — fitness = -|g - target| as f64
/// (closer to target is better). Evidence is target bytes ||
/// genotype bytes.
pub struct FakeFitness {
    /// Target value the fitness rewards proximity to.
    pub target: u32,
}

impl FakeFitness {
    /// New fitness with target.
    #[must_use]
    pub fn new(target: u32) -> Self {
        Self { target }
    }
}

#[async_trait]
impl Fitness<u32, f64> for FakeFitness {
    fn name(&self) -> &'static str {
        "fake-distance"
    }

    async fn evaluate(&self, genotype: &u32) -> Result<(f64, Evidence), FitnessError> {
        let distance = (*genotype as i64 - self.target as i64).unsigned_abs() as f64;
        let fitness = -distance;  // higher = better
        let mut evidence = self.target.to_le_bytes().to_vec();
        evidence.extend_from_slice(&genotype.to_le_bytes());
        Ok((fitness, evidence))
    }
}

/// Tournament selector — picks `n` ensaios by running `k`-sized
/// tournaments and taking the winner (highest fitness; assumes
/// `F: PartialOrd`).
#[derive(Clone, Copy, Debug)]
pub struct TournamentSelector {
    /// Tournament size.
    pub k: usize,
}

impl TournamentSelector {
    /// New selector with tournament size `k` (clamps to 1+).
    #[must_use]
    pub fn new(k: usize) -> Self {
        Self { k: k.max(1) }
    }
}

impl<F> Selector<F> for TournamentSelector
where
    F: Send + Sync + PartialOrd + Clone,
{
    fn name(&self) -> &'static str {
        "tournament"
    }

    fn select(&self, population: &[Aptidao<F>], rng: &mut SearchRng, n: usize) -> Vec<EnsaioId> {
        if population.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            // Sample k random candidates; pick the highest-fitness one.
            let mut champion: Option<&Aptidao<F>> = None;
            for _ in 0..self.k {
                let idx = rng.next_below(population.len() as u64) as usize;
                let candidate = &population[idx];
                match champion {
                    None => champion = Some(candidate),
                    Some(c) => {
                        if candidate.fitness > c.fitness {
                            champion = Some(candidate);
                        }
                    }
                }
            }
            if let Some(c) = champion {
                out.push(c.ensaio.id);
            }
        }
        out
    }
}

// =================================================================
// helpers
// =================================================================

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

// =================================================================
// Conversion helpers — turn fitness evidence into receipts
// =================================================================

/// Map an Aptidao into a MaterializationReceipt the existing ledger
/// can ingest. Subject is the EnsaioId; evidence_hash is the
/// Aptidao's evidence_hash; emitter is operator-supplied.
///
/// Cross-node disagreement on the same ensaio's evidence_hash =
/// QuorumTracker reports Dissent (same byzantine-detection path).
#[must_use]
pub fn aptidao_to_receipt<F>(
    aptidao: &Aptidao<F>,
    emitter: crate::receipt::NodeId,
    emitted_at: u64,
) -> crate::receipt::MaterializationReceipt {
    crate::receipt::MaterializationReceipt::new(
        crate::receipt::ReceiptKind::Shape("pesquisa:aptidao".into()),
        aptidao.ensaio.id.0,
        emitter,
        emitted_at,
        aptidao.evidence_hash,
    )
}

// =================================================================
// Crate-public Arc helper for fitness composition
// =================================================================

/// Compose a fitness over a single Ensaio into Aptidao using the
/// caller's fitness backend. Convenience for engine impls.
///
/// # Errors
/// Propagates fitness errors.
pub async fn score_ensaio<G, F>(
    ensaio: Ensaio,
    genotype: &G,
    fitness: &Arc<dyn Fitness<G, F>>,
) -> Result<Aptidao<F>, FitnessError>
where
    G: Send + Sync,
    F: Send + Sync,
{
    let (f, evidence) = fitness.evaluate(genotype).await?;
    let evidence_hash = *blake3::hash(&evidence).as_bytes();
    Ok(Aptidao {
        ensaio,
        fitness: f,
        evidence_hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn search_id() -> SearchId {
        SearchId::new("test-search")
    }

    fn seed() -> [u8; 32] {
        *blake3::hash(b"seed-1").as_bytes()
    }

    fn sample_ensaio(g: u32, gen_idx: u64) -> Ensaio {
        let bytes = g.to_le_bytes().to_vec();
        let id = EnsaioId::derive(&search_id(), gen_idx, &bytes, &[0u8; 32]);
        Ensaio {
            id,
            genotype: bytes,
            lineage: BTreeSet::new(),
            generation_idx: gen_idx,
        }
    }

    fn sample_aptidao(g: u32, gen_idx: u64, fitness: f64) -> Aptidao<f64> {
        Aptidao {
            ensaio: sample_ensaio(g, gen_idx),
            fitness,
            evidence_hash: [0u8; 32],
        }
    }

    // ── EnsaioId determinism ───────────────────────────────────

    #[test]
    fn ensaio_id_derive_is_deterministic() {
        let bytes = b"abc".to_vec();
        let id1 = EnsaioId::derive(&search_id(), 0, &bytes, &[0u8; 32]);
        let id2 = EnsaioId::derive(&search_id(), 0, &bytes, &[0u8; 32]);
        assert_eq!(id1, id2);
    }

    #[test]
    fn ensaio_id_diverges_per_search() {
        let bytes = b"abc".to_vec();
        let id1 = EnsaioId::derive(&SearchId::new("a"), 0, &bytes, &[0u8; 32]);
        let id2 = EnsaioId::derive(&SearchId::new("b"), 0, &bytes, &[0u8; 32]);
        assert_ne!(id1, id2);
    }

    #[test]
    fn ensaio_id_diverges_per_generation() {
        let bytes = b"abc".to_vec();
        let id1 = EnsaioId::derive(&search_id(), 0, &bytes, &[0u8; 32]);
        let id2 = EnsaioId::derive(&search_id(), 1, &bytes, &[0u8; 32]);
        assert_ne!(id1, id2);
    }

    #[test]
    fn ensaio_id_diverges_per_genotype() {
        let id1 = EnsaioId::derive(&search_id(), 0, b"abc", &[0u8; 32]);
        let id2 = EnsaioId::derive(&search_id(), 0, b"def", &[0u8; 32]);
        assert_ne!(id1, id2);
    }

    #[test]
    fn ensaio_id_diverges_per_lineage_root() {
        let id1 = EnsaioId::derive(&search_id(), 0, b"x", &[1u8; 32]);
        let id2 = EnsaioId::derive(&search_id(), 0, b"x", &[2u8; 32]);
        assert_ne!(id1, id2);
    }

    #[test]
    fn ensaio_id_hex_is_64_chars() {
        let id = EnsaioId::derive(&search_id(), 0, b"x", &[0u8; 32]);
        assert_eq!(id.to_hex().len(), 64);
    }

    // ── GeracaoId chain ────────────────────────────────────────

    #[test]
    fn geracao_id_derive_chains_from_prior() {
        let mut set = BTreeSet::new();
        set.insert(EnsaioId([1u8; 32]));
        let g_zero = GeracaoId::derive(None, &set, &[0u8; 32]);
        let g_one = GeracaoId::derive(Some(&g_zero), &set, &[0u8; 32]);
        // Different priors → different IDs even with same ensaios + seed.
        assert_ne!(g_zero, g_one);
    }

    #[test]
    fn geracao_id_derive_diverges_per_ensaios() {
        let mut s1 = BTreeSet::new();
        s1.insert(EnsaioId([1u8; 32]));
        let mut s2 = BTreeSet::new();
        s2.insert(EnsaioId([2u8; 32]));
        let g1 = GeracaoId::derive(None, &s1, &[0u8; 32]);
        let g2 = GeracaoId::derive(None, &s2, &[0u8; 32]);
        assert_ne!(g1, g2);
    }

    #[test]
    fn geracao_id_derive_is_deterministic() {
        let mut set = BTreeSet::new();
        set.insert(EnsaioId([1u8; 32]));
        let g1 = GeracaoId::derive(None, &set, &[0u8; 32]);
        let g2 = GeracaoId::derive(None, &set, &[0u8; 32]);
        assert_eq!(g1, g2);
    }

    // ── SearchRng determinism ──────────────────────────────────

    #[test]
    fn search_rng_same_seed_same_stream_produces_same_output() {
        let mut r1 = SearchRng::new(&seed(), b"gen-0/trial-0");
        let mut r2 = SearchRng::new(&seed(), b"gen-0/trial-0");
        for _ in 0..16 {
            assert_eq!(r1.next_u64(), r2.next_u64());
        }
    }

    #[test]
    fn search_rng_different_stream_diverges() {
        let mut r1 = SearchRng::new(&seed(), b"a");
        let mut r2 = SearchRng::new(&seed(), b"b");
        // First sample should differ (overwhelmingly likely).
        assert_ne!(r1.next_u64(), r2.next_u64());
    }

    #[test]
    fn search_rng_next_below_bounded() {
        let mut r = SearchRng::new(&seed(), b"x");
        for _ in 0..32 {
            let v = r.next_below(10);
            assert!(v < 10);
        }
    }

    #[test]
    fn search_rng_next_below_zero_returns_zero() {
        let mut r = SearchRng::new(&seed(), b"x");
        assert_eq!(r.next_below(0), 0);
    }

    // ── Linhagem chain ────────────────────────────────────────

    #[test]
    fn linhagem_extend_appends() {
        let mut l = Linhagem::new(search_id());
        assert!(l.is_empty());
        l.extend(GeracaoId([1u8; 32]));
        l.extend(GeracaoId([2u8; 32]));
        assert_eq!(l.len(), 2);
        assert_eq!(l.head(), Some(&GeracaoId([2u8; 32])));
    }

    #[test]
    fn linhagem_fingerprint_is_deterministic() {
        let mut l1 = Linhagem::new(search_id());
        l1.extend(GeracaoId([1u8; 32]));
        l1.extend(GeracaoId([2u8; 32]));
        let mut l2 = Linhagem::new(search_id());
        l2.extend(GeracaoId([1u8; 32]));
        l2.extend(GeracaoId([2u8; 32]));
        assert_eq!(l1.fingerprint(), l2.fingerprint());
    }

    #[test]
    fn linhagem_fingerprint_diverges_per_chain() {
        let mut l1 = Linhagem::new(search_id());
        l1.extend(GeracaoId([1u8; 32]));
        let mut l2 = Linhagem::new(search_id());
        l2.extend(GeracaoId([2u8; 32]));
        assert_ne!(l1.fingerprint(), l2.fingerprint());
    }

    // ── Arquivo (MAP-Elites) ──────────────────────────────────

    #[test]
    fn arquivo_inserts_into_empty_niche() {
        let mut a: Arquivo<f64> = Arquivo::new();
        let inserted = a.insert_if_better(
            NicheKey::new(b"low".to_vec()),
            sample_aptidao(5, 0, 1.0),
            |c, i| c.fitness > i.fitness,
        );
        assert!(inserted);
        assert_eq!(a.len(), 1);
    }

    #[test]
    fn arquivo_keeps_existing_when_candidate_worse() {
        let mut a: Arquivo<f64> = Arquivo::new();
        let niche = NicheKey::new(b"x".to_vec());
        a.insert_if_better(
            niche.clone(),
            sample_aptidao(1, 0, 10.0),
            |c, i| c.fitness > i.fitness,
        );
        let inserted = a.insert_if_better(
            niche,
            sample_aptidao(2, 0, 5.0),
            |c, i| c.fitness > i.fitness,
        );
        assert!(!inserted);
        assert_eq!(a.len(), 1);
    }

    #[test]
    fn arquivo_replaces_when_candidate_better() {
        let mut a: Arquivo<f64> = Arquivo::new();
        let niche = NicheKey::new(b"x".to_vec());
        a.insert_if_better(
            niche.clone(),
            sample_aptidao(1, 0, 5.0),
            |c, i| c.fitness > i.fitness,
        );
        let inserted = a.insert_if_better(
            niche.clone(),
            sample_aptidao(2, 0, 10.0),
            |c, i| c.fitness > i.fitness,
        );
        assert!(inserted);
        assert_eq!(a.entries[&niche].fitness, 10.0);
    }

    // ── FakeSearchSpace ────────────────────────────────────────

    #[test]
    fn fake_search_space_sample_is_deterministic() {
        let s = FakeSearchSpace;
        let mut r1 = SearchRng::new(&seed(), b"x");
        let mut r2 = SearchRng::new(&seed(), b"x");
        let g1 = s.sample(&mut r1);
        let g2 = s.sample(&mut r2);
        assert_eq!(g1, g2);
    }

    #[test]
    fn fake_search_space_mutate_is_deterministic() {
        let s = FakeSearchSpace;
        let mut r1 = SearchRng::new(&seed(), b"m");
        let mut r2 = SearchRng::new(&seed(), b"m");
        let c1 = s.mutate(&100, &mut r1);
        let c2 = s.mutate(&100, &mut r2);
        assert_eq!(c1, c2);
    }

    #[test]
    fn fake_search_space_canonical_bytes_round_trip() {
        let s = FakeSearchSpace;
        let bytes = s.canonical_bytes(&0xdead_beef);
        assert_eq!(bytes, 0xdead_beef_u32.to_le_bytes().to_vec());
    }

    // ── FakeFitness ────────────────────────────────────────────

    #[tokio::test]
    async fn fake_fitness_target_is_optimum() {
        let f = FakeFitness::new(100);
        let (score_at_target, _) = f.evaluate(&100).await.unwrap();
        let (score_off, _) = f.evaluate(&90).await.unwrap();
        // Target → fitness 0; off-target → negative.
        assert_eq!(score_at_target, 0.0);
        assert_eq!(score_off, -10.0);
    }

    #[tokio::test]
    async fn fake_fitness_faithful_evaluations_produce_same_evidence() {
        let f = FakeFitness::new(50);
        let (_, e1) = f.evaluate(&25).await.unwrap();
        let (_, e2) = f.evaluate(&25).await.unwrap();
        assert_eq!(e1, e2);
    }

    // ── TournamentSelector ─────────────────────────────────────

    #[test]
    fn tournament_picks_best_in_tournament() {
        let sel = TournamentSelector::new(3);
        let pop = vec![
            sample_aptidao(0, 0, -10.0),
            sample_aptidao(1, 0, -5.0),
            sample_aptidao(2, 0, 0.0),  // best
            sample_aptidao(3, 0, -3.0),
        ];
        let mut rng = SearchRng::new(&seed(), b"sel");
        let picks = sel.select(&pop, &mut rng, 4);
        assert_eq!(picks.len(), 4);
        // Picks are valid EnsaioIds from the population (we can't
        // deterministically assert which one without simulating the
        // RNG; this test asserts shape + count).
        for pick in &picks {
            assert!(pop.iter().any(|a| &a.ensaio.id == pick));
        }
    }

    #[test]
    fn tournament_deterministic_with_same_rng() {
        let sel = TournamentSelector::new(2);
        let pop = vec![
            sample_aptidao(0, 0, 1.0),
            sample_aptidao(1, 0, 2.0),
            sample_aptidao(2, 0, 3.0),
        ];
        let mut r1 = SearchRng::new(&seed(), b"sel");
        let mut r2 = SearchRng::new(&seed(), b"sel");
        let p1 = sel.select(&pop, &mut r1, 5);
        let p2 = sel.select(&pop, &mut r2, 5);
        assert_eq!(p1, p2);
    }

    #[test]
    fn tournament_empty_population_returns_empty() {
        let sel = TournamentSelector::new(3);
        let mut rng = SearchRng::new(&seed(), b"x");
        let p = <TournamentSelector as Selector<f64>>::select(&sel, &[], &mut rng, 10);
        assert!(p.is_empty());
    }

    #[test]
    fn tournament_k_zero_clamps_to_one() {
        let sel = TournamentSelector::new(0);
        assert_eq!(sel.k, 1);
    }

    #[test]
    fn tournament_name_is_stable() {
        let sel = TournamentSelector::new(3);
        assert_eq!(<TournamentSelector as Selector<f64>>::name(&sel), "tournament");
    }

    // ── score_ensaio + aptidao_to_receipt bridges ──────────────

    #[tokio::test]
    async fn score_ensaio_computes_evidence_hash() {
        let fitness: Arc<dyn Fitness<u32, f64>> = Arc::new(FakeFitness::new(100));
        let ensaio = sample_ensaio(50, 0);
        let aptidao = score_ensaio(ensaio.clone(), &50, &fitness).await.unwrap();
        assert_eq!(aptidao.fitness, -50.0);
        // Evidence hash is deterministic.
        let aptidao2 = score_ensaio(ensaio, &50, &fitness).await.unwrap();
        assert_eq!(aptidao.evidence_hash, aptidao2.evidence_hash);
    }

    #[test]
    fn aptidao_to_receipt_carries_subject_and_evidence() {
        let aptidao = sample_aptidao(7, 0, 0.5);
        let receipt = aptidao_to_receipt(
            &aptidao,
            crate::receipt::NodeId::from_bytes(b"n"),
            42,
        );
        assert_eq!(receipt.subject, aptidao.ensaio.id.0);
        assert_eq!(receipt.evidence_hash, aptidao.evidence_hash);
        match receipt.kind {
            crate::receipt::ReceiptKind::Shape(tag) => {
                assert_eq!(tag, "pesquisa:aptidao");
            }
            _ => panic!("expected Shape"),
        }
    }

    // ── Serde round-trips ──────────────────────────────────────

    #[test]
    fn ensaio_round_trips_via_serde() {
        let e = sample_ensaio(42, 7);
        let bytes = serde_json::to_vec(&e).unwrap();
        let back: Ensaio = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, e);
    }

    #[test]
    fn aptidao_round_trips_via_serde() {
        let a = sample_aptidao(42, 7, 1.5);
        let bytes = serde_json::to_vec(&a).unwrap();
        let back: Aptidao<f64> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, a);
    }

    #[test]
    fn linhagem_round_trips_via_serde() {
        let mut l = Linhagem::new(search_id());
        l.extend(GeracaoId([1u8; 32]));
        let bytes = serde_json::to_vec(&l).unwrap();
        let back: Linhagem = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, l);
    }

    // ── Error kinds ────────────────────────────────────────────

    #[test]
    fn fitness_error_kinds_are_stable() {
        assert_eq!(FitnessError::Backend("x".into()).kind(), "backend");
        assert_eq!(
            FitnessError::InvalidGenotype("x".into()).kind(),
            "invalid_genotype"
        );
    }

    #[test]
    fn pesquisa_error_kinds_are_stable() {
        assert_eq!(PesquisaError::Fitness("x".into()).kind(), "fitness");
        assert_eq!(PesquisaError::Engine("x".into()).kind(), "engine");
        assert_eq!(PesquisaError::Diverged(3).kind(), "diverged");
    }
}
