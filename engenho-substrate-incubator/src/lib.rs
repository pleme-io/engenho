//! # engenho-substrate-incubator
//!
//! Substrate modules that no shipped engenho crate references. They moved
//! here out of `engenho-substrate` so the shipped closure stops compiling
//! them; they were not deleted.
//!
//! | Module | What it is |
//! |---|---|
//! | [`pesquisa`] | typed search: `SearchEngine`, `Ensaio`, `Linhagem`, fitness and selection |
//! | [`orcamento`] | `Budget`, a spend ceiling over a clock |
//! | [`compose_ir`] | a docker-compose-shaped IR over `CommandRunner` |
//! | [`oci_renderer`] | an OCI image `ShapeRenderer` over `CommandRunner` |
//! | [`command_runner`] | `CommandRunner` + `FakeCommandRunner` |
//! | [`fake_shell`] | the `fake_backend_shell!` harness macro |
//! | [`disposable`] | `Disposable` / `Transient` lifecycles over a ledger |
//!
//! This crate depends on `engenho-substrate`; the reverse edge would be a
//! cycle, which Cargo refuses. A module graduates by moving back into the
//! leaf in the same commit that gives it a shipped consumer.

#![warn(clippy::pedantic)]
#![warn(missing_docs)]
#![allow(clippy::module_name_repetitions)]

pub mod command_runner;
pub mod compose_ir;
pub mod disposable;
pub mod fake_shell;
pub mod oci_renderer;
pub mod orcamento;
pub mod pesquisa;

pub use command_runner::{
    CommandError, CommandRequest, CommandResponse, CommandRunner, FakeCommandRunner,
};
pub use compose_ir::{ComposeError, ComposeHealthcheck, ComposeIr, ComposeService, ComposeStack};
pub use disposable::{Disposable, DisposableError, Transient};
pub use oci_renderer::{OciDestReader, OciDestRef, OciImageRenderer, OciSourceRef};
pub use orcamento::{Budget, BudgetError, BudgetSnapshot};
pub use pesquisa::{
    Aptidao, Arquivo, Ensaio, EnsaioId, Evidence, FakeFitness, FakeSearchSpace, Fitness,
    FitnessError, Geracao, GeracaoId, Linhagem, NicheKey, PesquisaError, SearchEngine, SearchId,
    SearchRng, SearchSpace, Selector, TournamentSelector, aptidao_to_receipt, score_ensaio,
};
