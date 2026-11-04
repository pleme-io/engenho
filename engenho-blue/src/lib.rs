//! A blue program embedded in an engenho crate, running on its own thread.
//!
//! A loaded blue program is not `Send`, so calls reach it over a channel. Each
//! program is loaded under an empty posture: it decides from the facts it is
//! handed and reaches nothing, and Rust applies its verdict.

use blue_lang_host::{Embedded, Entry, Program};
use tokio::sync::oneshot;

struct Job {
    word: &'static str,
    facts: String,
    reply: oneshot::Sender<Result<String, String>>,
}

/// A handle on one embedded program.
#[derive(Clone)]
pub struct BlueWorker {
    name: &'static str,
    jobs: std::sync::mpsc::Sender<Job>,
}

impl BlueWorker {
    /// Load `files` under an empty posture and start the program's thread.
    ///
    /// # Errors
    /// The load error, as text, when the program does not load.
    pub fn start(
        name: &'static str,
        entry: &'static str,
        files: &'static [(&'static str, &'static str)],
    ) -> Result<Self, String> {
        let (jobs, inbox) = std::sync::mpsc::channel::<Job>();
        let (ready, loaded) = std::sync::mpsc::channel::<Result<(), String>>();
        std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                let embedded = Embedded { name, files };
                let mut host = ();
                let loaded = Program::builder(Entry::anonymous(entry))
                    .loader(&embedded)
                    .posture(&[])
                    .load(&mut host);
                let mut program = match loaded {
                    Ok(p) => p,
                    Err(e) => {
                        let _ = ready.send(Err(e.to_string()));
                        return;
                    }
                };
                let _ = ready.send(Ok(()));
                for job in inbox {
                    let answer = program
                        .call::<String>(job.word, (job.facts,), &mut host)
                        .map_err(|e| e.to_string());
                    let _ = job.reply.send(answer);
                }
            })
            .map_err(|e| e.to_string())?;
        loaded
            .recv()
            .map_err(|_| format!("the {name} thread ended before it loaded"))??;
        Ok(Self { name, jobs })
    }

    /// Ask `word` about `facts`, and read its answer as `T`.
    ///
    /// # Errors
    /// The program's error, or an answer that does not read.
    pub async fn ask<T: for<'de> serde::Deserialize<'de>>(
        &self,
        word: &'static str,
        facts: String,
    ) -> Result<T, String> {
        let (reply, answer) = oneshot::channel();
        let stopped = || format!("the {} thread stopped", self.name);
        self.jobs
            .send(Job { word, facts, reply })
            .map_err(|_| stopped())?;
        let text = answer.await.map_err(|_| stopped())??;
        serde_json::from_str(&text).map_err(|e| format!("{} answered {text}: {e}", self.name))
    }
}
