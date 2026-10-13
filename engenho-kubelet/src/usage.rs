//! Resource usage as `metrics.k8s.io` reports it: CPU as a rate in
//! nanocores over a window, memory as the working set in bytes.
//!
//! A runtime reports CUMULATIVE readings ([`Cumulative`]); a rate needs two,
//! so [`Sampler`] keeps the last reading per key and turns the next one into
//! a [`Usage`]. Where nothing can be read (no cgroup leaf, macOS, a backend
//! with no stats API) the answer is [`Reading::Unmeasurable`] with the
//! reason: never a zero, which would read as an idle container.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

/// Cumulative readings at one instant.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Cumulative {
    /// CPU time consumed since the container (or node) started, in ns.
    pub cpu_ns: u64,
    /// Memory working set right now, in bytes.
    pub memory_working_set_bytes: u64,
}

/// What a runtime could say about one container (or the node).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reading {
    /// Measured.
    Measured(Cumulative),
    /// Not measurable here, and why.
    Unmeasurable(String),
}

/// Usage over a window, as `PodMetrics`/`NodeMetrics` carry it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Usage {
    /// Average CPU over the window, in nanocores (1e-9 CPU).
    pub cpu_nanocores: u64,
    /// Working set at the window's end, in bytes.
    pub memory_working_set_bytes: u64,
    /// When the window ended.
    pub timestamp: SystemTime,
    /// The window's length.
    pub window: Duration,
}

/// The value after `key ` on its own line of a `key value` file
/// (`cpu.stat`, `memory.stat`, `/proc/meminfo` without the unit).
#[must_use]
pub fn keyed_u64(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let k = fields.next()?.trim_end_matches(':');
        (k == key).then(|| fields.next()?.parse().ok())?
    })
}

/// Busy CPU time from `/proc/stat`'s aggregate `cpu` line, in ns:
/// everything but idle and iowait, at `USER_HZ` = 100.
#[must_use]
pub fn proc_stat_busy_ns(text: &str) -> Option<u64> {
    let line = text.lines().find(|l| l.starts_with("cpu "))?;
    let ticks: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .map(str::parse)
        .collect::<Result<_, _>>()
        .ok()?;
    let total: u64 = ticks.iter().take(8).sum();
    let idle = ticks.get(3)? + ticks.get(4).copied().unwrap_or(0);
    Some(total.saturating_sub(idle).saturating_mul(10_000_000))
}

/// The node's working set from `/proc/meminfo`: `MemTotal − MemAvailable`.
#[must_use]
pub fn meminfo_working_set_bytes(text: &str) -> Option<u64> {
    let kib = |k| keyed_u64(text, k);
    Some(kib("MemTotal")?.saturating_sub(kib("MemAvailable")?) * 1024)
}

/// The node's own cumulative reading. Linux reads `/proc`; elsewhere the
/// node is unmeasurable.
#[must_use]
pub fn node_reading() -> Reading {
    if !cfg!(target_os = "linux") {
        return Reading::Unmeasurable(
            "this node is not Linux: node CPU and memory are not read here".into(),
        );
    }
    let read = |p: &str| std::fs::read_to_string(p).map_err(|e| format!("{p}: {e}"));
    let measured = read("/proc/stat").and_then(|stat| {
        let mem = read("/proc/meminfo")?;
        Ok(Cumulative {
            cpu_ns: proc_stat_busy_ns(&stat).ok_or("/proc/stat has no cpu line")?,
            memory_working_set_bytes: meminfo_working_set_bytes(&mem)
                .ok_or("/proc/meminfo lacks MemTotal or MemAvailable")?,
        })
    });
    match measured {
        Ok(c) => Reading::Measured(c),
        Err(why) => Reading::Unmeasurable(why),
    }
}

/// The last cumulative reading per key, turned into rates.
#[derive(Debug, Default)]
pub struct Sampler {
    last: Mutex<HashMap<String, (Instant, Cumulative)>>,
}

impl Sampler {
    /// Record `now` for `key`; the [`Usage`] since the previous reading, or
    /// `None` when there is none yet, or the counter went backwards (a
    /// restarted container), or no time has passed.
    pub fn observe(
        &self,
        key: &str,
        at: Instant,
        wall: SystemTime,
        now: Cumulative,
    ) -> Option<Usage> {
        let mut last = self.last.lock().ok()?;
        let prev = last.insert(key.to_owned(), (at, now));
        let (then, before) = prev?;
        let window = at.checked_duration_since(then)?;
        let used = now.cpu_ns.checked_sub(before.cpu_ns)?;
        let nanos = window.as_nanos();
        if nanos == 0 {
            return None;
        }
        let cores = u128::from(used) * 1_000_000_000 / nanos;
        Some(Usage {
            cpu_nanocores: u64::try_from(cores).unwrap_or(u64::MAX),
            memory_working_set_bytes: now.memory_working_set_bytes,
            timestamp: wall,
            window,
        })
    }

    /// Whether `key` has a reading a rate can be taken from.
    #[must_use]
    pub fn has(&self, key: &str) -> bool {
        self.last.lock().is_ok_and(|l| l.contains_key(key))
    }

    /// Forget every key not in `live`.
    pub fn retain(&self, live: &std::collections::HashSet<String>) {
        if let Ok(mut last) = self.last.lock() {
            last.retain(|k, _| live.contains(k));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cgroup_and_meminfo_fields_are_read_by_key() {
        let stat = "usage_usec 2500\nuser_usec 2000\nsystem_usec 500\n";
        assert_eq!(keyed_u64(stat, "usage_usec"), Some(2500));
        assert_eq!(keyed_u64(stat, "nope"), None);
        let mem = "MemTotal:       16000 kB\nMemFree:  1 kB\nMemAvailable:   6000 kB\n";
        assert_eq!(meminfo_working_set_bytes(mem), Some(10_000 * 1024));
    }

    #[test]
    fn busy_cpu_excludes_idle_and_iowait() {
        let stat = "cpu  100 0 50 1000 25 0 0 0 0 0\ncpu0 1 1 1 1 1 1 1 1\n";
        assert_eq!(proc_stat_busy_ns(stat), Some(150 * 10_000_000));
    }

    #[test]
    fn a_rate_needs_two_readings_and_a_monotonic_counter() {
        let s = Sampler::default();
        let t0 = Instant::now();
        let wall = SystemTime::now();
        let c = |cpu, mem| Cumulative {
            cpu_ns: cpu,
            memory_working_set_bytes: mem,
        };
        assert_eq!(s.observe("a", t0, wall, c(0, 10)), None);
        let u = s
            .observe("a", t0 + Duration::from_secs(2), wall, c(1_000_000_000, 20))
            .expect("two readings");
        assert_eq!(u.cpu_nanocores, 500_000_000);
        assert_eq!(u.memory_working_set_bytes, 20);
        assert_eq!(u.window, Duration::from_secs(2));
        assert_eq!(
            s.observe("a", t0 + Duration::from_secs(3), wall, c(5, 20)),
            None,
            "a counter that went backwards is a restart, not negative CPU"
        );
    }
}
