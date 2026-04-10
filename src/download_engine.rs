//! Download engine — shared NNTP worker pool that services all active jobs.
//!
//! Architecture:
//! - A single long-lived [`WorkerPool`] is owned by [`crate::queue_manager::QueueManager`].
//! - For each enabled server, exactly `server.connections` workers are
//!   spawned and live as long as the server stays enabled. When the server
//!   list or per-server connection limit changes, the pool reconciles.
//! - Jobs register a [`JobContext`] and push their work items into a
//!   [`SharedWorkQueue`]. Workers pop items tagged with `job_id` and look up
//!   per-job state (assembler, progress sink, pause/cancel flags) via the
//!   shared [`JobContextMap`].
//! - Pause / cancel / completion are per-job flags; workers themselves are
//!   never torn down on job transitions. Pausing a job causes workers holding
//!   one of its items to return that item to the queue and pull something
//!   else. Cancelling a job drains its items and drops in-flight results.
//! - A supervisor task detects "all enabled servers circuit-broken for a
//!   given job" and emits [`ProgressUpdate::NoServersAvailable`] so the user
//!   can fix config and resume, matching the prior per-engine behaviour.
//!
//! Retry logic (per article):
//! 1. Try the article on the current server up to [`MAX_TRIES_PER_SERVER`]
//!    times, reconnecting on transient errors.
//! 2. On `ArticleNotFound` (430) — requeue with the current server added to
//!    `tried_servers`; another worker on a different server picks it up.
//! 3. On connection loss — requeue and reconnect.
//! 4. On decode error — treated like "not available on this server", try
//!    another.
//! 5. When every enabled server is in `tried_servers` (or circuit-broken),
//!    the article is marked failed.
//! 6. A job only fails if failed articles exceed the threshold and no par2
//!    recovery is possible.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::nzb_core::config::ServerConfig;
use crate::nzb_core::models::NzbJob;
use crate::nzb_core::nzb_nntp::Pipeline;
use crate::nzb_core::nzb_nntp::connection::NntpConnection;
use crate::nzb_core::nzb_nntp::error::NntpError;
use nzb_decode::FileAssembler;
use nzb_decode::yenc::decode_yenc;

use crate::bandwidth::BandwidthLimiter;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Max times to retry an article on the SAME server before trying the next.
const MAX_TRIES_PER_SERVER: u32 = 3;
/// Delay between reconnection attempts.
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
/// Max reconnect attempts before giving up on a server for this session.
const MAX_RECONNECT_ATTEMPTS: u32 = 5;
/// Stagger delay between worker initial connections to avoid thundering herd.
/// Each worker waits conn_idx * WORKER_RAMP_DELAY before its first connect.
const WORKER_RAMP_DELAY: Duration = Duration::from_millis(15);
/// Consecutive connect failures before circuit-breaking a server.
const CIRCUIT_BREAK_THRESHOLD: u32 = 3;
/// Cooldown after auth/permission failure (bad credentials, 502, account blocked).
const AUTH_FAILURE_COOLDOWN: Duration = Duration::from_secs(120);
/// Cooldown after transient connection failures exceed threshold.
const TRANSIENT_FAILURE_COOLDOWN: Duration = Duration::from_secs(30);
/// Supervisor tick interval for detecting stuck jobs.
const SUPERVISOR_INTERVAL: Duration = Duration::from_secs(5);
/// Worker idle poll interval when the shared queue is empty.
const WORKER_IDLE_POLL: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// Global connection tracking
// ---------------------------------------------------------------------------

/// Tracks active NNTP connections per server for observability.
///
/// With the shared worker pool, the per-server worker count is the hard cap
/// on concurrent connections — this tracker exists to validate that invariant
/// and to surface warnings if anything escapes the pool (e.g. future direct
/// readers). Workers increment on connect, decrement on disconnect via
/// [`ConnectionGuard`].
pub struct ConnectionTracker {
    counts: Mutex<HashMap<String, Arc<AtomicUsize>>>,
    limits: Mutex<HashMap<String, usize>>,
}

impl ConnectionTracker {
    pub fn new() -> Self {
        Self {
            counts: Mutex::new(HashMap::new()),
            limits: Mutex::new(HashMap::new()),
        }
    }

    pub fn set_limit(&self, server_id: &str, limit: usize) {
        self.limits.lock().insert(server_id.to_string(), limit);
    }

    fn counter(&self, server_id: &str) -> Arc<AtomicUsize> {
        let mut counts = self.counts.lock();
        counts
            .entry(server_id.to_string())
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone()
    }

    pub fn connect(&self, server_id: &str, server_name: &str) -> usize {
        let counter = self.counter(server_id);
        let new_count = counter.fetch_add(1, Ordering::Relaxed) + 1;
        let limit = self.limits.lock().get(server_id).copied().unwrap_or(0);
        if limit > 0 && new_count > limit {
            warn!(
                server = %server_name,
                active = new_count,
                limit,
                "NNTP connections EXCEED configured limit"
            );
        }
        new_count
    }

    pub fn disconnect(&self, server_id: &str) {
        let counter = self.counter(server_id);
        let prev = counter.fetch_sub(1, Ordering::Relaxed);
        if prev == 0 {
            counter.store(0, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> Vec<(String, usize, usize)> {
        let counts = self.counts.lock();
        let limits = self.limits.lock();
        counts
            .iter()
            .map(|(id, count)| {
                let limit = limits.get(id).copied().unwrap_or(0);
                (id.clone(), count.load(Ordering::Relaxed), limit)
            })
            .collect()
    }

    pub fn total(&self) -> usize {
        self.counts
            .lock()
            .values()
            .map(|c| c.load(Ordering::Relaxed))
            .sum()
    }
}

impl Default for ConnectionTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard that decrements the connection counter on drop.
struct ConnectionGuard {
    server_id: String,
    tracker: Arc<ConnectionTracker>,
    active: bool,
}

impl ConnectionGuard {
    fn new(tracker: Arc<ConnectionTracker>, server_id: &str, server_name: &str) -> Self {
        tracker.connect(server_id, server_name);
        Self {
            server_id: server_id.to_string(),
            tracker,
            active: true,
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if self.active {
            self.tracker.disconnect(&self.server_id);
        }
    }
}

// ---------------------------------------------------------------------------
// Server health tracking (circuit breaker)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ServerHealth {
    pub consecutive_failures: u32,
    pub disabled_until: Option<Instant>,
    pub reason: Option<String>,
    pub is_auth_failure: bool,
}

impl Default for ServerHealth {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerHealth {
    pub fn new() -> Self {
        Self {
            consecutive_failures: 0,
            disabled_until: None,
            reason: None,
            is_auth_failure: false,
        }
    }

    pub fn is_available(&self) -> bool {
        match self.disabled_until {
            None => true,
            Some(until) => Instant::now() >= until,
        }
    }

    pub fn record_failure(&mut self, is_auth: bool, reason: &str) {
        self.consecutive_failures += 1;
        self.is_auth_failure = is_auth;
        self.reason = Some(reason.to_string());

        if is_auth || self.consecutive_failures >= CIRCUIT_BREAK_THRESHOLD {
            let cooldown = if is_auth {
                AUTH_FAILURE_COOLDOWN
            } else {
                TRANSIENT_FAILURE_COOLDOWN
            };
            self.disabled_until = Some(Instant::now() + cooldown);
        }
    }

    pub fn record_success(&mut self) {
        *self = Self::new();
    }
}

pub type ServerHealthMap = Arc<Mutex<HashMap<String, ServerHealth>>>;

// ---------------------------------------------------------------------------
// Progress update messages
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ProgressUpdate {
    ArticleComplete {
        job_id: String,
        file_id: String,
        segment_number: u32,
        decoded_bytes: u64,
        file_complete: bool,
        server_id: Option<String>,
    },
    ArticleFailed {
        job_id: String,
        file_id: String,
        segment_number: u32,
        error: String,
        server_id: Option<String>,
    },
    JobFinished {
        job_id: String,
        success: bool,
        articles_failed: usize,
    },
    NoServersAvailable {
        job_id: String,
        reason: String,
    },
    JobAborted {
        job_id: String,
        reason: String,
    },
}

// ---------------------------------------------------------------------------
// Work item
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct WorkItem {
    pub(crate) job_id: String,
    pub(crate) file_id: String,
    pub(crate) filename: String,
    pub(crate) message_id: String,
    pub(crate) segment_number: u32,
    /// Servers already tried for this article (by server ID).
    pub(crate) tried_servers: Vec<String>,
    /// Number of attempts on the current server.
    pub(crate) tries_on_current: u32,
}

// ---------------------------------------------------------------------------
// Per-job context
// ---------------------------------------------------------------------------

/// Per-job state that workers reference via `item.job_id`.
///
/// Everything a worker needs to process an article for a specific job lives
/// here. The queue manager owns one `Arc<JobContext>` per active job; the
/// pool holds a clone in its [`JobContextMap`] so workers can look it up.
pub(crate) struct JobContext {
    pub job_id: String,
    pub work_dir: PathBuf,
    pub assembler: Arc<FileAssembler>,
    pub progress_tx: mpsc::UnboundedSender<ProgressUpdate>,
    pub yenc_names: Arc<Mutex<HashMap<String, String>>>,
    pub nzb_filenames: HashMap<String, String>,
    /// Articles that still need a definitive result (success or all-server
    /// failure). When this reaches zero, `JobFinished` is emitted.
    pub articles_remaining: AtomicUsize,
    pub articles_failed: AtomicUsize,
    pub paused: AtomicBool,
    pub cancelled: AtomicBool,
    /// Optional abort reason — if set when articles_remaining hits zero
    /// (or when cancellation fires), `JobAborted` is emitted instead of
    /// `JobFinished`.
    pub abort_reason: Mutex<Option<String>>,
    pub total_decode_us: Arc<AtomicU64>,
    pub total_assemble_us: Arc<AtomicU64>,
    pub total_articles_decoded: Arc<AtomicU64>,
    pub engine_start: Instant,
    /// Total bytes across all files (for perf summary throughput).
    pub total_bytes: u64,
    /// Ensures JobFinished/JobAborted is only emitted once.
    finished: AtomicBool,
}

pub(crate) type JobContextMap = Arc<Mutex<HashMap<String, Arc<JobContext>>>>;

impl JobContext {
    fn new(
        job: &NzbJob,
        assembler: Arc<FileAssembler>,
        progress_tx: mpsc::UnboundedSender<ProgressUpdate>,
        total_articles: usize,
    ) -> Self {
        let nzb_filenames = job
            .files
            .iter()
            .map(|f| (f.id.clone(), f.filename.clone()))
            .collect();
        Self {
            job_id: job.id.clone(),
            work_dir: job.work_dir.clone(),
            assembler,
            progress_tx,
            yenc_names: Arc::new(Mutex::new(HashMap::new())),
            nzb_filenames,
            articles_remaining: AtomicUsize::new(total_articles),
            articles_failed: AtomicUsize::new(0),
            paused: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            abort_reason: Mutex::new(None),
            total_decode_us: Arc::new(AtomicU64::new(0)),
            total_assemble_us: Arc::new(AtomicU64::new(0)),
            total_articles_decoded: Arc::new(AtomicU64::new(0)),
            engine_start: Instant::now(),
            total_bytes: job.total_bytes,
            finished: AtomicBool::new(false),
        }
    }

    /// Decrement articles_remaining. If it reaches zero, run deobfuscation
    /// and emit the job-finished/aborted terminal update. Idempotent.
    fn resolve_one(&self) {
        let prev = self.articles_remaining.fetch_sub(1, Ordering::Relaxed);
        if prev != 1 {
            return;
        }
        self.emit_terminal();
    }

    /// Emit the terminal (JobFinished / JobAborted) message. Safe to call
    /// multiple times; only the first call does anything.
    fn emit_terminal(&self) {
        if self.finished.swap(true, Ordering::Relaxed) {
            return;
        }

        // Run deobfuscation before signalling completion so post-processing
        // sees the final filenames.
        self.deobfuscate_files();

        let download_elapsed = self.engine_start.elapsed();
        let decode_total_us = self.total_decode_us.load(Ordering::Relaxed);
        let assemble_total_us = self.total_assemble_us.load(Ordering::Relaxed);
        let articles_decoded = self.total_articles_decoded.load(Ordering::Relaxed);
        let elapsed_us = download_elapsed.as_micros().max(1);
        let throughput_mbps = (self.total_bytes as f64 / download_elapsed.as_secs_f64().max(0.001))
            / (1024.0 * 1024.0);
        info!(
            job_id = %self.job_id,
            elapsed_secs = download_elapsed.as_secs_f64(),
            total_bytes = self.total_bytes,
            throughput_mbps = format!("{throughput_mbps:.2}"),
            "Download phase complete"
        );
        info!(
            job_id = %self.job_id,
            articles_decoded,
            decode_secs = format!("{:.3}", decode_total_us as f64 / 1_000_000.0),
            assemble_secs = format!("{:.3}", assemble_total_us as f64 / 1_000_000.0),
            decode_pct = format!("{:.1}", decode_total_us as f64 / elapsed_us as f64 * 100.0),
            assemble_pct = format!("{:.1}", assemble_total_us as f64 / elapsed_us as f64 * 100.0),
            "Decode timing summary (cumulative across all workers)"
        );

        let abort_reason = self.abort_reason.lock().clone();
        if let Some(reason) = abort_reason {
            let _ = self.progress_tx.send(ProgressUpdate::JobAborted {
                job_id: self.job_id.clone(),
                reason,
            });
            return;
        }

        let failed = self.articles_failed.load(Ordering::Relaxed);
        let _ = self.progress_tx.send(ProgressUpdate::JobFinished {
            job_id: self.job_id.clone(),
            success: failed == 0,
            articles_failed: failed,
        });
    }

    /// Choose the best filename between NZB subject and yEnc header per file
    /// and rename on disk if needed. Called exactly once at job completion.
    fn deobfuscate_files(&self) {
        let renames = self.yenc_names.lock();
        for (file_id, yenc_name) in renames.iter() {
            let Some(nzb_name) = self.nzb_filenames.get(file_id) else {
                continue;
            };
            if nzb_name == yenc_name {
                continue;
            }
            let clean_yenc = std::path::Path::new(yenc_name.as_str())
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(yenc_name);
            if clean_yenc.is_empty() || nzb_name == clean_yenc {
                continue;
            }

            let nzb_has_ext = has_known_extension(nzb_name);
            let yenc_has_ext = has_known_extension(clean_yenc);

            let (old_name, new_name) = if yenc_has_ext && !nzb_has_ext {
                (nzb_name.as_str(), clean_yenc)
            } else if nzb_has_ext && !yenc_has_ext {
                continue;
            } else if yenc_has_ext && nzb_has_ext {
                (nzb_name.as_str(), clean_yenc)
            } else {
                continue;
            };

            let old_path = self.work_dir.join(old_name);
            let new_path = self.work_dir.join(new_name);
            if old_path.exists() && !new_path.exists() {
                if let Err(e) = std::fs::rename(&old_path, &new_path) {
                    warn!(
                        job_id = %self.job_id,
                        from = %old_name,
                        to = %new_name,
                        "Failed to deobfuscate file: {e}"
                    );
                } else {
                    info!(
                        job_id = %self.job_id,
                        from = %old_name,
                        to = %new_name,
                        "Deobfuscated file"
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Shared work queue
// ---------------------------------------------------------------------------

/// Multi-job FIFO work queue with PAR2-first priority within each submission.
///
/// Items submitted via [`SharedWorkQueue::submit_items`] are inserted so that
/// PAR2 index and volume files land ahead of data files (matching the prior
/// per-job ordering), while data files land at the tail. Cross-job ordering
/// is FIFO by submission time, per the chosen FIFO priority model.
pub(crate) struct SharedWorkQueue {
    inner: Mutex<VecDeque<WorkItem>>,
    notify: Notify,
}

impl SharedWorkQueue {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
        }
    }

    /// Insert a batch of work items with PAR2 items ahead of data items.
    /// Cross-batch order is preserved: PAR2 items from this batch go after
    /// any existing items, then data items.
    pub fn submit_items(&self, mut items: Vec<WorkItem>) {
        items.sort_by_key(|item| par2_sort_key(&item.filename));
        let had_items = !items.is_empty();
        {
            let mut q = self.inner.lock();
            q.reserve(items.len());
            for item in items {
                q.push_back(item);
            }
        }
        if had_items {
            self.notify.notify_waiters();
        }
    }

    /// Push a single item back onto the front (used when a worker is
    /// returning an item because its job is paused or its server was just
    /// tried for this item).
    fn push_front(&self, item: WorkItem) {
        self.inner.lock().push_front(item);
        self.notify.notify_waiters();
    }

    /// Push a single item to the back (used after handle_article_not_available
    /// when another server can still try it).
    fn push_back(&self, item: WorkItem) {
        self.inner.lock().push_back(item);
        self.notify.notify_waiters();
    }

    /// Pop the next item that can be processed by a worker on `server_id`.
    ///
    /// Skips items that have already tried `server_id`, rotating them to the
    /// back of the queue. Returns `None` if the queue is empty or if every
    /// item has already tried this server (the caller should sleep briefly).
    fn pop_workable(&self, server_id: &str) -> Option<WorkItem> {
        let mut q = self.inner.lock();
        let len = q.len();
        for _ in 0..len {
            let item = q.pop_front()?;
            if item.tried_servers.iter().any(|s| s == server_id) {
                q.push_back(item);
                continue;
            }
            return Some(item);
        }
        None
    }

    /// Remove all items belonging to `job_id`. Used on cancel_job / remove_job.
    fn drain_job(&self, job_id: &str) -> Vec<WorkItem> {
        let mut q = self.inner.lock();
        let mut kept = VecDeque::with_capacity(q.len());
        let mut drained = Vec::new();
        while let Some(item) = q.pop_front() {
            if item.job_id == job_id {
                drained.push(item);
            } else {
                kept.push_back(item);
            }
        }
        *q = kept;
        drained
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }
}

impl Default for SharedWorkQueue {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Worker pool
// ---------------------------------------------------------------------------

struct ActiveWorker {
    shutdown: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

/// Long-lived worker pool that services all active download jobs.
pub struct WorkerPool {
    work_queue: Arc<SharedWorkQueue>,
    job_contexts: JobContextMap,
    servers: Arc<Mutex<Vec<ServerConfig>>>,
    server_health: ServerHealthMap,
    bandwidth: Arc<BandwidthLimiter>,
    conn_tracker: Arc<ConnectionTracker>,
    stall_timeout: Option<Duration>,
    workers: Mutex<HashMap<String, Vec<ActiveWorker>>>,
    shutdown: Arc<AtomicBool>,
    supervisor_handle: Mutex<Option<JoinHandle<()>>>,
}

impl WorkerPool {
    pub fn new(
        servers: Arc<Mutex<Vec<ServerConfig>>>,
        bandwidth: Arc<BandwidthLimiter>,
        conn_tracker: Arc<ConnectionTracker>,
        stall_timeout_secs: u64,
    ) -> Arc<Self> {
        let stall_timeout = if stall_timeout_secs > 0 {
            Some(Duration::from_secs(stall_timeout_secs))
        } else {
            None
        };
        Arc::new(Self {
            work_queue: Arc::new(SharedWorkQueue::new()),
            job_contexts: Arc::new(Mutex::new(HashMap::new())),
            servers,
            server_health: Arc::new(Mutex::new(HashMap::new())),
            bandwidth,
            conn_tracker,
            stall_timeout,
            workers: Mutex::new(HashMap::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            supervisor_handle: Mutex::new(None),
        })
    }

    /// Spawn workers for all currently enabled servers and start the
    /// supervisor task. Call once at queue-manager startup.
    pub fn start(self: &Arc<Self>) {
        self.reconcile_servers();

        let this = Arc::clone(self);
        let handle = tokio::spawn(async move {
            this.supervisor_loop().await;
        });
        *self.supervisor_handle.lock() = Some(handle);
    }

    /// Create or tear down workers to match the current server list.
    ///
    /// For each enabled server, ensures exactly `server.connections` workers
    /// exist. Extra workers (from a shrunk limit or disabled server) have
    /// their per-worker shutdown flag flipped so they exit gracefully after
    /// the current article.
    pub fn reconcile_servers(self: &Arc<Self>) {
        if self.shutdown.load(Ordering::Relaxed) {
            return;
        }

        let servers_snapshot: Vec<ServerConfig> = self.servers.lock().clone();
        let mut workers = self.workers.lock();

        // First pass: retire workers for servers that are gone or disabled.
        let mut retire: Vec<String> = Vec::new();
        for key in workers.keys() {
            let still_active = servers_snapshot.iter().any(|s| s.enabled && &s.id == key);
            if !still_active {
                retire.push(key.clone());
            }
        }
        for key in retire {
            if let Some(list) = workers.remove(&key) {
                for w in list {
                    w.shutdown.store(true, Ordering::Relaxed);
                    // Don't await — workers check shutdown on next loop
                    // iteration and exit within ~WORKER_IDLE_POLL.
                    drop(w.handle);
                }
            }
        }

        // Second pass: spawn or shrink to match target count per enabled server.
        for server in &servers_snapshot {
            if !server.enabled {
                continue;
            }
            let target = (server.connections as usize).min(500);
            let entry = workers.entry(server.id.clone()).or_default();

            // Shrink: signal extras to exit.
            while entry.len() > target {
                if let Some(w) = entry.pop() {
                    w.shutdown.store(true, Ordering::Relaxed);
                    drop(w.handle);
                }
            }

            // Grow: spawn new workers with stagger.
            let current = entry.len();
            for conn_idx in current..target {
                let worker_shutdown = Arc::new(AtomicBool::new(false));
                let pool = Arc::clone(self);
                let server_clone = server.clone();
                let ws_clone = Arc::clone(&worker_shutdown);
                let handle = tokio::spawn(async move {
                    pool_worker(pool, server_clone, conn_idx, ws_clone).await;
                });
                entry.push(ActiveWorker {
                    shutdown: worker_shutdown,
                    handle,
                });
            }
        }
    }

    /// Register a new job context and submit its unfinished articles to the
    /// shared queue. Called by QueueManager::launch_download.
    pub(crate) fn submit_job(self: &Arc<Self>, ctx: Arc<JobContext>, items: Vec<WorkItem>) {
        let job_id = ctx.job_id.clone();
        if items.is_empty() {
            // Nothing to do — emit completion immediately.
            ctx.emit_terminal();
            return;
        }
        self.job_contexts.lock().insert(job_id.clone(), ctx);
        self.work_queue.submit_items(items);
        debug!(job_id = %job_id, queue_len = self.work_queue.len(), "Job submitted to worker pool");
    }

    /// Pause a job: workers stop pulling its items, and any item currently
    /// being held while paused is returned to the queue.
    pub fn pause_job(&self, job_id: &str) {
        if let Some(ctx) = self.job_contexts.lock().get(job_id) {
            ctx.paused.store(true, Ordering::Relaxed);
        }
    }

    /// Resume a paused job.
    pub fn resume_job(&self, job_id: &str) {
        if let Some(ctx) = self.job_contexts.lock().get(job_id) {
            ctx.paused.store(false, Ordering::Relaxed);
            // Wake any workers that were idle waiting for work.
            self.work_queue.notify.notify_waiters();
        }
    }

    /// Abort a job with a reason. Drains queued items, sets the abort flag,
    /// and emits JobAborted via the job's progress channel.
    pub fn abort_job(&self, job_id: &str, reason: String) {
        let ctx = self.job_contexts.lock().get(job_id).cloned();
        let Some(ctx) = ctx else {
            return;
        };
        *ctx.abort_reason.lock() = Some(reason);
        ctx.cancelled.store(true, Ordering::Relaxed);
        let drained = self.work_queue.drain_job(job_id);
        // Decrement the remaining counter for drained items so the terminal
        // callback fires if nothing is in-flight for this job.
        for _ in drained {
            ctx.resolve_one();
        }
        ctx.emit_terminal();
        self.job_contexts.lock().remove(job_id);
    }

    /// Cancel a job silently (no JobFinished / JobAborted emission).
    /// Used by `remove_job` when the user deletes a job from the queue —
    /// the progress receiver is about to be dropped anyway.
    pub fn cancel_job(&self, job_id: &str) {
        let ctx = self.job_contexts.lock().remove(job_id);
        let Some(ctx) = ctx else {
            return;
        };
        ctx.cancelled.store(true, Ordering::Relaxed);
        let _ = self.work_queue.drain_job(job_id);
    }

    /// Emit NoServersAvailable for a stuck job and unregister it.
    fn mark_no_servers(&self, job_id: &str, reason: String) {
        let ctx = self.job_contexts.lock().remove(job_id);
        let Some(ctx) = ctx else {
            return;
        };
        ctx.paused.store(true, Ordering::Relaxed);
        let _ = ctx.progress_tx.send(ProgressUpdate::NoServersAvailable {
            job_id: ctx.job_id.clone(),
            reason,
        });
        // Remove pending work for this job so other jobs aren't blocked.
        let _ = self.work_queue.drain_job(job_id);
    }

    /// Supervisor loop: periodically detects jobs whose remaining articles
    /// cannot possibly be fetched (all enabled servers circuit-broken or
    /// already-tried), and marks them NoServersAvailable.
    async fn supervisor_loop(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(SUPERVISOR_INTERVAL);
        loop {
            ticker.tick().await;
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            let enabled_servers: Vec<String> = {
                let srv = self.servers.lock();
                srv.iter()
                    .filter(|s| s.enabled)
                    .map(|s| s.id.clone())
                    .collect()
            };

            if enabled_servers.is_empty() {
                continue;
            }

            // Which servers are currently healthy?
            let healthy_servers: Vec<String> = {
                let health = self.server_health.lock();
                enabled_servers
                    .iter()
                    .filter(|sid| health.get(sid.as_str()).is_none_or(|h| h.is_available()))
                    .cloned()
                    .collect()
            };

            let all_broken = healthy_servers.is_empty();

            // Snapshot job contexts; don't hold the lock while sending.
            let ctxs: Vec<Arc<JobContext>> = self.job_contexts.lock().values().cloned().collect();

            for ctx in ctxs {
                if ctx.articles_remaining.load(Ordering::Relaxed) == 0 {
                    continue;
                }
                if ctx.cancelled.load(Ordering::Relaxed) {
                    continue;
                }
                if all_broken {
                    let reason = {
                        let health = self.server_health.lock();
                        health
                            .values()
                            .filter_map(|h| h.reason.clone())
                            .next()
                            .unwrap_or_else(|| "All servers unavailable".into())
                    };
                    warn!(
                        job_id = %ctx.job_id,
                        remaining = ctx.articles_remaining.load(Ordering::Relaxed),
                        "All servers circuit-broken — pausing job for user intervention"
                    );
                    self.mark_no_servers(&ctx.job_id, reason);
                }
            }
        }
    }

    /// Shut down all workers gracefully. In-flight articles finish first.
    pub async fn shutdown(self: &Arc<Self>) {
        self.shutdown.store(true, Ordering::Relaxed);
        let handles: Vec<JoinHandle<()>> = {
            let mut workers = self.workers.lock();
            let mut out = Vec::new();
            for (_id, list) in workers.drain() {
                for w in list {
                    w.shutdown.store(true, Ordering::Relaxed);
                    out.push(w.handle);
                }
            }
            out
        };
        // Notify workers so any parked on notify.notified() wake up.
        self.work_queue.notify.notify_waiters();

        let timeout = Duration::from_secs(10);
        for h in handles {
            let _ = tokio::time::timeout(timeout, h).await;
        }

        if let Some(h) = self.supervisor_handle.lock().take() {
            h.abort();
        }
    }

    pub fn conn_tracker(&self) -> &Arc<ConnectionTracker> {
        &self.conn_tracker
    }

    /// Whether this job still has an active context in the pool.
    pub fn has_job(&self, job_id: &str) -> bool {
        self.job_contexts.lock().contains_key(job_id)
    }
}

// ---------------------------------------------------------------------------
// Worker body
// ---------------------------------------------------------------------------

/// Single pool worker. Owns an NNTP connection to `primary_server` and pulls
/// items from the shared queue until `worker_shutdown` is flipped (server
/// reconciled away, limit shrunk) or the pool shuts down.
async fn pool_worker(
    pool: Arc<WorkerPool>,
    primary_server: ServerConfig,
    conn_idx: usize,
    worker_shutdown: Arc<AtomicBool>,
) {
    let worker_id = format!("{}#{}", primary_server.id, conn_idx);

    // Stagger worker startup to avoid thundering herd of connections.
    if conn_idx > 0 {
        let stagger = WORKER_RAMP_DELAY * conn_idx as u32;
        tokio::time::sleep(stagger).await;
    }

    let should_exit = |worker_shutdown: &Arc<AtomicBool>, pool: &Arc<WorkerPool>| {
        worker_shutdown.load(Ordering::Relaxed) || pool.shutdown.load(Ordering::Relaxed)
    };

    'reconnect: loop {
        if should_exit(&worker_shutdown, &pool) {
            return;
        }

        // Check circuit breaker before connecting. Compute an owned bool
        // so we don't hold the MutexGuard across an await point.
        let circuit_broken = {
            let health = pool.server_health.lock();
            health
                .get(&primary_server.id)
                .is_some_and(|h| !h.is_available())
        };
        if circuit_broken {
            tokio::time::sleep(WORKER_IDLE_POLL).await;
            continue 'reconnect;
        }

        info!(
            worker = %worker_id,
            server = %primary_server.name,
            host = %primary_server.host,
            port = primary_server.port,
            ssl = primary_server.ssl,
            conn_idx,
            "Worker starting — connecting to primary server"
        );

        let mut conn = NntpConnection::new(worker_id.clone());
        if let Err(e) = connect_with_retry(
            &mut conn,
            &primary_server,
            &worker_id,
            &pool.server_health,
            &pool.servers,
        )
        .await
        {
            warn!(
                worker = %worker_id,
                server = %primary_server.name,
                host = %primary_server.host,
                "Worker FAILED to connect after all retries: {e}"
            );
            if should_exit(&worker_shutdown, &pool) {
                return;
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
            continue 'reconnect;
        }

        let mut conn_guard = ConnectionGuard::new(
            Arc::clone(&pool.conn_tracker),
            &primary_server.id,
            &primary_server.name,
        );

        let pipe_depth = primary_server.pipelining.max(1);
        let active_conns = pool.conn_tracker.total();
        info!(
            worker = %worker_id,
            server = %primary_server.name,
            host = %primary_server.host,
            pipelining = pipe_depth,
            total_nntp_connections = active_conns,
            "Worker connected and ready"
        );

        let reconnect_needed = if pipe_depth <= 1 {
            run_worker_serial(
                &pool,
                &primary_server,
                &worker_id,
                &worker_shutdown,
                &mut conn,
                &mut conn_guard,
            )
            .await
        } else {
            run_worker_pipelined(
                &pool,
                &primary_server,
                &worker_id,
                pipe_depth,
                &worker_shutdown,
                &mut conn,
                &mut conn_guard,
            )
            .await
        };

        let _ = conn.quit().await;
        drop(conn_guard);

        match reconnect_needed {
            WorkerExit::Reconnect => {
                // Loop back to the top and reconnect.
                continue 'reconnect;
            }
            WorkerExit::Exit => {
                return;
            }
        }
    }
}

enum WorkerExit {
    /// Exit the worker function entirely (server retired or pool shutdown).
    Exit,
    /// Reconnect and keep pulling work (transient connection loss).
    Reconnect,
}

/// Wait for work, with early exit on shutdown / server retirement.
/// Returns `Some(item, ctx)` when a processable item is available, or `None`
/// if the worker should exit.
async fn next_work_item(
    pool: &Arc<WorkerPool>,
    server_id: &str,
    worker_shutdown: &Arc<AtomicBool>,
) -> Option<(WorkItem, Arc<JobContext>)> {
    loop {
        if worker_shutdown.load(Ordering::Relaxed) || pool.shutdown.load(Ordering::Relaxed) {
            return None;
        }

        if let Some(item) = pool.work_queue.pop_workable(server_id) {
            // Look up the job context. If the job is gone or cancelled, drop
            // the item and keep going.
            let ctx = pool.job_contexts.lock().get(&item.job_id).cloned();
            let Some(ctx) = ctx else {
                continue;
            };
            if ctx.cancelled.load(Ordering::Relaxed) {
                continue;
            }
            // Respect per-job pause: return the item and wait.
            if ctx.paused.load(Ordering::Relaxed) {
                pool.work_queue.push_back(item);
                tokio::time::sleep(WORKER_IDLE_POLL).await;
                continue;
            }
            return Some((item, ctx));
        }

        // Queue empty (or nothing workable for this server) — wait with a
        // timeout so we still notice shutdown and new work alike.
        let notified = pool.work_queue.notify.notified();
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(WORKER_IDLE_POLL) => {}
        }
    }
}

async fn run_worker_serial(
    pool: &Arc<WorkerPool>,
    primary_server: &ServerConfig,
    worker_id: &str,
    worker_shutdown: &Arc<AtomicBool>,
    conn: &mut NntpConnection,
    _conn_guard: &mut ConnectionGuard,
) -> WorkerExit {
    let mut consecutive_errors: u32 = 0;

    loop {
        // Server runtime checks.
        let server_disabled = pool
            .servers
            .lock()
            .iter()
            .find(|s| s.id == primary_server.id)
            .is_none_or(|s| !s.enabled);
        if server_disabled {
            info!(
                worker = %worker_id,
                server = %primary_server.name,
                "Server disabled, worker exiting"
            );
            return WorkerExit::Exit;
        }
        {
            let health = pool.server_health.lock();
            if let Some(h) = health.get(&primary_server.id)
                && !h.is_available()
            {
                info!(
                    worker = %worker_id,
                    server = %primary_server.name,
                    reason = h.reason.as_deref().unwrap_or("unknown"),
                    "Server circuit-broken, worker reconnecting after cooldown"
                );
                return WorkerExit::Reconnect;
            }
        }

        let Some((mut item, ctx)) = next_work_item(pool, &primary_server.id, worker_shutdown).await
        else {
            return WorkerExit::Exit;
        };

        let fetch_fut =
            fetch_article_with_retry(conn, &item, &ctx.assembler, primary_server, worker_id);
        let result = if let Some(timeout) = pool.stall_timeout {
            match tokio::time::timeout(timeout, fetch_fut).await {
                Ok(r) => r,
                Err(_) => {
                    warn!(
                        worker = %worker_id,
                        server = %primary_server.name,
                        article = %item.message_id,
                        "Connection stalled — no response within {}s, reconnecting",
                        timeout.as_secs()
                    );
                    pool.work_queue.push_front(item);
                    return WorkerExit::Reconnect;
                }
            }
        } else {
            fetch_fut.await
        };

        match result {
            Ok(process_result) => {
                consecutive_errors = 0;
                ctx.total_decode_us
                    .fetch_add(process_result.decode_us, Ordering::Relaxed);
                ctx.total_assemble_us
                    .fetch_add(process_result.assemble_us, Ordering::Relaxed);
                ctx.total_articles_decoded.fetch_add(1, Ordering::Relaxed);
                if let Some(ref yname) = process_result.yenc_filename {
                    ctx.yenc_names
                        .lock()
                        .entry(item.file_id.clone())
                        .or_insert_with(|| crate::util::normalize_nfc(yname));
                }
                if let Some(n) = std::num::NonZeroU32::new(process_result.decoded_bytes as u32) {
                    let _ = pool.bandwidth.acquire_download(n).await;
                }
                let _ = ctx.progress_tx.send(ProgressUpdate::ArticleComplete {
                    job_id: item.job_id.clone(),
                    file_id: item.file_id.clone(),
                    segment_number: item.segment_number,
                    decoded_bytes: process_result.decoded_bytes,
                    file_complete: process_result.file_complete,
                    server_id: Some(primary_server.id.clone()),
                });
                ctx.resolve_one();
            }
            Err(ArticleError::ArticleNotFound) => {
                handle_article_not_available(
                    &mut item,
                    primary_server,
                    &pool.servers,
                    &pool.server_health,
                    &ctx,
                    &pool.work_queue,
                    "Article not found on any server",
                );
            }
            Err(ArticleError::ConnectionLost(msg)) => {
                consecutive_errors += 1;
                warn!(
                    worker = %worker_id,
                    server = %primary_server.name,
                    host = %primary_server.host,
                    consecutive_errors,
                    max_reconnects = MAX_RECONNECT_ATTEMPTS,
                    article = %item.message_id,
                    "Connection lost: {msg}"
                );
                pool.work_queue.push_front(item);
                if consecutive_errors > MAX_RECONNECT_ATTEMPTS {
                    warn!(
                        worker = %worker_id,
                        server = %primary_server.name,
                        host = %primary_server.host,
                        consecutive_errors,
                        "Too many consecutive errors — worker reconnecting"
                    );
                    return WorkerExit::Reconnect;
                }
                return WorkerExit::Reconnect;
            }
            Err(ArticleError::DecodeError(msg)) => {
                handle_article_not_available(
                    &mut item,
                    primary_server,
                    &pool.servers,
                    &pool.server_health,
                    &ctx,
                    &pool.work_queue,
                    &format!("Decode error: {msg}"),
                );
            }
            Err(ArticleError::AssemblyError(msg)) => {
                error!(article = %item.message_id, "Assembly error: {msg}");
                let _ = ctx.progress_tx.send(ProgressUpdate::ArticleFailed {
                    job_id: item.job_id.clone(),
                    file_id: item.file_id.clone(),
                    segment_number: item.segment_number,
                    error: format!("Assembly error: {msg}"),
                    server_id: Some(primary_server.id.clone()),
                });
                ctx.articles_failed.fetch_add(1, Ordering::Relaxed);
                ctx.resolve_one();
            }
        }
    }
}

async fn run_worker_pipelined(
    pool: &Arc<WorkerPool>,
    primary_server: &ServerConfig,
    worker_id: &str,
    pipe_depth: u8,
    worker_shutdown: &Arc<AtomicBool>,
    conn: &mut NntpConnection,
    _conn_guard: &mut ConnectionGuard,
) -> WorkerExit {
    let mut pipeline = Pipeline::new(pipe_depth);
    let mut in_flight_items: HashMap<u64, WorkItem> = HashMap::new();
    let mut next_tag: u64 = 0;
    let mut consecutive_errors: u32 = 0;

    // Perf metrics
    let mut perf_articles: u64 = 0;
    let mut perf_bytes: u64 = 0;
    let mut perf_queue_lock_us: u64 = 0;
    let mut perf_receive_us: u64 = 0;
    let mut perf_decode_us: u64 = 0;
    let mut perf_assemble_us: u64 = 0;
    let mut perf_bandwidth_us: u64 = 0;
    let mut perf_yield_us: u64 = 0;
    let mut perf_flush_us: u64 = 0;
    let mut perf_last_log = Instant::now();
    const PERF_LOG_INTERVAL: Duration = Duration::from_secs(10);

    loop {
        if worker_shutdown.load(Ordering::Relaxed) || pool.shutdown.load(Ordering::Relaxed) {
            requeue_all(&mut in_flight_items, &pool.work_queue);
            return WorkerExit::Exit;
        }

        // Server runtime checks.
        let server_disabled = pool
            .servers
            .lock()
            .iter()
            .find(|s| s.id == primary_server.id)
            .is_none_or(|s| !s.enabled);
        if server_disabled {
            info!(
                worker = %worker_id,
                server = %primary_server.name,
                "Server disabled, worker exiting"
            );
            requeue_all(&mut in_flight_items, &pool.work_queue);
            return WorkerExit::Exit;
        }
        {
            let health = pool.server_health.lock();
            if let Some(h) = health.get(&primary_server.id)
                && !h.is_available()
            {
                info!(
                    worker = %worker_id,
                    server = %primary_server.name,
                    reason = h.reason.as_deref().unwrap_or("unknown"),
                    "Server circuit-broken, worker reconnecting after cooldown"
                );
                requeue_all(&mut in_flight_items, &pool.work_queue);
                return WorkerExit::Reconnect;
            }
        }

        // Fill the pipeline.
        while pipeline.pending_count() + pipeline.in_flight_count() < pipe_depth as usize {
            let lock_t = Instant::now();
            let item = pool.work_queue.pop_workable(&primary_server.id);
            perf_queue_lock_us += lock_t.elapsed().as_micros() as u64;
            let Some(item) = item else {
                break;
            };
            // Look up ctx to respect pause/cancel.
            let ctx = pool.job_contexts.lock().get(&item.job_id).cloned();
            let Some(ctx) = ctx else {
                continue;
            };
            if ctx.cancelled.load(Ordering::Relaxed) {
                continue;
            }
            if ctx.paused.load(Ordering::Relaxed) {
                pool.work_queue.push_back(item);
                break;
            }
            let tag = next_tag;
            next_tag += 1;
            pipeline.submit(item.message_id.clone(), tag);
            in_flight_items.insert(tag, item);
        }

        // If nothing is queued and nothing in flight, wait for work.
        if pipeline.is_empty() && in_flight_items.is_empty() {
            let Some((first_item, ctx)) =
                next_work_item(pool, &primary_server.id, worker_shutdown).await
            else {
                return WorkerExit::Exit;
            };
            let _ = ctx; // ctx is validated in next_work_item
            let tag = next_tag;
            next_tag += 1;
            pipeline.submit(first_item.message_id.clone(), tag);
            in_flight_items.insert(tag, first_item);
        }

        let flush_t = Instant::now();
        if let Err(e) = pipeline.flush_sends(conn).await {
            warn!(
                worker = %worker_id,
                server = %primary_server.name,
                host = %primary_server.host,
                error = %e,
                in_flight = in_flight_items.len(),
                "Pipeline send error — re-queuing all in-flight items"
            );
            requeue_all(&mut in_flight_items, &pool.work_queue);
            consecutive_errors += 1;
            if consecutive_errors > MAX_RECONNECT_ATTEMPTS {
                warn!(
                    worker = %worker_id,
                    server = %primary_server.name,
                    consecutive_errors,
                    "Too many pipeline errors — worker reconnecting"
                );
                return WorkerExit::Reconnect;
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
            return WorkerExit::Reconnect;
        }
        perf_flush_us += flush_t.elapsed().as_micros() as u64;

        // Read one response.
        let recv_t = Instant::now();
        let result = if let Some(timeout) = pool.stall_timeout {
            match tokio::time::timeout(timeout, pipeline.receive_one(conn)).await {
                Ok(r) => r,
                Err(_) => {
                    let elapsed_ms = recv_t.elapsed().as_millis();
                    warn!(
                        worker = %worker_id,
                        server = %primary_server.name,
                        elapsed_ms,
                        in_flight = in_flight_items.len(),
                        "Connection stalled — no response within {}s, reconnecting",
                        timeout.as_secs()
                    );
                    requeue_all(&mut in_flight_items, &pool.work_queue);
                    return WorkerExit::Reconnect;
                }
            }
        } else {
            pipeline.receive_one(conn).await
        };
        perf_receive_us += recv_t.elapsed().as_micros() as u64;

        match result {
            Ok(Some(pipe_result)) => {
                let Some(mut item) = in_flight_items.remove(&pipe_result.request.tag) else {
                    continue;
                };
                // Look up the ctx for this item's job (may have been cancelled).
                let ctx = pool.job_contexts.lock().get(&item.job_id).cloned();
                let Some(ctx) = ctx else {
                    continue;
                };
                if ctx.cancelled.load(Ordering::Relaxed) {
                    continue;
                }

                match pipe_result.result {
                    Ok(response) => {
                        consecutive_errors = 0;
                        let raw_data = response.data.unwrap_or_default();
                        let yield_t = Instant::now();
                        tokio::task::yield_now().await;
                        perf_yield_us += yield_t.elapsed().as_micros() as u64;
                        match decode_and_assemble(&item, &raw_data, &ctx.assembler) {
                            Ok(process_result) => {
                                perf_decode_us += process_result.decode_us;
                                perf_assemble_us += process_result.assemble_us;
                                perf_bytes += process_result.decoded_bytes;
                                perf_articles += 1;
                                ctx.total_decode_us
                                    .fetch_add(process_result.decode_us, Ordering::Relaxed);
                                ctx.total_assemble_us
                                    .fetch_add(process_result.assemble_us, Ordering::Relaxed);
                                ctx.total_articles_decoded.fetch_add(1, Ordering::Relaxed);
                                if let Some(ref yname) = process_result.yenc_filename {
                                    ctx.yenc_names
                                        .lock()
                                        .entry(item.file_id.clone())
                                        .or_insert_with(|| crate::util::normalize_nfc(yname));
                                }
                                let bw_t = Instant::now();
                                if let Some(n) =
                                    std::num::NonZeroU32::new(process_result.decoded_bytes as u32)
                                {
                                    let _ = pool.bandwidth.acquire_download(n).await;
                                }
                                perf_bandwidth_us += bw_t.elapsed().as_micros() as u64;
                                let _ = ctx.progress_tx.send(ProgressUpdate::ArticleComplete {
                                    job_id: item.job_id.clone(),
                                    file_id: item.file_id.clone(),
                                    segment_number: item.segment_number,
                                    decoded_bytes: process_result.decoded_bytes,
                                    file_complete: process_result.file_complete,
                                    server_id: Some(primary_server.id.clone()),
                                });
                                ctx.resolve_one();

                                if perf_last_log.elapsed() >= PERF_LOG_INTERVAL {
                                    let elapsed = perf_last_log.elapsed().as_secs_f64();
                                    let mbps = perf_bytes as f64 / elapsed / (1024.0 * 1024.0);
                                    info!(
                                        worker = %worker_id,
                                        articles = perf_articles,
                                        throughput_mbps = format!("{mbps:.1}"),
                                        recv_ms = perf_receive_us / 1000,
                                        decode_ms = perf_decode_us / 1000,
                                        assemble_ms = perf_assemble_us / 1000,
                                        queue_lock_ms = perf_queue_lock_us / 1000,
                                        flush_ms = perf_flush_us / 1000,
                                        yield_ms = perf_yield_us / 1000,
                                        bw_wait_ms = perf_bandwidth_us / 1000,
                                        "Worker perf summary"
                                    );
                                    perf_articles = 0;
                                    perf_bytes = 0;
                                    perf_queue_lock_us = 0;
                                    perf_receive_us = 0;
                                    perf_decode_us = 0;
                                    perf_assemble_us = 0;
                                    perf_bandwidth_us = 0;
                                    perf_yield_us = 0;
                                    perf_flush_us = 0;
                                    perf_last_log = Instant::now();
                                }
                            }
                            Err(ArticleError::DecodeError(msg)) => {
                                handle_article_not_available(
                                    &mut item,
                                    primary_server,
                                    &pool.servers,
                                    &pool.server_health,
                                    &ctx,
                                    &pool.work_queue,
                                    &format!("Decode error: {msg}"),
                                );
                            }
                            Err(ArticleError::AssemblyError(msg)) => {
                                error!(article = %item.message_id, "Assembly error: {msg}");
                                let _ = ctx.progress_tx.send(ProgressUpdate::ArticleFailed {
                                    job_id: item.job_id.clone(),
                                    file_id: item.file_id.clone(),
                                    segment_number: item.segment_number,
                                    error: format!("Assembly error: {msg}"),
                                    server_id: Some(primary_server.id.clone()),
                                });
                                ctx.articles_failed.fetch_add(1, Ordering::Relaxed);
                                ctx.resolve_one();
                            }
                            Err(_) => {}
                        }
                    }
                    Err(NntpError::ArticleNotFound(_)) => {
                        handle_article_not_available(
                            &mut item,
                            primary_server,
                            &pool.servers,
                            &pool.server_health,
                            &ctx,
                            &pool.work_queue,
                            "Article not found on any server",
                        );
                    }
                    Err(NntpError::Connection(_) | NntpError::Io(_)) => {
                        warn!(
                            worker = %worker_id,
                            server = %primary_server.name,
                            host = %primary_server.host,
                            article = %item.message_id,
                            in_flight = in_flight_items.len(),
                            consecutive_errors,
                            "Pipeline: connection lost during receive — re-queuing all"
                        );
                        pool.work_queue.push_front(item);
                        requeue_all(&mut in_flight_items, &pool.work_queue);
                        consecutive_errors += 1;
                        if consecutive_errors > MAX_RECONNECT_ATTEMPTS {
                            return WorkerExit::Reconnect;
                        }
                        tokio::time::sleep(RECONNECT_DELAY).await;
                        return WorkerExit::Reconnect;
                    }
                    Err(e) => {
                        warn!(worker = %worker_id, article = %item.message_id, "Pipeline error: {e}");
                        handle_article_not_available(
                            &mut item,
                            primary_server,
                            &pool.servers,
                            &pool.server_health,
                            &ctx,
                            &pool.work_queue,
                            &format!("Pipeline error: {e}"),
                        );
                    }
                }
            }
            Ok(None) => {
                // No in-flight requests — loop will fill more.
            }
            Err(e) => {
                warn!(
                    worker = %worker_id,
                    server = %primary_server.name,
                    host = %primary_server.host,
                    error = %e,
                    in_flight = in_flight_items.len(),
                    consecutive_errors,
                    "Pipeline receive error"
                );
                requeue_all(&mut in_flight_items, &pool.work_queue);
                consecutive_errors += 1;
                if consecutive_errors > MAX_RECONNECT_ATTEMPTS {
                    return WorkerExit::Reconnect;
                }
                tokio::time::sleep(RECONNECT_DELAY).await;
                return WorkerExit::Reconnect;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Connection with retry
// ---------------------------------------------------------------------------

async fn connect_with_retry(
    conn: &mut NntpConnection,
    server: &ServerConfig,
    worker_id: &str,
    server_health: &ServerHealthMap,
    all_servers: &Arc<Mutex<Vec<ServerConfig>>>,
) -> Result<(), String> {
    for attempt in 1..=MAX_RECONNECT_ATTEMPTS {
        {
            let health = server_health.lock();
            if let Some(h) = health.get(&server.id)
                && !h.is_available()
            {
                return Err(format!(
                    "Server circuit-broken: {}",
                    h.reason.as_deref().unwrap_or("unknown")
                ));
            }
        }

        let current_config = all_servers
            .lock()
            .iter()
            .find(|s| s.id == server.id)
            .cloned()
            .unwrap_or_else(|| server.clone());

        info!(
            worker = %worker_id,
            server = %current_config.name,
            host = %current_config.host,
            port = current_config.port,
            attempt,
            max_attempts = MAX_RECONNECT_ATTEMPTS,
            "Connect attempt starting"
        );
        match conn.connect(&current_config).await {
            Ok(()) => {
                info!(
                    worker = %worker_id,
                    server = %current_config.name,
                    host = %current_config.host,
                    attempt,
                    "Connect attempt succeeded"
                );
                server_health
                    .lock()
                    .entry(server.id.clone())
                    .or_default()
                    .record_success();
                return Ok(());
            }
            Err(e) => {
                let is_auth = matches!(e, NntpError::Auth(_) | NntpError::ServiceUnavailable(_));
                {
                    let mut health = server_health.lock();
                    let entry = health.entry(server.id.clone()).or_default();
                    entry.record_failure(is_auth, &e.to_string());
                    if !entry.is_available() {
                        warn!(
                            worker = %worker_id,
                            server = %current_config.name,
                            host = %current_config.host,
                            error = %e,
                            cooldown_secs = if is_auth { AUTH_FAILURE_COOLDOWN.as_secs() } else { TRANSIENT_FAILURE_COOLDOWN.as_secs() },
                            "Server circuit-broken — stopping all connection attempts"
                        );
                        return Err(format!("Server circuit-broken: {e}"));
                    }
                }

                warn!(
                    worker = %worker_id,
                    server = %current_config.name,
                    host = %current_config.host,
                    attempt,
                    max_attempts = MAX_RECONNECT_ATTEMPTS,
                    error = %e,
                    is_auth,
                    "Connect attempt FAILED: {e}"
                );

                if is_auth {
                    return Err(format!("Auth/permission failure: {e}"));
                }

                if attempt < MAX_RECONNECT_ATTEMPTS {
                    info!(
                        worker = %worker_id,
                        server = %current_config.name,
                        delay_secs = RECONNECT_DELAY.as_secs(),
                        "Waiting before retry"
                    );
                    tokio::time::sleep(RECONNECT_DELAY).await;
                    *conn = NntpConnection::new(worker_id.to_string());
                } else {
                    return Err(format!(
                        "All {MAX_RECONNECT_ATTEMPTS} connect attempts failed: {e}"
                    ));
                }
            }
        }
    }
    Err("Connect retry loop exited unexpectedly".into())
}

// ---------------------------------------------------------------------------
// Helpers: re-queue, not-available routing, par2 sort key
// ---------------------------------------------------------------------------

/// Handle an article that's not available on this server (not found or decode
/// error): mark the server as tried and either requeue or mark failed.
fn handle_article_not_available(
    item: &mut WorkItem,
    primary_server: &ServerConfig,
    all_servers: &Arc<Mutex<Vec<ServerConfig>>>,
    server_health: &ServerHealthMap,
    ctx: &Arc<JobContext>,
    work_queue: &Arc<SharedWorkQueue>,
    error_msg: &str,
) {
    item.tried_servers.push(primary_server.id.clone());
    item.tries_on_current = 0;

    let all_tried = {
        let servers = all_servers.lock();
        let health = server_health.lock();
        servers.iter().filter(|s| s.enabled).all(|s| {
            item.tried_servers.contains(&s.id)
                || health.get(&s.id).is_some_and(|h| !h.is_available())
        })
    };

    if all_tried {
        warn!(article = %item.message_id, "{error_msg}");
        let _ = ctx.progress_tx.send(ProgressUpdate::ArticleFailed {
            job_id: item.job_id.clone(),
            file_id: item.file_id.clone(),
            segment_number: item.segment_number,
            error: error_msg.to_string(),
            server_id: Some(primary_server.id.clone()),
        });
        ctx.articles_failed.fetch_add(1, Ordering::Relaxed);
        ctx.resolve_one();
    } else {
        work_queue.push_back(item.clone());
    }
}

/// Re-queue all in-flight items back to the work queue (on connection loss).
fn requeue_all(in_flight: &mut HashMap<u64, WorkItem>, work_queue: &Arc<SharedWorkQueue>) {
    let items: Vec<WorkItem> = in_flight.drain().map(|(_, item)| item).collect();
    for item in items {
        work_queue.push_front(item);
    }
}

/// Sort key for work-queue prioritisation of PAR2 files. Index files (0)
/// first, then volume files (1), then data files (2).
fn par2_sort_key(filename: &str) -> u8 {
    let lower = filename.to_lowercase();
    if lower.ends_with(".par2") {
        if lower.contains(".vol") { 1 } else { 0 }
    } else {
        2
    }
}

fn has_known_extension(name: &str) -> bool {
    let lower = name.to_lowercase();
    if let Some(dot_pos) = lower.rfind('.') {
        let ext = &lower[dot_pos + 1..];
        matches!(
            ext,
            "rar"
                | "r00"
                | "r01"
                | "r02"
                | "r03"
                | "r04"
                | "r05"
                | "zip"
                | "7z"
                | "gz"
                | "bz2"
                | "xz"
                | "tar"
                | "mkv"
                | "mp4"
                | "avi"
                | "wmv"
                | "ts"
                | "m4v"
                | "mov"
                | "mpg"
                | "mpeg"
                | "mp3"
                | "flac"
                | "ogg"
                | "m4a"
                | "aac"
                | "wav"
                | "srt"
                | "sub"
                | "idx"
                | "ass"
                | "ssa"
                | "sup"
                | "nfo"
                | "jpg"
                | "jpeg"
                | "png"
                | "gif"
                | "bmp"
                | "par2"
                | "001"
                | "002"
                | "003"
                | "004"
                | "005"
        )
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// Public helper used by queue_manager: build work items + context for a job
// ---------------------------------------------------------------------------

/// Build the WorkItems for a job's unfinished articles and an initialised
/// JobContext. Called by QueueManager before [`WorkerPool::submit_job`].
pub(crate) fn build_job_submission(
    job: &NzbJob,
    progress_tx: mpsc::UnboundedSender<ProgressUpdate>,
) -> (Arc<JobContext>, Vec<WorkItem>) {
    let assembler = Arc::new(FileAssembler::new());
    for file in &job.files {
        let output_path = job.work_dir.join(&file.filename);
        if let Err(e) =
            assembler.register_file(&job.id, &file.id, output_path, file.articles.len() as u32)
        {
            error!(file = %file.filename, "Failed to register file for assembly: {e}");
        }
    }

    let work_items: Vec<WorkItem> = job
        .files
        .iter()
        .flat_map(|file| {
            file.articles
                .iter()
                .enumerate()
                .filter(|(_, a)| !a.downloaded)
                .map(move |(idx, article)| WorkItem {
                    job_id: job.id.clone(),
                    file_id: file.id.clone(),
                    filename: file.filename.clone(),
                    message_id: article.message_id.clone(),
                    segment_number: (idx as u32) + 1,
                    tried_servers: Vec::new(),
                    tries_on_current: 0,
                })
        })
        .collect();

    let total_remaining = work_items.len();
    let ctx = Arc::new(JobContext::new(
        job,
        assembler,
        progress_tx,
        total_remaining,
    ));
    (ctx, work_items)
}

// ---------------------------------------------------------------------------
// Article fetch with per-server retry
// ---------------------------------------------------------------------------

async fn fetch_article_with_retry(
    conn: &mut NntpConnection,
    item: &WorkItem,
    assembler: &FileAssembler,
    _server: &ServerConfig,
    worker_id: &str,
) -> Result<ProcessResult, ArticleError> {
    let mut last_error = None;

    for attempt in 1..=MAX_TRIES_PER_SERVER {
        let fetch_start = Instant::now();
        match conn.fetch_article(&item.message_id).await {
            Ok(response) => {
                let fetch_us = fetch_start.elapsed().as_micros();
                let raw_data = response.data.unwrap_or_default();
                debug!(
                    worker = %worker_id,
                    article = %item.message_id,
                    raw_bytes = raw_data.len(),
                    fetch_us,
                    "NNTP fetch complete"
                );
                return decode_and_assemble(item, &raw_data, assembler);
            }
            Err(NntpError::ArticleNotFound(_)) => {
                debug!(
                    worker = %worker_id,
                    article = %item.message_id,
                    "Article not found (430) — will try next server"
                );
                return Err(ArticleError::ArticleNotFound);
            }
            Err(e @ (NntpError::Connection(_) | NntpError::Io(_))) => {
                warn!(
                    worker = %worker_id,
                    article = %item.message_id,
                    attempt,
                    error = %e,
                    conn_state = ?conn.state,
                    "Connection/IO error during fetch — connection lost"
                );
                return Err(ArticleError::ConnectionLost(format!(
                    "Connection error on attempt {attempt}: {e}"
                )));
            }
            Err(e @ NntpError::Tls(_)) => {
                warn!(
                    worker = %worker_id,
                    article = %item.message_id,
                    attempt,
                    error = %e,
                    "TLS error during fetch — connection lost"
                );
                return Err(ArticleError::ConnectionLost(format!("TLS error: {e}")));
            }
            Err(e @ NntpError::ServiceUnavailable(_)) => {
                warn!(
                    worker = %worker_id,
                    article = %item.message_id,
                    attempt,
                    error = %e,
                    "Service unavailable (502) during article fetch — likely rate limited or blocked"
                );
                return Err(ArticleError::ConnectionLost(format!(
                    "Service unavailable: {e}"
                )));
            }
            Err(e @ NntpError::AuthRequired(_)) => {
                warn!(
                    worker = %worker_id,
                    article = %item.message_id,
                    attempt,
                    error = %e,
                    "Auth required (480) during article fetch — session expired or rate limited"
                );
                return Err(ArticleError::ConnectionLost(format!(
                    "Auth required mid-session: {e}"
                )));
            }
            Err(e) => {
                last_error = Some(format!("{e}"));
                if attempt < MAX_TRIES_PER_SERVER {
                    warn!(
                        worker = %worker_id,
                        article = %item.message_id,
                        attempt,
                        max_tries = MAX_TRIES_PER_SERVER,
                        error = %e,
                        "Transient fetch error, retrying in 500ms"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                } else {
                    warn!(
                        worker = %worker_id,
                        article = %item.message_id,
                        attempt,
                        error = %e,
                        "All retries on this server exhausted"
                    );
                }
            }
        }
    }

    Err(ArticleError::DecodeError(
        last_error.unwrap_or_else(|| "Unknown error after retries".into()),
    ))
}

// ---------------------------------------------------------------------------
// Article processing
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ProcessResult {
    decoded_bytes: u64,
    file_complete: bool,
    yenc_filename: Option<String>,
    decode_us: u64,
    assemble_us: u64,
}

#[derive(Debug, thiserror::Error)]
enum ArticleError {
    #[error("Article not found on server")]
    ArticleNotFound,
    #[error("Connection lost: {0}")]
    ConnectionLost(String),
    #[error("Decode error: {0}")]
    DecodeError(String),
    #[error("Assembly error: {0}")]
    AssemblyError(String),
}

fn decode_and_assemble(
    item: &WorkItem,
    raw_data: &[u8],
    assembler: &FileAssembler,
) -> Result<ProcessResult, ArticleError> {
    let decode_start = Instant::now();
    let decoded = decode_yenc(raw_data).map_err(|e| {
        ArticleError::DecodeError(format!(
            "yEnc decode failed for {} seg {}: {e}",
            item.filename, item.segment_number
        ))
    })?;
    let decode_us = decode_start.elapsed().as_micros();

    let yenc_filename = decoded.filename;
    let data_begin = decoded.part_begin.unwrap_or(0);
    let decoded_len = decoded.data.len() as u64;

    let assemble_start = Instant::now();
    let file_complete = assembler
        .assemble_article(
            &item.job_id,
            &item.file_id,
            item.segment_number,
            data_begin,
            &decoded.data,
        )
        .map_err(|e| {
            ArticleError::AssemblyError(format!(
                "Assembly failed for {} seg {}: {e}",
                item.filename, item.segment_number
            ))
        })?;
    let assemble_us = assemble_start.elapsed().as_micros();

    debug!(
        file = %item.filename,
        segment = item.segment_number,
        raw_bytes = raw_data.len(),
        decoded_bytes = decoded_len,
        decode_us,
        assemble_us,
        "Article decode+assemble timing"
    );

    Ok(ProcessResult {
        decoded_bytes: decoded_len,
        file_complete,
        yenc_filename,
        decode_us: decode_us as u64,
        assemble_us: assemble_us as u64,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_known_extension_recognizes_archives() {
        assert!(has_known_extension("movie.rar"));
        assert!(has_known_extension("movie.part01.rar"));
        assert!(has_known_extension("file.zip"));
        assert!(has_known_extension("file.7z"));
        assert!(has_known_extension("archive.001"));
    }

    #[test]
    fn has_known_extension_recognizes_video() {
        assert!(has_known_extension("episode.mkv"));
        assert!(has_known_extension("movie.mp4"));
        assert!(has_known_extension("video.avi"));
        assert!(has_known_extension("clip.ts"));
    }

    #[test]
    fn has_known_extension_recognizes_par2() {
        assert!(has_known_extension("file.par2"));
        assert!(has_known_extension("file.vol00+01.par2"));
        assert!(has_known_extension("file.vol015-031.par2"));
    }

    #[test]
    fn has_known_extension_recognizes_misc() {
        assert!(has_known_extension("info.nfo"));
        assert!(has_known_extension("sub.srt"));
        assert!(has_known_extension("cover.jpg"));
        assert!(has_known_extension("song.flac"));
    }

    #[test]
    fn has_known_extension_rejects_obfuscated_hashes() {
        assert!(!has_known_extension("9b6a324d7560b87091685020371ba869"));
        assert!(!has_known_extension("1fG1GP7L2263LHXH213HTNIxZsX7l0cv44BZ"));
        assert!(!has_known_extension("DfKUx3bl7L6PSo6276WSaXSZ7"));
        assert!(!has_known_extension("Q77O1ZxL237vc241z77hFoLBxl"));
    }

    #[test]
    fn has_known_extension_rejects_unknown_extensions() {
        assert!(!has_known_extension("file.xyz123"));
        assert!(!has_known_extension("noext"));
        assert!(!has_known_extension(""));
    }

    #[test]
    fn has_known_extension_case_insensitive() {
        assert!(has_known_extension("file.RAR"));
        assert!(has_known_extension("file.MKV"));
        assert!(has_known_extension("file.Par2"));
        assert!(has_known_extension("file.MP4"));
    }

    fn make_item(job_id: &str, msg_id: &str, filename: &str) -> WorkItem {
        WorkItem {
            job_id: job_id.to_string(),
            file_id: "f1".to_string(),
            filename: filename.to_string(),
            message_id: msg_id.to_string(),
            segment_number: 1,
            tried_servers: Vec::new(),
            tries_on_current: 0,
        }
    }

    #[test]
    fn shared_queue_par2_first() {
        let q = SharedWorkQueue::new();
        q.submit_items(vec![
            make_item("j1", "a", "movie.rar"),
            make_item("j1", "b", "movie.par2"),
            make_item("j1", "c", "movie.vol00+01.par2"),
            make_item("j1", "d", "movie.r00"),
        ]);
        let first = q.pop_workable("srv1").unwrap();
        assert_eq!(first.filename, "movie.par2", "index file first");
        let second = q.pop_workable("srv1").unwrap();
        assert_eq!(second.filename, "movie.vol00+01.par2", "vol file second");
    }

    #[test]
    fn shared_queue_skips_tried_servers() {
        let q = SharedWorkQueue::new();
        let mut item = make_item("j1", "a", "file.rar");
        item.tried_servers.push("srv1".to_string());
        q.submit_items(vec![item, make_item("j1", "b", "other.rar")]);

        // srv1 should skip the first item and return the second.
        let picked = q.pop_workable("srv1").unwrap();
        assert_eq!(picked.message_id, "b");
    }

    #[test]
    fn shared_queue_drain_job_removes_only_target() {
        let q = SharedWorkQueue::new();
        q.submit_items(vec![
            make_item("j1", "a", "a.rar"),
            make_item("j2", "b", "b.rar"),
            make_item("j1", "c", "c.rar"),
        ]);
        let drained = q.drain_job("j1");
        assert_eq!(drained.len(), 2);
        assert_eq!(q.len(), 1);
        let remaining = q.pop_workable("srv1").unwrap();
        assert_eq!(remaining.job_id, "j2");
    }
}
