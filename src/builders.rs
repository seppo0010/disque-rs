use std::collections::HashMap;
use std::time::Duration;

use disque::Disque;
use redis::{RedisResult, Iter, Value};

/// Helper to add a new job
///
/// # Examples
///
/// ```
/// # use disque::Disque;
/// # use disque::AddJobBuilder;
///
/// let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
/// let jobid = AddJobBuilder::new(b"example queue", b"my job", 10000
///     ).delay(1440).run(&disque).unwrap();
/// ```
pub struct AddJobBuilder<'a> {
    queue_name: &'a [u8],
    job: &'a [u8],
    timeout: Duration,
    replicate: Option<usize>,
    delay: Option<Duration>,
    retry: Option<Duration>,
    ttl: Option<Duration>,
    maxlen: Option<usize>,
    async: bool,
}

impl<'a> AddJobBuilder<'a> {
    /// Creates a new builder for `queue_name`.
    /// Timeout is specified in milliseconds.
    pub fn new(queue_name: &'a [u8], job: &'a [u8], timeout_ms: u64
            ) -> AddJobBuilder<'a> {
        AddJobBuilder {
            queue_name: queue_name,
            job: job,
            timeout: Duration::from_millis(timeout_ms),
            replicate: None,
            delay: None,
            retry: None,
            ttl: None,
            maxlen: None,
            async: false,
        }
    }

    /// Changes the queue name where the job will be added.
    pub fn queue_name(&mut self, queue_name: &'a [u8]) -> &mut Self {
        self.queue_name = queue_name; self
    }

    /// Changes the job body.
    pub fn job(&mut self, job: &'a [u8]) -> &mut Self {
        self.job = job; self
    }

    /// Changes the timeout. It must be specified in milliseconds.
    pub fn timeout(&mut self, timeout_ms: u64) -> &mut Self {
        self.timeout = Duration::from_millis(timeout_ms); self
    }

    /// The number of nodes the job should be replicated to.
    pub fn replicate(&mut self, replicate: usize) -> &mut Self {
        self.replicate = Some(replicate); self
    }

    /// The number of seconds that should elapse before the job is queued.
    pub fn delay(&mut self, delay: u64) -> &mut Self {
        self.delay = Some(Duration::from_secs(delay)); self
    }

    /// Period after which, if no ACK is received, the job is put again
    /// into the queue for delivery
    pub fn retry(&mut self, retry: u64) -> &mut Self {
        self.retry = Some(Duration::from_secs(retry)); self
    }

    /// The max job life in seconds. After this time, the job is deleted even
    /// if it was not successfully delivered.
    pub fn ttl(&mut self, ttl: u64) -> &mut Self {
        self.ttl = Some(Duration::from_secs(ttl)); self
    }

    /// If there are already count messages queued for the specified queue
    /// name, the message is refused and an error reported to the client.
    pub fn maxlen(&mut self, maxlen: usize) -> &mut Self {
        self.maxlen = Some(maxlen); self
    }

    /// If true, asks the server to let the command return ASAP and replicate
    /// the job to other nodes in the background.
    /// Otherwise, the job is put into the queue only when the client gets a
    /// positive reply.
    pub fn async(&mut self, async: bool) -> &mut Self {
        self.async = async; self
    }

    /// Executes the addjob command.
    pub fn run(&self, disque: &Disque) -> RedisResult<String> {
        disque.addjob(self.queue_name, self.job, self.timeout, self.replicate,
                self.delay, self.retry, self.ttl, self.maxlen, self.async)
    }
}

#[test]
fn add_job_builder() {
    let mut jb = AddJobBuilder::new(b"queue", b"job", 123);
    assert_eq!(jb.queue_name, b"queue");
    assert_eq!(jb.job, b"job");
    assert_eq!(jb.timeout, Duration::from_millis(123));
    assert_eq!(jb.replicate, None);
    assert_eq!(jb.delay, None);
    assert_eq!(jb.retry, None);
    assert_eq!(jb.ttl, None);
    assert_eq!(jb.maxlen, None);
    assert_eq!(jb.async, false);
    jb.replicate(3).delay(4).retry(5).ttl(6).maxlen(7).async(true);
    assert_eq!(jb.queue_name, b"queue");
    assert_eq!(jb.job, b"job");
    assert_eq!(jb.timeout, Duration::from_millis(123));
    assert_eq!(jb.replicate, Some(3));
    assert_eq!(jb.delay, Some(Duration::from_secs(4)));
    assert_eq!(jb.retry, Some(Duration::from_secs(5)));
    assert_eq!(jb.ttl, Some(Duration::from_secs(6)));
    assert_eq!(jb.maxlen, Some(7));
    assert_eq!(jb.async, true);
    jb.queue_name(b"my queue").job(b"my job").timeout(234);
    assert_eq!(jb.queue_name, b"my queue");
    assert_eq!(jb.job, b"my job");
    assert_eq!(jb.timeout, Duration::from_millis(234));
    assert_eq!(jb.replicate, Some(3));
    assert_eq!(jb.delay, Some(Duration::from_secs(4)));
    assert_eq!(jb.retry, Some(Duration::from_secs(5)));
    assert_eq!(jb.ttl, Some(Duration::from_secs(6)));
    assert_eq!(jb.maxlen, Some(7));
    assert_eq!(jb.async, true);
}

/// Helper to get a list of queues
///
/// # Examples
///
/// ```
/// # use disque::Disque;
/// # use disque::QueueQueryBuilder;
///
/// let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
/// let queues = QueueQueryBuilder::new().busyloop(true).minlen(50)
///     .iter(&disque).unwrap().collect::<Vec<_>>();
/// assert!(queues.len() >= 0);
/// ```
pub struct QueueQueryBuilder {
    count: u64,
    busyloop: bool,
    minlen: Option<u64>,
    maxlen: Option<u64>,
    importrate: Option<u64>,
}

impl QueueQueryBuilder {
    /// Creates a new builder.
    pub fn new() -> QueueQueryBuilder {
        QueueQueryBuilder {
            count: 16,
            busyloop: false,
            minlen: None,
            maxlen: None,
            importrate: None,
        }
    }

    /// A hint about how much work to do per iteration.
    pub fn count(&mut self, count: u64) -> &mut Self {
        self.count = count; self
    }

    /// If true, blocks and returns all the queues in a busy loop.
    pub fn busyloop(&mut self, busyloop: bool) -> &mut Self {
        self.busyloop = busyloop; self
    }

    /// Only return queues with at least `minlen` jobs.
    pub fn minlen(&mut self, minlen: u64) -> &mut Self {
        self.minlen = Some(minlen); self
    }

    /// Only return queues with at most `maxlen` jobs.
    pub fn maxlen(&mut self, maxlen: u64) -> &mut Self {
        self.maxlen = Some(maxlen); self
    }

    /// Only return queues with a job import rate (from other nodes) greater
    /// than or equal to  `importrate`.
    pub fn importrate(&mut self, importrate: u64) -> &mut Self {
        self.importrate = Some(importrate); self
    }

    /// Gets the queue iterator.
    pub fn iter<'a>(&'a self, disque: &'a Disque) -> RedisResult<Iter<Vec<u8>>> {
        disque.qscan(0, self.count, self.busyloop, self.minlen, self.maxlen,
                self.importrate)
    }
}

#[test]
fn queue_query_builder() {
    let mut qqb = QueueQueryBuilder::new();
    assert_eq!(qqb.count, 16);
    assert_eq!(qqb.busyloop, false);
    assert_eq!(qqb.minlen, None);
    assert_eq!(qqb.maxlen, None);
    assert_eq!(qqb.importrate, None);
    qqb.count(11).busyloop(true).minlen(1).maxlen(10).importrate(5);
    assert_eq!(qqb.count, 11);
    assert_eq!(qqb.busyloop, true);
    assert_eq!(qqb.minlen, Some(1));
    assert_eq!(qqb.maxlen, Some(10));
    assert_eq!(qqb.importrate, Some(5));
}

/// Helper to get a list of jobs
///
/// # Examples
///
/// ```
/// # use disque::Disque;
/// # use disque::JobQueryBuilder;
///
/// let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
/// let jobs = JobQueryBuilder::new().queue(b"my queue").state("queued")
///     .iter_ids(&disque).unwrap().collect::<Vec<_>>();
/// assert!(jobs.len() >= 0);
/// ```
pub struct JobQueryBuilder<'a> {
    count: u64,
    blocking: bool,
    queue: Option<&'a [u8]>,
    states: Vec<&'a str>,
}

impl<'a> JobQueryBuilder<'a> {
    pub fn new() -> JobQueryBuilder<'a> {
        JobQueryBuilder {
            count: 16,
            blocking: false,
            queue: None,
            states: vec![],
        }
    }

    pub fn count(&mut self, count: u64) -> &mut Self {
        self.count = count; self
    }

    pub fn blocking(&mut self, blocking: bool) -> &mut Self {
        self.blocking = blocking; self
    }

    pub fn queue(&mut self, queue: &'a [u8]) -> &mut Self {
        self.queue = Some(queue); self
    }

    pub fn state(&mut self, state: &'a str) -> &mut Self {
        self.states.push(state); self
    }

    /// Gets a job ids iterator.
    pub fn iter_ids<'b>(&'b self, disque: &'b Disque
            ) -> RedisResult<Iter<String>> {
        disque.jscan_id(0, self.count, self.blocking, self.queue,
                &*self.states)
    }

    /// Gets a job information iterator.
    pub fn iter_all<'b>(&'b self, disque: &'b Disque
            ) -> RedisResult<Iter<HashMap<String, Value>>> {
        disque.jscan_all(0, self.count, self.blocking, self.queue,
                &*self.states)
    }
}

#[test]
fn job_query_builder() {
    let mut jqb = JobQueryBuilder::new();
    assert_eq!(jqb.count, 16);
    assert_eq!(jqb.blocking, false);
    assert_eq!(jqb.queue, None);
    assert_eq!(jqb.states.len(), 0);
    jqb.count(20).blocking(true).queue(b"jqb queue").state("state1"
            ).state("state2");
    assert_eq!(jqb.count, 20);
    assert_eq!(jqb.blocking, true);
    assert_eq!(jqb.queue, Some(b"jqb queue" as &[u8]));
    assert_eq!(jqb.states, vec!["state1", "state2"]);
}
