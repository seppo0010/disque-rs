extern crate redis;

use std::time::Duration;

use redis::{Connection, RedisError, cmd};

fn duration_to_millis(d: &Duration) -> u64 {
    (d.subsec_nanos() / 1_000_000) as u64 + d.as_secs()
}

macro_rules! option_arg {
    ($disque: expr, $name: expr, $param: expr) => (
        match $param {
            Some(u) => $disque.arg($name).arg(u),
            None => &mut $disque,
        };
    )
}
pub struct Disque {
    connection: Connection,
}

impl Disque {
    pub fn open(url: &str) -> Result<Disque, RedisError> {
        let client = try!(redis::Client::open(url));
        let connection = try!(client.get_connection());
        Ok(Disque { connection: connection })
    }

    pub fn hello(&self) -> Result<Vec<Vec<u8>>, RedisError> {
        cmd("HELLO").query(&self.connection)
    }

    pub fn addjob(&self, queue_name: &[u8], job: &[u8], timeout: Duration,
            replicate: Option<usize>, delay: Option<Duration>,
            retry: Option<Duration>, ttl: Option<Duration>,
            maxlen: Option<usize>, async: bool,
            ) -> Result<String, RedisError> {
        let mut c = cmd("ADDJOB");
        c
            .arg(queue_name)
            .arg(job)
            .arg(duration_to_millis(&timeout));
        option_arg!(c, "REPLICATE", replicate);
        option_arg!(c, "DELAY", delay.map(|x| x.as_secs()));
        option_arg!(c, "RETRY", retry.map(|x| x.as_secs()));
        option_arg!(c, "TTL", ttl.map(|x| x.as_secs()));
        option_arg!(c, "MAXLEN", maxlen);
        if async { c.arg("ASYNC"); }
        c.query(&self.connection)
    }

    pub fn getjob_count(&self, nohang: bool, timeout: Option<Duration>,
            count: usize, withcounters: bool, queues: &[&[u8]]
            ) -> Result<Vec<Vec<Vec<u8>>>, RedisError> {
        let mut c = cmd("GETJOB");
        if nohang { c.arg("NOHANG"); }
        option_arg!(c, "TIMEOUT", timeout.map(|t| duration_to_millis(&t)));
        c.arg("COUNT").arg(count);
        if withcounters { c.arg("WITHCOUNTERS"); }
        c.arg("FROM");
        for queue in queues { c.arg(*queue); }
        c.query(&self.connection)
    }
}

#[cfg(test)]
fn conn() -> Disque {
    Disque::open("redis://127.0.0.1:7711/").unwrap()
}

#[test]
fn can_connect() {
    conn();
}

#[test]
fn hello() {
    let disque = conn();
    disque.hello().unwrap();
}

#[test]
fn addjob() {
    let disque = conn();
    let jobid = disque.addjob(b"queue", b"job", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert_eq!(jobid.len(), 48);
    assert_eq!(&jobid[..2], "DI");
    assert_eq!(&jobid[46..], "SQ");
}

#[test]
fn getjob_count() {
    let disque = conn();
    let j1 = disque.addjob(b"queue1", b"job1", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j2 = disque.addjob(b"queue2", b"job2", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    disque.addjob(b"queue3", b"job3", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let jobs = disque.getjob_count(false, None, 3, true, &[b"queue1", b"queue2"]).unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0][0], b"queue1");
    assert_eq!(jobs[0][1], j1.into_bytes());
    assert_eq!(jobs[0][2], b"job1");
    assert_eq!(jobs[1][0], b"queue2");
    assert_eq!(jobs[1][1], j2.into_bytes());
    assert_eq!(jobs[1][2], b"job2");
}
