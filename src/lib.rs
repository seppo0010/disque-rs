//! disque-rs is a rust implementation of a Disque client library.
//! It uses redis-rs to handle the connection and low level protocol.
//!
//! The crate is called `disque` and you can depend on it via cargo:
//!
//! ``ini
//! [dependencies.disque]
//! version = "*"
//! ```
//!
//! ## Connection Parameters
//!
//! disque-rs knows different ways to define where a connection should
//! go. The parameter to `Disque::open` needs to implement the
//! `IntoConnectionInfo` trait of which there are three implementations:
//!
//! * string slices in `redis://` URL format.
//! * URL objects from the redis-url crate.
//! * `ConnectionInfo` objects.
//!
//! The URL format is `redis://[:<passwd>@]<hostname>[:port][/<db>]`
//!
//! Notice the scheme is actually "redis" because it uses the Redis protocol.
//! By default, it will use port 6379, although Disque uses 7711.
//!
//! ## Unix Sockets
//!
//! For unix socket support, install `redis` with the feature "unix_socket".

extern crate redis;

use std::collections::HashMap;
use std::time::Duration;

use redis::{Connection, RedisError, cmd, Value, ErrorKind, FromRedisValue,
    IntoConnectionInfo, Iter, RedisResult, InfoDict};

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
    pub fn open<T: IntoConnectionInfo>(params: T) -> RedisResult<Disque> {
        let client = try!(redis::Client::open(params));
        let connection = try!(client.get_connection());
        Ok(Disque { connection: connection })
    }

    pub fn hello(&self) -> RedisResult<(u8, String, Vec<(String, String, u16, u32)>)> {
        let mut items = match try!(cmd("HELLO").query(&self.connection)) {
            Value::Bulk(items) => items,
            _ => return Err(RedisError::from((ErrorKind::TypeError,
                            "Expected multi-bulk"))),
        };
        if items.len() != 3 {
            return Err(RedisError::from((ErrorKind::TypeError,
                            "Expected multi-bulk with size 3")));
        }
        let nodes = try!(Vec::from_redis_value(&items.pop().unwrap()));
        let nodeid = try!(String::from_redis_value(&items.pop().unwrap()));
        let hellov = try!(u8::from_redis_value(&items.pop().unwrap()));
        Ok((hellov, nodeid, nodes))
    }

    pub fn addjob(&self, queue_name: &[u8], job: &[u8], timeout: Duration,
            replicate: Option<usize>, delay: Option<Duration>,
            retry: Option<Duration>, ttl: Option<Duration>,
            maxlen: Option<usize>, async: bool,
            ) -> RedisResult<String> {
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
            ) -> RedisResult<Vec<Vec<Vec<u8>>>> {
        let mut c = cmd("GETJOB");
        if nohang { c.arg("NOHANG"); }
        option_arg!(c, "TIMEOUT", timeout.map(|t| duration_to_millis(&t)));
        c.arg("COUNT").arg(count);
        if withcounters { c.arg("WITHCOUNTERS"); }
        c.arg("FROM");
        for queue in queues { c.arg(*queue); }
        c.query(&self.connection)
    }

    pub fn getjob(&self, nohang: bool, timeout: Option<Duration>,
            withcounters: bool, queues: &[&[u8]]
            ) -> RedisResult<Option<Vec<Vec<u8>>>> {
        let mut jobs = try!(self.getjob_count(nohang, timeout, 1, withcounters,
                    queues));
        Ok(jobs.pop())
    }

    pub fn ackjob(&self, jobids: &[&[u8]]) -> RedisResult<bool> {
        let mut c = cmd("ACKJOB");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    pub fn fastack(&self, jobids: &[&[u8]]) -> RedisResult<usize> {
        let mut c = cmd("FASTACK");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    pub fn working(&self, jobid: &[u8]) -> RedisResult<Duration> {
        let retry = try!(cmd("WORKING").arg(jobid).query(&self.connection));
        Ok(Duration::from_secs(retry))
    }

    pub fn nack(&self, jobids: &[&[u8]]) -> RedisResult<usize> {
        let mut c = cmd("NACK");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    pub fn info(&self) -> RedisResult<InfoDict> {
        cmd("INFO").query(&self.connection)
    }

    pub fn qlen(&self, queue_name: &[u8]) -> RedisResult<usize> {
        cmd("QLEN").arg(queue_name).query(&self.connection)
    }

    pub fn qpeek(&self, queue_name: &[u8], count: i64
            ) -> RedisResult<Vec<Vec<Vec<u8>>>> {
        cmd("QPEEK").arg(queue_name).arg(count).query(&self.connection)
    }

    pub fn enqueue(&self, jobids: &[&[u8]]) -> RedisResult<usize> {
        let mut c = cmd("ENQUEUE");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    pub fn dequeue(&self, jobids: &[&[u8]]) -> RedisResult<usize> {
        let mut c = cmd("DEQUEUE");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    pub fn deljob(&self, jobids: &[&[u8]]) -> RedisResult<usize> {
        let mut c = cmd("DELJOB");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    pub fn show(&self, jobid: &[u8]) -> RedisResult<HashMap<String, Value>> {
        let info:Value = try!(cmd("SHOW").arg(jobid).query(&self.connection));
        let mut h = HashMap::new();
        let mut items = match info {
            Value::Bulk(items) => items,
            _ => return Err(RedisError::from((ErrorKind::TypeError,
                            "Expected multi-bulk"))),
        };
        if items.len() % 2 != 0 {
            return Err(RedisError::from((ErrorKind::TypeError,
                            "Expected an even number of elements")));
        }
        while items.len() > 0 {
            let value = items.pop().unwrap();
            let key:String = try!(String::from_redis_value(&items.pop().unwrap()));
            h.insert(key, value);
        }
        Ok(h)
    }

    pub fn qscan(&self, cursor: u64, count: u64, busyloop: bool,
            minlen: Option<u64>, maxlen: Option<u64>, importrate: Option<u64>
            ) -> RedisResult<Iter<Vec<u8>>> {
        let mut c = cmd("QSCAN");
        c.arg("COUNT").arg(count);
        if busyloop { c.arg("BUSYLOOP"); }
        option_arg!(c, "MINLEN", minlen);
        option_arg!(c, "MAXLEN", maxlen);
        option_arg!(c, "IMPORTRATE", importrate);
        c.cursor_arg(cursor).iter(&self.connection)
    }

    pub fn jscan_id(&self, cursor: u64, count: u64, blocking: bool,
            queue: Option<&[u8]>, states: &[&str]
            ) -> RedisResult<Iter<String>> {
        let mut c = cmd("JSCAN");
        c.arg("COUNT").arg(count);
        if blocking { c.arg("BLOCKING"); }
        option_arg!(c, "QUEUE", queue);
        for state in states {
            c.arg("STATE").arg(*state);
        }
        c.cursor_arg(cursor).iter(&self.connection)
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
    let (v, nodeid, nodes) = disque.hello().unwrap();
    assert_eq!(v, 1);
    assert!(nodes.into_iter().map(|n| n.0).collect::<Vec<_>>().contains(&nodeid));
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

#[test]
fn getjob() {
    let disque = conn();
    let jobid = disque.addjob(b"queue4", b"job4", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let job = disque.getjob(false, None, true, &[b"queue4", b"queue5"]).unwrap().unwrap();
    assert_eq!(job[0], b"queue4");
    assert_eq!(job[1], jobid.into_bytes());
    assert_eq!(job[2], b"job4");
}

#[test]
fn ackjob() {
    let disque = conn();
    let jobid = disque.addjob(b"queue6", b"job6", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert!(disque.ackjob(&[jobid.as_bytes()]).unwrap());
    // FIXME: crashes disque-server, see https://github.com/antirez/disque/issues/113
    // assert!(!disque.ackjob(&[jobid.as_bytes()]).unwrap());
    // assert!(!disque.ackjob(&[jobid.as_bytes()]).unwrap());
}

#[test]
fn fastack() {
    let disque = conn();
    let jobid = disque.addjob(b"queue7", b"job7", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert!(disque.fastack(&[jobid.as_bytes()]).unwrap() == 1);
    assert!(disque.fastack(&[jobid.as_bytes()]).unwrap() == 0);
    assert!(disque.fastack(&[jobid.as_bytes()]).unwrap() == 0);
}

#[test]
fn working() {
    let disque = conn();
    let jobid = disque.addjob(b"queue8", b"job8", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert!(disque.working(jobid.as_bytes()).unwrap().as_secs() > 0);
}

#[test]
fn nack() {
    let disque = conn();
    let j1 = disque.addjob(b"queue9", b"job9.1", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j2 = disque.addjob(b"queue9", b"job9.2", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j3 = disque.addjob(b"queue9", b"job9.3", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert_eq!(disque.getjob_count(false, None, 100, true, &[b"queue9"]).unwrap().len(), 3);
    assert_eq!(disque.nack(&[j1.as_bytes(), j2.as_bytes(), j3.as_bytes()]).unwrap(), 3);
    assert_eq!(disque.getjob_count(false, None, 100, true, &[b"queue9"]).unwrap().len(), 3);
}

#[test]
fn info() {
    let disque = conn();
    let info = disque.info().unwrap();
    let _:String = info.get("disque_version").unwrap();
}

#[test]
fn qlen() {
    let disque = conn();
    disque.addjob(b"queue10", b"job10", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    disque.addjob(b"queue10", b"job10", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    disque.addjob(b"queue10", b"job10", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert_eq!(disque.qlen(b"queue10").unwrap(), 3);
    assert_eq!(disque.getjob_count(false, None, 100, true, &[b"queue10"]).unwrap().len(), 3);
}

#[test]
fn qpeek() {
    let disque = conn();
    let j1 = disque.addjob(b"queue11", b"job11.1", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j2 = disque.addjob(b"queue11", b"job11.2", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert_eq!(disque.qpeek(b"queue11", 10).unwrap(), vec![
                vec![
                b"queue11".to_vec(),
                j1.as_bytes().to_vec(),
                b"job11.1".to_vec(),
                ],
                vec![
                b"queue11".to_vec(),
                j2.as_bytes().to_vec(),
                b"job11.2".to_vec(),
                ],
            ]);
    assert_eq!(disque.qpeek(b"queue11", -10).unwrap(), vec![
                vec![
                b"queue11".to_vec(),
                j2.as_bytes().to_vec(),
                b"job11.2".to_vec(),
                ],
                vec![
                b"queue11".to_vec(),
                j1.as_bytes().to_vec(),
                b"job11.1".to_vec(),
                ],
            ]);
    assert_eq!(disque.getjob_count(false, None, 100, true, &[b"queue11"]).unwrap().len(), 2);
}

#[test]
fn enqueue() {
    let disque = conn();
    let j1 = disque.addjob(b"queue12", b"job12.1", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j2 = disque.addjob(b"queue12", b"job12.2", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j3 = disque.addjob(b"queue12", b"job12.3", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert_eq!(disque.getjob_count(false, None, 100, true, &[b"queue12"]).unwrap().len(), 3);
    assert_eq!(disque.enqueue(&[j1.as_bytes(), j2.as_bytes(), j3.as_bytes()]).unwrap(), 3);
    assert_eq!(disque.getjob_count(false, None, 100, true, &[b"queue12"]).unwrap().len(), 3);
}

#[test]
fn dequeue() {
    let disque = conn();
    let j1 = disque.addjob(b"queue13", b"job13.1", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j2 = disque.addjob(b"queue13", b"job13.2", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j3 = disque.addjob(b"queue13", b"job13.3", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert_eq!(disque.dequeue(&[j1.as_bytes(), j2.as_bytes(), j3.as_bytes()]).unwrap(), 3);
    assert_eq!(disque.getjob_count(true, None, 100, true, &[b"queue13"]).unwrap().len(), 0);
}

#[test]
fn deljob() {
    let disque = conn();
    let j1 = disque.addjob(b"queue14", b"job14.1", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j2 = disque.addjob(b"queue14", b"job14.2", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    disque.addjob(b"queue14", b"job14.3", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert_eq!(disque.deljob(&[j1.as_bytes(), j2.as_bytes()]).unwrap(), 2);
    assert_eq!(disque.getjob_count(true, None, 100, true, &[b"queue14"]).unwrap().len(), 1);
}

#[test]
fn show() {
    let disque = conn();
    let jobid = disque.addjob(b"queue15", b"job15", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let info = disque.show(jobid.as_bytes()).unwrap();
    assert_eq!(info.get("id").unwrap(), &Value::Data(jobid.as_bytes().to_vec()));
    assert_eq!(info.get("queue").unwrap(), &Value::Data(b"queue15".to_vec()));
    assert_eq!(info.get("state").unwrap(), &Value::Data(b"queued".to_vec()));
}

#[test]
fn qscan() {
    let disque = conn();
    disque.addjob(b"queue16", b"job16", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let queues = disque.qscan(0, 1000, false, None, None, None).unwrap().collect::<Vec<_>>();
    assert!(queues.contains(&b"queue16".to_vec()));
}

#[test]
fn jscan_id() {
    let disque = conn();
    let job = disque.addjob(b"queue17", b"job17", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert!(disque.jscan_id(0, 1000, false, None, &[]).unwrap().collect::<Vec<_>>().contains(&job));
    assert!(!disque.jscan_id(0, 1000, false, Some(b"queue16"), &[]).unwrap().collect::<Vec<_>>().contains(&job));
    assert!(disque.jscan_id(0, 1000, false, Some(b"queue17"), &[]).unwrap().collect::<Vec<_>>().contains(&job));
}
