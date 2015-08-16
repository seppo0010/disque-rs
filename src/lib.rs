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
//!
//! ## Command reference
//!
//! The commands are a direct implementation of Disque commands. To read a
//! reference about their meaning, go to https://github.com/antirez/disque

#![crate_name = "disque"]
#![crate_type = "lib"]

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
    /// Opens a new connection to a Disque server.
    ///
    /// # Examples
    /// ```
    /// # use disque::Disque;
    /// let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    /// ```
    pub fn open<T: IntoConnectionInfo>(params: T) -> RedisResult<Disque> {
        let client = try!(redis::Client::open(params));
        let connection = try!(client.get_connection());
        Ok(Disque { connection: connection })
    }

    /// The hello command returns information about the disque cluster.
    ///
    /// # Examples
    /// ```
    /// # use disque::Disque;
    /// let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    /// let (_, nodeid, _) = disque.hello().unwrap();
    /// println!("Connected to node {}", nodeid);
    /// ```
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

    /// Adds a job to a queue.
    ///
    /// # Examples
    /// ```
    /// # use disque::Disque;
    /// # use std::time::Duration;
    /// let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    /// let jobid = disque.addjob(b"my queue", b"my job",
    ///   Duration::from_secs(10), None, None, None, None, None, false
    ///   ).unwrap();
    /// println!("My job id is {}", jobid);
    /// ```
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

    /// Gets up to `count` jobs from certain `queues`.
    ///
    /// # Examples
    /// ```
    /// # use disque::Disque;
    /// # use std::time::Duration;
    /// let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    /// let queue = b"my getjob_count queue";
    /// disque.addjob(queue, b"my job 1", Duration::from_secs(10),
    ///   None, None, None, None, None, false
    ///   ).unwrap();
    /// disque.addjob(queue, b"my job 2", Duration::from_secs(10),
    ///   None, None, None, None, None, false
    ///   ).unwrap();
    ///
    /// let jobs = disque.getjob_count(true, None, 10, &[queue]).unwrap();
    /// assert_eq!(jobs.len(), 2);
    /// assert_eq!(jobs[0].2, b"my job 1");
    /// assert_eq!(jobs[1].2, b"my job 2");
    /// ```
    pub fn getjob_count(&self, nohang: bool, timeout: Option<Duration>,
            count: usize, queues: &[&[u8]]
            ) -> RedisResult<Vec<(Vec<u8>, String, Vec<u8>)>> {
        let mut c = cmd("GETJOB");
        if nohang { c.arg("NOHANG"); }
        option_arg!(c, "TIMEOUT", timeout.map(|t| duration_to_millis(&t)));
        c.arg("COUNT").arg(count);
        c.arg("FROM");
        for queue in queues { c.arg(*queue); }
        let v:Vec<Vec<Vec<u8>>> = try!(c.query(&self.connection));
        let mut r = vec![];
        for mut x in v.into_iter() {
            if x.len() != 3 {
                return Err(RedisError::from((ErrorKind::TypeError,
                                "Expected exactly three elements")));
            }
            let job = x.pop().unwrap();
            let jobid = x.pop().unwrap();
            let queue = x.pop().unwrap();
            r.push((queue, match String::from_utf8(jobid) {
                Ok(v) => v,
                Err(_) => return Err(RedisError::from((ErrorKind::TypeError,
                                "Expected utf8 job id"))),
            }, job));
        }
        Ok(r)

        // Ok(v.into_iter().map(|x| (x[0], String::from_utf8(x[1]).unwrap(), x[2])).collect())
    }

    pub fn getjob_count_withcounters(&self, nohang: bool, timeout: Option<Duration>,
            count: usize, queues: &[&[u8]]
            ) -> RedisResult<Vec<(Vec<u8>, String, Vec<u8>, u32, u32)>> {
        let mut c = cmd("GETJOB");
        if nohang { c.arg("NOHANG"); }
        option_arg!(c, "TIMEOUT", timeout.map(|t| duration_to_millis(&t)));
        c.arg("COUNT").arg(count);
        c.arg("WITHCOUNTERS");
        c.arg("FROM");
        for queue in queues { c.arg(*queue); }
        let v:Vec<Vec<Value>> = try!(c.query(&self.connection));
        let mut r = vec![];
        for mut x in v.into_iter() {
            if x.len() != 7 {
                return Err(RedisError::from((ErrorKind::TypeError,
                                "Expected exactly three elements")));
            }
            let additional_deliveries:u32 = try!(u32::from_redis_value(&x.pop().unwrap()));
            let _ = x.pop().unwrap();
            let nack:u32 = try!(u32::from_redis_value(&x.pop().unwrap()));
            let _ = x.pop().unwrap();
            let job:Vec<u8> = try!(Vec::from_redis_value(&x.pop().unwrap()));
            let jobid:String = try!(String::from_redis_value(&x.pop().unwrap()));
            let queue:Vec<u8> = try!(Vec::from_redis_value(&x.pop().unwrap()));
            r.push((queue, jobid, job, nack, additional_deliveries));
        }
        Ok(r)
    }

    /// Gets a single job from any of the specified `queues`.
    pub fn getjob(&self, nohang: bool, timeout: Option<Duration>,
            queues: &[&[u8]]
            ) -> RedisResult<Option<(Vec<u8>, String, Vec<u8>)>> {
        let mut jobs = try!(self.getjob_count(nohang, timeout, 1, queues));
        Ok(jobs.pop())
    }

    /// Gets a single job from any of the specified `queues` with its nack and
    /// additional deliveries count.
    pub fn getjob_withcounters(&self, nohang: bool, timeout: Option<Duration>,
            queues: &[&[u8]]
            ) -> RedisResult<Option<(Vec<u8>, String, Vec<u8>, u32, u32)>> {
        let mut jobs = try!(self.getjob_count_withcounters(nohang, timeout, 1, queues));
        Ok(jobs.pop())
    }

    /// Acknowledge jobs.
    pub fn ackjob(&self, jobids: &[&[u8]]) -> RedisResult<bool> {
        let mut c = cmd("ACKJOB");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    /// Fast acknowledge jobs.
    pub fn fastack(&self, jobids: &[&[u8]]) -> RedisResult<usize> {
        let mut c = cmd("FASTACK");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    /// Tell Disque that a job is still processed.
    pub fn working(&self, jobid: &[u8]) -> RedisResult<Duration> {
        let retry = try!(cmd("WORKING").arg(jobid).query(&self.connection));
        Ok(Duration::from_secs(retry))
    }

    /// Tells Disque to put back the job in the queue ASAP. Should be used when
    /// the worker was not able to process a message and wants the message to
    /// be put back into the queue in order to be processed again.
    pub fn nack(&self, jobids: &[&[u8]]) -> RedisResult<usize> {
        let mut c = cmd("NACK");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    /// Information about the server
    pub fn info(&self) -> RedisResult<InfoDict> {
        cmd("INFO").query(&self.connection)
    }

    /// Size of the queue
    pub fn qlen(&self, queue_name: &[u8]) -> RedisResult<usize> {
        cmd("QLEN").arg(queue_name).query(&self.connection)
    }

    /// Gets jobs from `queue_name` up to the absolute number of `count`.
    /// If count is negative, it will be from newest to oldest.
    pub fn qpeek(&self, queue_name: &[u8], count: i64
            ) -> RedisResult<Vec<Vec<Vec<u8>>>> {
        cmd("QPEEK").arg(queue_name).arg(count).query(&self.connection)
    }

    /// Queue jobs
    pub fn enqueue(&self, jobids: &[&[u8]]) -> RedisResult<usize> {
        let mut c = cmd("ENQUEUE");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    /// Remove jobs from queue
    pub fn dequeue(&self, jobids: &[&[u8]]) -> RedisResult<usize> {
        let mut c = cmd("DEQUEUE");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    /// Completely delete a job from a single node.
    pub fn deljob(&self, jobids: &[&[u8]]) -> RedisResult<usize> {
        let mut c = cmd("DELJOB");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    /// Returns full information about a job, like its current state and data.
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

    /// Iterator to run all queues that fulfil a criteria.
    /// The iterator will batch into segments of approximate `count` size.
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

    /// Iterator to run all jobs that fulfil a criteria.
    /// The iterator will batch into segments of approximate `count` size.
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
    let jobs = disque.getjob_count(false, None, 3, &[b"queue1", b"queue2"]).unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].0, b"queue1");
    assert_eq!(jobs[0].1, j1);
    assert_eq!(jobs[0].2, b"job1");
    assert_eq!(jobs[1].0, b"queue2");
    assert_eq!(jobs[1].1, j2);
    assert_eq!(jobs[1].2, b"job2");
}

#[test]
fn getjob_count_withcounters() {
    let disque = conn();
    let j1 = disque.addjob(b"queue18", b"job1", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j2 = disque.addjob(b"queue18", b"job2", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert_eq!(disque.getjob_count(false, None, 100, &[b"queue18"]).unwrap().len(), 2);
    assert_eq!(disque.nack(&[j1.as_bytes(), j2.as_bytes()]).unwrap(), 2);
    assert_eq!(disque.getjob_count(false, None, 100, &[b"queue18"]).unwrap().len(), 2);
    assert_eq!(disque.nack(&[j1.as_bytes(), j2.as_bytes()]).unwrap(), 2);
    assert_eq!(disque.getjob_count(false, None, 100, &[b"queue18"]).unwrap().len(), 2);
    assert_eq!(disque.nack(&[j1.as_bytes()]).unwrap(), 1);
    let jobs = disque.getjob_count_withcounters(false, None, 3, &[b"queue18"]).unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].0, b"queue18");
    assert_eq!(jobs[0].1, j1);
    assert_eq!(jobs[0].2, b"job1");
    assert_eq!(jobs[0].3, 3);
    assert_eq!(jobs[0].4, 0);
}

#[test]
fn getjob_withcounters() {
    let disque = conn();
    let jobid = disque.addjob(b"queue19", b"job1", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert_eq!(disque.getjob_count(false, None, 100, &[b"queue19"]).unwrap().len(), 1);
    assert_eq!(disque.nack(&[jobid.as_bytes()]).unwrap(), 1);
    assert_eq!(disque.getjob_count(false, None, 100, &[b"queue19"]).unwrap().len(), 1);
    assert_eq!(disque.nack(&[jobid.as_bytes()]).unwrap(), 1);
    assert_eq!(disque.getjob_count(false, None, 100, &[b"queue19"]).unwrap().len(), 1);
    assert_eq!(disque.nack(&[jobid.as_bytes()]).unwrap(), 1);
    let job = disque.getjob_withcounters(false, None, &[b"queue19"]).unwrap().unwrap();
    assert_eq!(job.0, b"queue19");
    assert_eq!(job.1, jobid);
    assert_eq!(job.2, b"job1");
    assert_eq!(job.3, 3);
    assert_eq!(job.4, 0);
}

#[test]
fn getjob() {
    let disque = conn();
    let jobid = disque.addjob(b"queue4", b"job4", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let job = disque.getjob(false, None, &[b"queue4", b"queue5"]).unwrap().unwrap();
    assert_eq!(job.0, b"queue4");
    assert_eq!(job.1, jobid);
    assert_eq!(job.2, b"job4");
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
    assert_eq!(disque.getjob_count(false, None, 100, &[b"queue9"]).unwrap().len(), 3);
    assert_eq!(disque.nack(&[j1.as_bytes(), j2.as_bytes(), j3.as_bytes()]).unwrap(), 3);
    assert_eq!(disque.getjob_count(false, None, 100, &[b"queue9"]).unwrap().len(), 3);
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
    assert_eq!(disque.getjob_count(false, None, 100, &[b"queue10"]).unwrap().len(), 3);
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
    assert_eq!(disque.getjob_count(false, None, 100, &[b"queue11"]).unwrap().len(), 2);
}

#[test]
fn enqueue() {
    let disque = conn();
    let j1 = disque.addjob(b"queue12", b"job12.1", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j2 = disque.addjob(b"queue12", b"job12.2", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j3 = disque.addjob(b"queue12", b"job12.3", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert_eq!(disque.getjob_count(false, None, 100, &[b"queue12"]).unwrap().len(), 3);
    assert_eq!(disque.enqueue(&[j1.as_bytes(), j2.as_bytes(), j3.as_bytes()]).unwrap(), 3);
    assert_eq!(disque.getjob_count(false, None, 100, &[b"queue12"]).unwrap().len(), 3);
}

#[test]
fn dequeue() {
    let disque = conn();
    let j1 = disque.addjob(b"queue13", b"job13.1", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j2 = disque.addjob(b"queue13", b"job13.2", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j3 = disque.addjob(b"queue13", b"job13.3", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert_eq!(disque.dequeue(&[j1.as_bytes(), j2.as_bytes(), j3.as_bytes()]).unwrap(), 3);
    assert_eq!(disque.getjob_count(true, None, 100, &[b"queue13"]).unwrap().len(), 0);
}

#[test]
fn deljob() {
    let disque = conn();
    let j1 = disque.addjob(b"queue14", b"job14.1", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let j2 = disque.addjob(b"queue14", b"job14.2", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    disque.addjob(b"queue14", b"job14.3", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    assert_eq!(disque.deljob(&[j1.as_bytes(), j2.as_bytes()]).unwrap(), 2);
    assert_eq!(disque.getjob_count(true, None, 100, &[b"queue14"]).unwrap().len(), 1);
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
