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

    pub fn getjob(&self, nohang: bool, timeout: Option<Duration>,
            withcounters: bool, queues: &[&[u8]]
            ) -> Result<Option<Vec<Vec<u8>>>, RedisError> {
        let mut jobs = try!(self.getjob_count(nohang, timeout, 1, withcounters,
                    queues));
        Ok(jobs.pop())
    }

    pub fn ackjob(&self, jobids: &[&[u8]]) -> Result<bool, RedisError> {
        let mut c = cmd("ACKJOB");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    pub fn fastack(&self, jobids: &[&[u8]]) -> Result<usize, RedisError> {
        let mut c = cmd("FASTACK");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    pub fn working(&self, jobid: &[u8]) -> Result<Duration, RedisError> {
        let retry = try!(cmd("WORKING").arg(jobid).query(&self.connection));
        Ok(Duration::from_secs(retry))
    }

    pub fn nack(&self, jobids: &[&[u8]]) -> Result<usize, RedisError> {
        let mut c = cmd("NACK");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    pub fn info(&self) -> Result<Vec<u8>, RedisError> {
        cmd("INFO").query(&self.connection)
    }

    pub fn qlen(&self, queue_name: &[u8]) -> Result<usize, RedisError> {
        cmd("QLEN").arg(queue_name).query(&self.connection)
    }

    pub fn qpeek(&self, queue_name: &[u8], count: i64) -> Result<Vec<Vec<Vec<u8>>>, RedisError> {
        cmd("QPEEK").arg(queue_name).arg(count).query(&self.connection)
    }

    pub fn enqueue(&self, jobids: &[&[u8]]) -> Result<usize, RedisError> {
        let mut c = cmd("ENQUEUE");
        for jobid in jobids { c.arg(*jobid); }
        c.query(&self.connection)
    }

    pub fn dequeue(&self, jobids: &[&[u8]]) -> Result<usize, RedisError> {
        let mut c = cmd("DEQUEUE");
        for jobid in jobids { c.arg(*jobid); }
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
    assert!(!disque.ackjob(&[jobid.as_bytes()]).unwrap());
    assert!(!disque.ackjob(&[jobid.as_bytes()]).unwrap());
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
    disque.info().unwrap();
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
