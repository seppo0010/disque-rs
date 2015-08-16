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
