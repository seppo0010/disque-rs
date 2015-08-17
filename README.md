# disque-rs

[![Build Status](https://travis-ci.org/seppo0010/disque-rs.svg?branch=master)](https://travis-ci.org/seppo0010/disque-rs)
[![crates.io](http://meritbadge.herokuapp.com/disque)](https://crates.io/crates/disque)

disque-rs is a rust implementation of a Disque client library.

The crate is called `disque` and you can depend on it via cargo:

```toml
[dependencies]
disque = "0.2.0"
```

It currently requires Rust Beta or Nightly.

## Basic Operation

```rust
extern crate disque;

use disque::Disque;
use std::time::Duration;

fn main() {
    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    let jobid = disque.addjob(b"my queue", b"my job",
            Duration::from_secs(10), None, None, None, None, None, false
            ).unwrap();
    let jobs = disque.getjob_count(true, None, 10, &[b"my queue"]).unwrap();
}
```

## Documentation

For a more comprehensive documentation with all the available functions and
parameters go to http://seppo0010.github.io/disque-rs/

For a complete reference on Disque, check out https://github.com/antirez/disque
