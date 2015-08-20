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

mod disque;
mod builders;

pub use builders::*;
pub use disque::*;
