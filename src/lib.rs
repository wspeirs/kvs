//! KVS is an implementation of a Log-Structed Merge-Tree.
//! It provides a persistent key/value store.

#[macro_use]
extern crate log;

extern crate byteorder;
extern crate itertools;
extern crate lru_cache;
extern crate positioned_io;
extern crate regex;
extern crate rmp_serde as rmps;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate walkdir;


// these are for tests
#[cfg(test)] extern crate simple_logger;
#[cfg(test)] extern crate rand;

mod record_file;
mod sstable;
mod record;
mod serde_utils;

pub mod kvs;

pub use crate::kvs::{KVSOptions, KVS};

use std::mem;

const U32_SIZE :usize = mem::size_of::<u32>();
const U64_SIZE :usize = mem::size_of::<u64>();

#[cfg(test)] use std::sync::{Once, ONCE_INIT};
#[cfg(test)] static LOGGER_INIT: Once = ONCE_INIT;
