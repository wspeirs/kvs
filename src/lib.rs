#[macro_use]
extern crate log;

extern crate byteorder;
extern crate itertools;
extern crate num_cpus;
extern crate positioned_io;
extern crate rmp_serde as rmps;
extern crate serde;
#[macro_use]
extern crate serde_derive;


// these are for tests
#[cfg(test)] extern crate simple_logger;
#[cfg(test)] extern crate rand;

mod record_file;
mod sstable;
mod record;
mod serde_utils;

pub mod kvs;

pub use kvs::KVS;

use std::mem;

const U32_SIZE :usize = mem::size_of::<u32>();
const U64_SIZE :usize = mem::size_of::<u64>();

