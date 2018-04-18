#[macro_use]
extern crate log;

extern crate byteorder;
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

pub mod kvs;

pub use kvs::KVS;

