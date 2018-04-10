#[macro_use]
extern crate log;

extern crate byteorder;
extern crate num_cpus;
extern crate positioned_io;

pub mod kvs;
mod record_file;

pub use kvs::KVS;


