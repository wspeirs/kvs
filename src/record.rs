use rmps::encode::to_vec;
use rmps::decode::{from_slice, from_read};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct Record {
    key: Vec<u8>,
    value: Option<Vec<u8>>, // if None, means we're deleting this key
    ttl: u64 // timestamp when this record should be deleted; 0 = never delete
}

impl Record {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Record {
        Record::new_with_ttl(key, value, 0)
    }

    pub fn new_with_ttl(key: Vec<u8>, value: Vec<u8>, ttl: u64) -> Record {
        Record {key: key, value: Some(value), ttl: ttl}
    }

    pub fn serialize(rec: &Record) -> Vec<u8> {
        return to_vec(rec).unwrap(); // should handle this better
    }
}
