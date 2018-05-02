use rmps::encode::to_vec;

use std::cmp::Ordering;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use record_file::buf2string;

use kvs::get_timestamp;

#[derive(Serialize, Deserialize, Clone)]
pub struct Record {
    key: Vec<u8>,
    value: Option<Vec<u8>>, // if None, means we're deleting this key
    created: u64, // timestamp of when the record was created
    ttl: u64 // timestamp when this record should be deleted
}

impl Record {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Record {
        Record::new_with_ttl(key, value, u64::max_value())
    }

    pub fn new_with_ttl(key: Vec<u8>, value: Vec<u8>, ttl: u64) -> Record {
        Record {
            key: key,
            value: Some(value),
            created: get_timestamp(),
            ttl: ttl
        }
    }

    pub fn serialize(rec: &Record) -> Vec<u8> {
        return to_vec(rec).unwrap(); // should handle this better
    }

    pub fn is_expired(&self, ts: u64) -> bool {
        self.ttl <= ts
    }

    pub fn created(&self) -> u64 {
        self.created
    }

    pub fn is_delete(&self) -> bool {
        self.value.is_none()
    }

    pub fn key(&self) -> Vec<u8> {
        self.key.to_owned()
    }

    pub fn value(&self) -> Vec<u8> {
        self.value.to_owned().expect("Tried to get value of delete record")
    }
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Record) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Record) -> Ordering {
        let key_ord = self.key.cmp(&other.key);

        match key_ord {
            Ordering::Equal => self.created.cmp(&other.created),
            Ordering::Less | Ordering::Greater => key_ord
        }
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Record) -> bool {
        self.key == other.key && self.created == self.created
    }
}

impl Eq for Record { }

impl Debug for Record {
    fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
        formatter.debug_struct("Record")
            .field("key", &buf2string(&self.key))
            .field("value", &match &self.value { &None => String::from("None"), &Some(ref v) => buf2string(&v) })
            .field("created", &self.created)
            .field("ttl", &self.ttl)
            .finish()
    }
}
