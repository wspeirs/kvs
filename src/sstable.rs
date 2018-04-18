use rmps::encode::to_vec;
use rmps::decode::from_slice;

use serde::{Deserialize, Serialize};

use std::cmp::Ordering;
use std::io::{Error as IOError};
use std::path::PathBuf;

use record_file::RecordFile;
use record::Record;

const SSTABLE_HEADER: &[u8; 8] = b"DATA\x01\x00\x00\x00";


#[derive(Serialize, Deserialize, Clone)]
struct SSTableInfo {
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
    oldest_ts: u64
}

pub struct SSTable {
    rec_file: RecordFile,
    info: Option<SSTableInfo>
}

impl SSTable {
    pub fn new(file_path: &PathBuf) -> Result<SSTable, IOError> {
        let is_new = file_path.exists();

        let mut rec_file = RecordFile::new(file_path, SSTABLE_HEADER)?;

        let info = if is_new {
            Some(from_slice(&rec_file.get_last_record().expect("Error reading SSTableInfo")).expect("Error decoding SSTableInfo"))
        } else {
            None
        };

        info!("Created SSTable: {:?}", file_path);

        Ok(SSTable {
            rec_file: rec_file,
            info: info
        })
    }

    pub fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        let info : &SSTableInfo = self.info.as_ref().expect("Error trying to using newly created SSTable");

        // check if the key is in the range of this SSTable
        if key < info.smallest_key || info.largest_key < key {
            return None;
        }

        // for now, just iterate through looking for the key
        for rec_buff in self.rec_file.iter() {
            let rec :Record = from_slice(&rec_buff).expect("Error decoding Record");

            if rec.get_key() == key {
                return Some(rec.get_value())
            } else if rec.get_key() > key {
                return None
            }
        }

        None // should never get here
    }

    pub fn append(&mut self, rec: &Record) -> Result<(), IOError> {
        self.rec_file.append(&Record::serialize(rec))?; // append without flush

        let key = rec.get_key();
        let ts = rec.get_created();

        // check to see if this is a new SSTable or not
        if self.info.is_none() {
            self.info = Some(SSTableInfo {
                smallest_key: key.to_vec(),
                largest_key: key,
                oldest_ts: ts
            })
        } else {
            let info : &mut SSTableInfo = self.info.as_mut().expect("Error unwrapping some");

            // update the smallest & largest key
            if key < info.smallest_key {
                info.smallest_key = key.to_vec();
            }

            if key > info.largest_key {
                info.largest_key = key;
            }

            // update the ts if it's older
            if ts > info.oldest_ts {
                info.oldest_ts = ts;
            }
        }

        Ok( () )
    }

    pub fn flush(&mut self) -> Result<(), IOError> {
        self.rec_file.flush()
    }

    pub fn get_oldest_ts(&self) -> u64 {
        self.info.as_ref().expect("Attempting to get oldest ts from new SSTable").oldest_ts
    }
}

impl Drop for SSTable {
    fn drop(&mut self) {
        let info_buff = to_vec(&self.info.as_ref().expect("Trying to drop/close an empty SSTable")).expect("Error serializing SSTableInfo");

        self.rec_file.append_flush(&info_buff);
    }
}

impl PartialOrd for SSTable {
    fn partial_cmp(&self, other: &SSTable) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SSTable {
    fn cmp(&self, other: &SSTable) -> Ordering {
        self.info.as_ref().expect("Attempting to compare new SSTable").oldest_ts.cmp(&other.info.as_ref().expect("Attempting to compare new SSTable").oldest_ts)
    }
}

impl PartialEq for SSTable {
    fn eq(&self, other: &SSTable) -> bool {
        self.info.as_ref().expect("Attempting to eq new SSTable").oldest_ts == other.info.as_ref().expect("Attempting to eq new SSTable").oldest_ts
    }
}

impl Eq for SSTable { }


#[cfg(test)]
mod tests {
    use sstable::SSTable;
    use record::Record;
    use std::path::PathBuf;
    use std::thread;
    use std::time;
    use rand::{thread_rng, Rng};
    use std::fs::create_dir;
    use simple_logger;


    fn gen_dir() -> PathBuf {
        simple_logger::init().unwrap(); // this will panic on error

        let tmp_dir: String = thread_rng().gen_ascii_chars().take(6).collect();
        let ret_dir = PathBuf::from("/tmp").join(format!("kvs_{}", tmp_dir));

        debug!("CREATING TMP DIR: {:?}", ret_dir);

        create_dir(&ret_dir).unwrap();

        return ret_dir;
    }

    #[test]
    fn new_append() {
        let db_dir = gen_dir();
        let mut sstable = SSTable::new(&db_dir.join("test.data")).unwrap();

        let rec = Record::new(vec![1;8], vec![2;8]);

        sstable.append(&rec).unwrap();
    }
}