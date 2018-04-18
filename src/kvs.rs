use std::path::PathBuf;
use std::io::{Error as IOError, ErrorKind, Read, Seek, SeekFrom, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc::channel;
use std::thread;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use num_cpus;
use record_file::RecordFile;
use sstable::SSTable;
use record::Record;

const WAL_HEADER: &[u8; 8] = b"WAL!\x01\x00\x00\x00";

const MAX_MEM_COUNT: usize = 1000;

pub struct KVS {
    db_dir: PathBuf,
    cur_sstable_num: u64,
    wal_file: RecordFile,
    sstables: BTreeSet<SSTable>,
    mem_table: BTreeMap<Vec<u8>, Record>
}

/// Gets the timestamp/epoch in ms
pub fn get_timestamp() -> u64 {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards");

    return ts.as_secs() * 1000 + ts.subsec_nanos() as u64 / 1_000_000;
}

/**
 * Files have the following meansings:
 * data.wal     - Write Ahead Log; journal of all put & deletes that are in mem_table
 * table_#.data - The various SSTables
 */

impl KVS {
    /// Creates a new KVS given a directory to store the files
    pub fn new(db_dir: &PathBuf) -> Result<KVS, IOError> {
        let wal_file_path = db_dir.join("data.wal");
        let wal_file = RecordFile::new(&wal_file_path, WAL_HEADER)?;

        let mut sstables = BTreeSet::<SSTable>::new();

        // gather up all the SSTables in this directory
        for entry in fs::read_dir(db_dir)? {
            let entry = entry.expect("Error reading directory entry");
            let path = entry.path();

            if path.is_dir() {
                continue
            }

            if path.ends_with(".data") {
                sstables.insert(SSTable::new(&path)?);
            }
        }

        return Ok(KVS {
            db_dir: PathBuf::from(db_dir),
            cur_sstable_num: 0,
            wal_file: wal_file,
            sstables: sstables,
            mem_table: BTreeMap::new()
        })
    }

    /// flush the memtable to disk
    fn flush(&mut self) -> Result<(), IOError> {
        info!("Starting a flush");

        if self.mem_table.len() == 0 {
            debug!("No records in mem_table");
            return Ok( () ); // don't need to do anything if we don't have values yet
        }

        // open a new SSTable
        let sstable_path = self.db_dir.join(format!("table_{}.data", self.cur_sstable_num));
        let mut sstable = SSTable::new(&sstable_path)?;
        self.cur_sstable_num += 1; // bump our count

        // go through the records in the mem_table, and add to file
        for (key, value) in self.mem_table.iter() {
            sstable.append(value);
        }

        // flush the table, and add it to our set
        sstable.flush().expect("Error flushing SSTable");
        self.sstables.insert(sstable);

        // remove everything in the mem_table
        self.mem_table.clear();

        debug!("MEM TABLE: {}", self.mem_table.len());

        // rename the WAL file
        fs::rename(self.db_dir.join("data.wal"), self.db_dir.join("old_data.wal"))?;

        // open a new version
        let wal_file_path = self.db_dir.join("data.wal");
        self.wal_file = RecordFile::new(&wal_file_path, WAL_HEADER)?;

        // remove the old one
        fs::remove_file(self.db_dir.join("old_data.wal"))?;

        info!("Leaving flush");

        Ok( () )
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        let cur_time = get_timestamp();

        debug!("MEM TABLE: {}", self.mem_table.len());

        // first check the mem_table
        if self.mem_table.contains_key(key) {
            let rec = self.mem_table.get(key).unwrap();

            // found an expired or deleted key
            return if rec.is_expired(cur_time) || rec.is_delete() {
                None
            } else {
                Some(rec.get_value())
            };
        }

        // need to go to SSTables
        for sstable in self.sstables.iter() {
            debug!("SSTABLE: {:?}", sstable.get_oldest_ts());

            let ret = sstable.get(key.to_vec());

            if ret.is_some() {
                return ret;
            }
        }

        None
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), IOError> {
        info!("Called put: {:?}", key);

        // create a record, and append to the WAL file
        let rec = Record::new(key.to_vec(), value);
        self.wal_file.append(&Record::serialize(&rec))?;

        // insert into the mem_table
        self.mem_table.insert(key, rec);

        // check to see if we need to flush to disk
        if self.mem_table.len() >= MAX_MEM_COUNT {
            self.flush();
        }

        Ok( () )
    }

    pub fn delete(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        None
    }
}

impl Drop for KVS {
    fn drop(&mut self) {
        self.flush();
    }
}


#[cfg(test)]
mod tests {
    use kvs::KVS;
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
    fn new() {
        let db_dir = gen_dir();
        let kvs = KVS::new(&PathBuf::from(db_dir)).unwrap();
    }

    #[test]
    fn flush() {
        let db_dir = gen_dir();
        let mut kvs = KVS::new(&PathBuf::from(db_dir)).unwrap();

        let key = "KEY".as_bytes().to_vec();
        let value = "VALUE".as_bytes().to_vec();

        kvs.put(key, value).unwrap();

        kvs.flush().unwrap();
    }

    #[test]
    fn auto_flush() {
        let db_dir = gen_dir();
        let mut kvs = KVS::new(&PathBuf::from(db_dir)).unwrap();

        for i in 0..1001 {
            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
            let key = format!("KEY_{}", rnd).as_bytes().to_vec();
            let value = rnd.as_bytes().to_vec();

            kvs.put(key, value).unwrap();
        }
    }

    #[test]
    fn get() {
        let db_dir = gen_dir();
        let mut kvs = KVS::new(&PathBuf::from(db_dir)).unwrap();

        let key = "KEY".as_bytes();
        let value = "VALUE".as_bytes();

        kvs.put(key.to_vec(), value.to_vec()).unwrap();

        kvs.flush().unwrap();

        debug!("GOT: {:?}", kvs.get(&key.to_vec()));
    }
}
