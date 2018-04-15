
use std::path::PathBuf;
use std::io::{Error as IOError, ErrorKind, Read, Seek, SeekFrom, Write};

use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc::channel;
use std::thread;
use std::collections::BTreeMap;

use num_cpus;
use record_file::RecordFile;
use record::Record;

const WAL_HEADER: &[u8; 8] = b"WAL!\x01\x00\x00\x00";
const SSTABLE_HEADER: &[u8; 8] = b"DATA\x01\x00\x00\x00";

pub struct KVS {
    db_dir: PathBuf,
    cur_sstable_num: u64,
    log_file: RecordFile,
    mem_table: BTreeMap<Vec<u8>, Record>
}

/**
 * Files have the following meansings:
 * data.wal     - Write Ahead Log; journal of all put & deletes that are in mem_table
 * table_#.data - The various SSTables; highest numbers = newest file.
 */

impl KVS {
    /// Creates a new KVS given a directory to store the files
    pub fn new(db_dir: &PathBuf) -> Result<KVS, IOError> {
        let log_file_path = db_dir.join("data.wal");
        let log_file = RecordFile::new(&log_file_path, WAL_HEADER)?;

        return Ok(KVS {
            db_dir: PathBuf::from(db_dir),
            cur_sstable_num: 0,
            log_file: log_file,
            mem_table: BTreeMap::new()
        })
    }

    /// Attempts to read a value for a given key from the SSTables on disk
    fn get_from_disk(&mut self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, IOError> {
        Ok(None)
    }

    /// flush the memtable to disk
    fn flush(&mut self) -> Result<(), IOError> {
        if self.mem_table.len() == 0 {
            debug!("No records in mem_table");
            return Ok( () ); // don't need to do anything if we don't have values yet
        }

        // open a new SSTable
        let sstable_path = self.db_dir.join(format!("table_{}.data", self.cur_sstable_num));
        let mut sstable = RecordFile::new(&sstable_path, SSTABLE_HEADER)?;
        self.cur_sstable_num += 1; // bump our count

        // go through the records in the mem_table, and add to file
        for (key, value) in self.mem_table.iter() {
            sstable.append(&Record::serialize(value));
        }

        Ok( () )
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        None
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), IOError> {
        // create a record, and append to the WAL file
        let rec = Record::new(key.to_vec(), value);
        self.log_file.append(&Record::serialize(&rec))?;

        // insert into the mem_table
        self.mem_table.insert(key, rec);

        Ok( () )
    }

    pub fn delete(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        None
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

        let tmp_dir: String = thread_rng().gen_ascii_chars().take(10).collect();
        let ret_dir = PathBuf::from("/tmp").join(tmp_dir);

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

        kvs.put(key, value);

        kvs.flush();
    }
}
