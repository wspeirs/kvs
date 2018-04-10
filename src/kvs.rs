
use std::path::PathBuf;
use std::io::{Error as IOError, ErrorKind, Read, Seek, SeekFrom, Write};

use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc::channel;
use std::thread;
use std::collections::BTreeMap;

use num_cpus;
use record_file::RecordFile;

const LOG_FILE_HEADER: &[u8; 8] = b"LOG!\x01\x00\x00\x00";

pub struct KVS {
    db_dir: PathBuf,
    log_file: RecordFile,
    mem_table: BTreeMap<Vec<u8>, Vec<u8>>
}

impl KVS {
    pub fn new(db_dir: &PathBuf) -> Result<KVS, IOError> {
        let log_file_path = db_dir.join("logs.data");
        let log_file = RecordFile::new(&log_file_path, LOG_FILE_HEADER)?;

        return Ok(KVS {
            db_dir: PathBuf::from(db_dir),
            log_file: log_file,
            mem_table: BTreeMap::new()
        })
    }

    pub fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        None
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        None
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

    #[test]
    fn it_works() {
        let kvs = KVS::new(&PathBuf::from("/tmp/")).unwrap();

        thread::sleep(time::Duration::from_secs(3));

    }
}
