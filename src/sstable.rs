use rmps::encode::to_vec;
use rmps::decode::from_slice;

use serde::{Deserialize, Serialize};

use std::cmp::Ordering;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::io::{Error as IOError, ErrorKind};
use std::path::PathBuf;

use std::iter::IntoIterator;
use std::borrow::Borrow;

use record_file::buf2string;
use record_file::RecordFile;
use record::Record;

const SSTABLE_HEADER: &[u8; 8] = b"DATA\x01\x00\x00\x00";


#[derive(Serialize, Deserialize, Clone)]
struct SSTableInfo {
    indices: Vec<u64>,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
    oldest_ts: u64
}

pub struct SSTable {
    rec_file: RecordFile,
    info: SSTableInfo
}

impl SSTable {
    pub fn open(file_path: &PathBuf) -> Result<SSTable, IOError> {
        if !file_path.exists() {
            return Err(IOError::new(ErrorKind::NotFound, format!("The SSTable {:?} was not found", file_path)));
        }

        let mut rec_file = RecordFile::new(file_path, SSTABLE_HEADER)?;

        let info = from_slice(&rec_file.get_last_record().expect("Error reading SSTableInfo")).expect("Error decoding SSTableInfo");

        info!("Created SSTable: {:?}", file_path);

        Ok(SSTable {
            rec_file: rec_file,
            info: info
        })
    }

    /// Creates a new `SSTable` that is immutable once returned.
    /// * file_path - the path to the SSTable to create
    /// * records - an iterator to records that will be inserted into this `SSTable`
    /// * index_count - the number of records to group together for each recorded index
    /// * count - the number of records to pull from the iterator and put into the `SSTable`
    pub fn new<I, B>(file_path: &PathBuf,  records: &mut I, index_count: u32, count: Option<u64>) -> Result<SSTable, IOError>
        where I: Iterator<Item=B>, B: Borrow<Record>
    {
        if file_path.exists() {
            return Err(IOError::new(ErrorKind::AlreadyExists, format!("The SSTable {:?} already exists", file_path)));
        }

        let rec_file = RecordFile::new(file_path, SSTABLE_HEADER)?;

        info!("Created SSTable: {:?}", file_path);

        let mut sstable_info = SSTableInfo { indices: vec!(), smallest_key: vec!(), largest_key: vec!(), oldest_ts: 0 };
        let rec_opt = records.next();

        if rec_opt.is_none() {
            panic!("Attempted to create SSTable with empty iterator");
        }

        let rec = rec_opt.unwrap();

        // setup our info for this table
        sstable_info.smallest_key = rec.borrow().get_key();
        sstable_info.largest_key = rec.borrow().get_key();
        sstable_info.oldest_ts = rec.borrow().get_created();

        // create our SSTable
        let mut sstable = SSTable {
            rec_file: rec_file,
            info: sstable_info
        };

        // append this record, and record it's index
        let loc = sstable.append(&rec.borrow())?;
        sstable.info.indices.push(loc);

        let mut insert_count :u64 = 1;

        // maybe we have an SSTable with only 1 record?
        if count.is_some() && count.unwrap() >= insert_count {
            // append our info as the last record, and flush to disk
            let info_buff = to_vec(&sstable.info).unwrap();
            sstable.rec_file.append_flush(&info_buff)?;

            debug!("RETURN EARLY");

            return Ok(sstable);
        }

        // keep fetching from this iterator
        while let Some(rec) = records.next() {
            let loc = sstable.append(&rec.borrow())?;
            insert_count += 1;

            // add to our indices
            if insert_count % index_count as u64 == 0 {
                sstable.info.indices.push(loc);
            }

            // break out if we've reached our limit
            if count.is_some() && count.unwrap() >= insert_count {
                break;
            }
        }

        // append our info as the last record, and flush to disk
        let info_buff = to_vec(&sstable.info).unwrap();
        sstable.rec_file.append_flush(&info_buff)?;

        Ok(sstable)
    }

    fn append(&mut self, rec: &Record) -> Result<u64, IOError> {
        let loc = self.rec_file.append(&Record::serialize(rec))?; // append without flush

        let key = rec.get_key();
        let ts = rec.get_created();

        // update the smallest & largest key
        if key < self.info.smallest_key {
            self.info.smallest_key = key.to_vec();
        }

        if key > self.info.largest_key {
            self.info.largest_key = key;
        }

        // update the ts if it's older
        if ts > self.info.oldest_ts {
            self.info.oldest_ts = ts;
        }

        Ok(loc)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Record>, IOError> {
        // check if the key is in the range of this SSTable
        if key < self.info.smallest_key || self.info.largest_key < key {
            return Ok(None);
        }

        // binary search using the indices, then iterating from there
        let index_res = self.info.indices.binary_search_by(|index| {
            let rec_buff = self.rec_file.read_at(*index).expect("Error reading SSTable");
            let rec :Record = from_slice(&rec_buff).unwrap();

            rec.get_key().cmp(&key)
        });

        let start_offset = self.info.indices[match index_res {
            Ok(i) => i,
            Err(i) => i-1
        }];

        debug!("Binary search: {:?} -> {}", index_res, start_offset);

        for rec_buff in self.rec_file.iter_from(start_offset as u64) {
            let rec :Record = from_slice(&rec_buff).expect("Error decoding Record");

            if rec.get_key() == key {
                debug!("FOUND KEY!!!");
                return Ok(Some(rec));
            } else if rec.get_key() > key {
                return Ok(None);
            }
        }

        Ok(None) // should never get here
    }

    pub fn get_oldest_ts(&self) -> u64 {
        self.info.oldest_ts
    }
}

//impl Drop for SSTable {
//    fn drop(&mut self) {
//        debug!("Calling Drop on SSTable");
//        let info_buff = to_vec(&self.info).expect("Error serializing SSTableInfo");
//
//        self.rec_file.append_flush(&info_buff);
//    }
//}

impl Debug for SSTable {
    fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
        formatter.debug_struct("SSTable")
            .field("record_file", &self.rec_file)
            .field("info", &self.info)
            .finish()
    }
}

impl Debug for SSTableInfo {
    fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
        formatter.debug_struct("SSTableInfo")
            .field("smallest_key", &buf2string(&self.smallest_key))
            .field("largest_key", &buf2string(&self.largest_key))
            .field("indices", &self.indices)
            .finish()
    }
}


impl PartialOrd for SSTable {
    fn partial_cmp(&self, other: &SSTable) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SSTable {
    fn cmp(&self, other: &SSTable) -> Ordering {
        self.info.oldest_ts.cmp(&other.info.oldest_ts)
    }
}

impl PartialEq for SSTable {
    fn eq(&self, other: &SSTable) -> bool {
        self.info.oldest_ts == other.info.oldest_ts
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
    fn new_open() {
        let db_dir = gen_dir();
        let rec = Record::new(vec![1;8], vec![2;8]);
        let records = vec![rec];

        {
            SSTable::new(&db_dir.join("test.data"), &mut records.iter(), 2, None).unwrap();
        }

        let sstable_2 = SSTable::open(&db_dir.join("test.data")).unwrap();

        let ret = sstable_2.get(vec![1;8]).unwrap();

        assert!(ret.is_some());
        assert_eq!(ret.unwrap(), Record::new(vec![1;8], vec![2;8]));
    }

    #[test]
    fn get() {
        let db_dir = gen_dir();
        let mut records = vec![];

        for i in 0..100 {
            let rec = Record::new(vec![i], vec![i]);

            records.push(rec);
        }

        let sstable = SSTable::new(&db_dir.join("test.data"), &mut records.iter(), 2, None).unwrap();

        debug!("SSTABLE: {:?}", sstable);

        // look for all the records
        for i in 0..100 {
            let ret = sstable.get(vec![i]).unwrap();

            assert!(ret.is_some());
            assert_eq!(ret.unwrap(), Record::new(vec![i], vec![i]));
        }
    }
}