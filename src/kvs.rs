use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Error as IOError};
use std::iter;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use itertools::kmerge;
use itertools::Itertools;

use regex::Regex;

use record_file::RecordFile;
use sstable::SSTable;
use record::Record;

const WAL_HEADER: &[u8; 8] = b"WAL!\x01\x00\x00\x00";

// constants for now
const MAX_MEM_COUNT: usize = 100;
const GROUP_COUNT: u32 = 10;
const MAX_FILE_COUNT: usize = 4;

pub struct KVS {
    db_dir: PathBuf,
    cur_sstable_num: u64,
    wal_file: RecordFile,
    mem_table: BTreeMap<Vec<u8>, Record>,
    cur_sstable: SSTable,
    sstables: BTreeSet<SSTable>,
}

/// Gets the timestamp/epoch in ms
pub fn get_timestamp() -> u64 {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards");

    return ts.as_secs() * 1000 + ts.subsec_nanos() as u64 / 1_000_000;
}


/// Used to coalesce records during iteration
fn coalesce_records(prev: Record, curr: Record) -> Result<Record, (Record, Record)> {
    // we always go with the newest timestamp
    if prev.key() == curr.key() {
        Ok(if prev.created() > curr.created() { prev } else { curr })
    } else {
        Err( (prev, curr) )
    }
}

/**
 * Files have the following meanings:
 * data.wal       - Write Ahead Log; journal of all put & deletes that are in mem_table
 * table.current  - SSTable with the merges from mem_table
 * table-#.data   - SSTables without overlapping ranges
 * *-new          - A new version of the file with the same name
 */
impl KVS {
    /// Creates a new KVS given a directory to store the files
    pub fn new(db_dir: &PathBuf) -> Result<KVS, IOError> {
        let wal_file = RecordFile::new(&db_dir.join("data.wal"), WAL_HEADER)?;

        let sstable_current_path = db_dir.join("table.current");

        let sstable_current = if sstable_current_path.exists() {
            SSTable::open(&sstable_current_path)
        } else {
            SSTable::new(&sstable_current_path, &mut iter::empty::<Record>(), GROUP_COUNT, None)
        }.expect("Error opening current SSTable");

        let mut sstables = BTreeSet::<SSTable>::new();

        let re = Regex::new(r"^table-(\d+).data$").unwrap();
        let mut max_sstable_num : u64 = 0;

        // gather up all the SSTables in this directory
        for entry in fs::read_dir(db_dir)? {
            let entry = entry.expect("Error reading directory entry");
            let path = entry.path();

            if path.is_dir() {
                continue
            }

            let file_name = path.file_name().expect("Error getting file name");
            let captures = re.captures(file_name.to_str().expect("Error getting string for file name"));

            if let Some(capture) = captures {
                // add to our set of tables
                sstables.insert(SSTable::open(&path)?);

                // get the number of the table
                let sstable_num = capture.get(1).expect("Error capturing SSTable number").as_str().parse::<u64>().expect("Error parsing number");

                if sstable_num > max_sstable_num {
                    max_sstable_num = sstable_num;
                }
            }
        }

        return Ok(KVS {
            db_dir: PathBuf::from(db_dir),
            cur_sstable_num: max_sstable_num + 1,
            wal_file: wal_file,
            mem_table: BTreeMap::new(),
            cur_sstable: sstable_current,
            sstables: sstables,
        })
    }

    /// Creates a new WAL file, deletes current WAL file, and renames the new to current
    fn update_wal_file(&mut self) -> Result<(), IOError> {
        let wal_file_path = self.db_dir.join("data.wal-new");

        {
            // create a new WAL file
            RecordFile::new(&wal_file_path, WAL_HEADER)?;
        }

        // remove the old one
        fs::remove_file(&self.db_dir.join("data.wal"))?;

        // rename the new to old
        fs::rename(wal_file_path, &self.db_dir.join("data.wal"))?;

        self.wal_file = RecordFile::new(&self.db_dir.join("data.wal"), WAL_HEADER)?;

        Ok( () )
    }

    /// flush the mem_table to disk
    fn flush(&mut self) -> Result<(), IOError> {
        info!("Starting a flush");

        if self.mem_table.len() < MAX_MEM_COUNT {
            debug!("Too few records in mem_table: {} < {}", self.mem_table.len(), MAX_MEM_COUNT);
            return Ok( () ); // don't need to do anything yet
        }

        // update the reference to our current SSTable
        self.cur_sstable = {
            // create a new SSTable
            let sstable_path = self.db_dir.join("table.current-new");

            let mem_it: Box<Iterator<Item=Record>> = Box::new(self.mem_table.values().map(move |r| r.to_owned()));
            let ss_it: Box<Iterator<Item=Record>> = Box::new(self.cur_sstable.iter());

            // create an iterator that merge-sorts and also coalesces out similar records
            let mut it = kmerge(vec![mem_it, ss_it]).coalesce(coalesce_records);

            {
                SSTable::new(&sstable_path, &mut it, MAX_MEM_COUNT as u32, None)?;
            }

            // remove the old file, and rename the new -> old
            // remove the old one
            fs::remove_file(&self.db_dir.join("table.current"))?;

            // rename the new to old
            fs::rename(&sstable_path, &self.db_dir.join("table.current"))?;

            SSTable::open(&self.db_dir.join("table.current"))?
        };

        // remove everything in the mem_table
        self.mem_table.clear();

        // update the WAL file
        self.update_wal_file()?;

        info!("Leaving flush");

        Ok( () )
    }

    fn compact(&mut self) -> Result<(), IOError> {
        info!("Starting a compaction");

        // we wait until we have enough records for every file to get MAX_MEM_COUNT
        if self.mem_table.len() as u64 + self.cur_sstable.record_count() < (MAX_MEM_COUNT * MAX_FILE_COUNT) as u64 {
            info!("Not enough records for compact: {} < {}", self.mem_table.len() as u64 + self.cur_sstable.record_count(), (MAX_MEM_COUNT * MAX_FILE_COUNT) as u64);
            return Ok( () )
        }

        // create iterators for all the SSTables and the mem_table
        self.sstables = {
            let mem_it: Box<Iterator<Item=Record>> = Box::new(self.mem_table.values().map(move |r| r.to_owned()));
            let ss_cur_it: Box<Iterator<Item=Record>> = Box::new(self.cur_sstable.iter());
            let mut ss_its = Vec::with_capacity(MAX_FILE_COUNT + 2);
            let mut record_count = self.mem_table.len() as u64 + self.cur_sstable.record_count();

            ss_its.push(mem_it);
            ss_its.push(ss_cur_it);

            for sstable in self.sstables.iter() {
                record_count += sstable.record_count();
                ss_its.push(Box::new(sstable.iter()));
            }

            let mut it = kmerge(ss_its).coalesce(coalesce_records);

            let records_per_file = record_count / MAX_FILE_COUNT as u64;

            debug!("RECORDS PER FILE: {} = {} / {}", records_per_file, record_count, MAX_FILE_COUNT as u64);

            let mut new_sstables = BTreeSet::<SSTable>::new();

            // create all the tables but the last one
            for _i in 0..MAX_FILE_COUNT-1 {
                let sstable_path = self.db_dir.join(format!("table-{}.data", self.cur_sstable_num));
                self.cur_sstable_num += 1;

                new_sstables.insert(SSTable::new(&sstable_path, &mut it, MAX_MEM_COUNT as u32, Some(records_per_file))?);
            }

            // the last one gets all the rest of the records
            let sstable_path = self.db_dir.join(format!("table-{}.data", self.cur_sstable_num));
            self.cur_sstable_num += 1;

            new_sstables.insert(SSTable::new(&sstable_path, &mut it, MAX_MEM_COUNT as u32, None)?);

            new_sstables
        };

        // remove the current SSTable
        fs::remove_file(self.db_dir.join("table.current"))?;

        // remove everything from the mem_table
        self.mem_table.clear();

        // update the WAL file
        self.update_wal_file()?;

        info!("Leaving compact");

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
                Some(rec.value())
            };
        }

        // next check the current SSTable
        if let Some(rec) = self.cur_sstable.get(key.to_vec()).expect("Error reading from SSTable") {
            return Some(rec.value());
        }

        // finally, need to go to SSTables
        for sstable in self.sstables.iter() {
            debug!("SSTABLE: {:?}", sstable);

            let ret_opt = sstable.get(key.to_vec()).expect("Error reading from SSTable");

            // we didn't find the key
            if ret_opt.is_none() {
                return None;
            }

            let rec = ret_opt.unwrap();

            // sanity check
            if rec.is_delete() {
                panic!("Found deleted key in SSTable: {:?}", sstable);
            }

            return Some(rec.value());
        }

        None // if we get to here, we don't have it
    }

    fn insert(&mut self, record: Record) -> Result<(), IOError> {
        self.wal_file.append(&Record::serialize(&record))?;

        // insert into the mem_table
        self.mem_table.insert(record.key(), record);

        // check to see if we need to flush to disk
        if self.mem_table.len() >= MAX_MEM_COUNT {
            // compact won't do anything if it's not needed,
            // but will take care of mem_table if we hit a threshold
            self.compact()?;

            // flush can be safely called after compact
            self.flush()?;
        }

        Ok( () )
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), IOError> {
        info!("Called put: {:?}", key);

        // create a record, and call insert
        let rec = Record::new(key.to_vec(), Some(value));

        self.insert(rec)
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<(), IOError> {
        info!("Called delete: {:?}", key);

        // create a record, and call insert
        let rec = Record::new(key.to_vec(), None);

        self.insert(rec)
    }
}

impl Drop for KVS {
    fn drop(&mut self) {
        debug!("KVS Drop");
        self.flush().expect("Error flushing to disk on Drop");
    }
}


#[cfg(test)]
mod tests {
    use kvs::KVS;
    use kvs::MAX_MEM_COUNT;
    use kvs::MAX_FILE_COUNT;
    use std::path::PathBuf;
    use rand::{thread_rng, Rng};
    use std::fs::create_dir;
    use simple_logger;
    use ::LOGGER_INIT;

    fn gen_dir() -> PathBuf {
        LOGGER_INIT.call_once(|| simple_logger::init().unwrap()); // this will panic on error

        let tmp_dir: String = thread_rng().gen_ascii_chars().take(6).collect();
        let ret_dir = PathBuf::from("/tmp").join(format!("kvs_{}", tmp_dir));

        debug!("CREATING TMP DIR: {:?}", ret_dir);

        create_dir(&ret_dir).unwrap();

        return ret_dir;
    }

    #[test]
    fn new() {
        let db_dir = gen_dir();
        let _kvs = KVS::new(&PathBuf::from(db_dir)).unwrap();
    }

    #[test]
    fn get() {
        let db_dir = gen_dir();
        let mut kvs = KVS::new(&PathBuf::from(db_dir)).unwrap();

        let key = "KEY".as_bytes();
        let value = "VALUE".as_bytes();

        kvs.put(key.to_vec(), value.to_vec()).unwrap();

        kvs.flush().unwrap();

        let ret = kvs.get(&key.to_vec());

        assert_eq!(value, ret.unwrap().as_slice());
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

        for _i in 0..MAX_MEM_COUNT+1 {
            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
            let key = format!("KEY_{}", rnd).as_bytes().to_vec();
            let value = rnd.as_bytes().to_vec();

            kvs.put(key, value).unwrap();
        }
    }

    #[test]
    fn compact() {
        let db_dir = gen_dir();
        let mut kvs = KVS::new(&PathBuf::from(db_dir)).unwrap();

        for _i in 0..MAX_MEM_COUNT*MAX_FILE_COUNT + 1 {
            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
            let key = format!("KEY_{}", rnd).as_bytes().to_vec();
            let value = rnd.as_bytes().to_vec();

            kvs.put(key, value).unwrap();
        }

        kvs.compact().unwrap();
    }

    #[test]
    fn compact2() {
        let db_dir = gen_dir();

        {
            let mut kvs = KVS::new(&db_dir).unwrap();

            for _i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
                let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
                let key = format!("KEY_{}", rnd).as_bytes().to_vec();
                let value = rnd.as_bytes().to_vec();

                kvs.put(key, value).unwrap();
            }

            kvs.compact().unwrap();
        }

        let mut kvs = KVS::new(&db_dir).unwrap();

        for _i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
            let key = format!("KEY_{}", rnd).as_bytes().to_vec();
            let value = rnd.as_bytes().to_vec();

            kvs.put(key, value).unwrap();
        }

        kvs.compact().unwrap();
    }
}
