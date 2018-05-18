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
const MAX_MEM_COUNT: usize = 10_000;
const GROUP_COUNT: u32 = 100;
const MAX_FILE_COUNT: usize = 5;

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
        let mut mem_table = BTreeMap::new();

        let wal_file = RecordFile::new(&db_dir.join("data.wal"), WAL_HEADER)?;

        // read back in our WAL file if we have one
        if wal_file.record_count() > 0 {
            for bytes in wal_file.iter() {
                let rec = Record::deserialize(bytes);
                mem_table.insert(rec.key(), rec);
            }
        }

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
            mem_table: mem_table,
            cur_sstable: sstable_current,
            sstables: sstables,
        })
    }

    /// Returns the path to the WAL file (or new one)
    fn wal_file_path(&self, new: bool) -> PathBuf {
        if new {
            self.db_dir.join("data.wal-new")
        } else {
            self.db_dir.join("data.wal")
        }
    }

    /// Returns the path to the current SSTable (or new one)
    fn cur_sstable_path(&self, new: bool) -> PathBuf {
        if new {
            self.db_dir.join("table.current-new")
        } else {
            self.db_dir.join("table.current")
        }
    }

    /// Generates the path to the current SSTable
    fn sstable_path(&self) -> PathBuf {
        self.db_dir.join(format!("table-{}.data", self.cur_sstable_num))
    }

    /// Creates a new WAL file, deletes current WAL file, and renames the new to current
    fn update_wal_file(&mut self) {
        {
            // create a new WAL file
            RecordFile::new(&self.wal_file_path(true), WAL_HEADER).expect(&format!("Error creating WAL file: {:?}", self.wal_file_path(true)));
        }

        // remove the old one
        fs::remove_file(&self.wal_file_path(false)).expect(&format!("Error removing: {:?}", self.wal_file_path(false)));

        // rename the new to old
        fs::rename(self.wal_file_path(true), &self.wal_file_path(false)).expect(&format!("Error renaming WAL file: {:?} -> {:?}", self.wal_file_path(true), self.wal_file_path(false)));

        self.wal_file = RecordFile::new(&self.wal_file_path(false), WAL_HEADER).expect(&format!("Error opening WAL file: {:?}", self.wal_file_path(false)));
    }

    /// flush the mem_table to disk
    /// return: true if the flush occured
    fn flush(&mut self, check_size: bool) -> bool {
        debug!("Starting a flush");

        if check_size && self.mem_table.len() < MAX_MEM_COUNT {
            debug!("Too few records in mem_table: {} < {}", self.mem_table.len(), MAX_MEM_COUNT);
            return false; // don't need to do anything yet
        }

        // update the reference to our current SSTable
        self.cur_sstable = {
            let mem_it: Box<Iterator<Item=Record>> = Box::new(self.mem_table.values().map(move |r| r.to_owned()));
            let ss_it: Box<Iterator<Item=Record>> = Box::new(self.cur_sstable.iter());

            // create an iterator that merge-sorts and also coalesces out similar records
            let mut it = kmerge(vec![mem_it, ss_it]).coalesce(coalesce_records);

            {
                // create and close the new SSTable
                SSTable::new(&self.cur_sstable_path(true), &mut it, GROUP_COUNT as u32, None).expect(&format!("Error creating SSTable: {:?}", &self.cur_sstable_path(true)));
            }

            // remove the old one if it exists
            // in theory, this should *always* exist because we create blank ones
            // however, flush() is called by Drop, so we could be in a funky state during this call
            if self.cur_sstable_path(false).exists() {
                fs::remove_file(&self.cur_sstable_path(false)).expect(&format!("Error removing: {:?}", self.cur_sstable_path(false)));
            }

            // rename the new to old
            fs::rename(&self.cur_sstable_path(true), &self.cur_sstable_path(false)).expect(&format!("Error renaming current SSTable: {:?} -> {:?}", self.cur_sstable_path(true), self.cur_sstable_path(false)));

            SSTable::open(&self.cur_sstable_path(false)).expect(&format!("Error opening current SSTable: {:?}", self.cur_sstable_path(false)))
        };

        // remove everything in the mem_table
        self.mem_table.clear();

        // update the WAL file
        self.update_wal_file();

        debug!("Leaving flush");

        true
    }

    /// Compacts the mem_table, current_sstable, and sstables into new sstables
    /// return: true if the compaction actually ran
    fn compact(&mut self) -> bool {
        debug!("Starting a compaction");

        // we wait until we have enough records for every file to get MAX_MEM_COUNT
        if self.mem_table.len() as u64 + self.cur_sstable.record_count() < (MAX_MEM_COUNT * MAX_FILE_COUNT) as u64 {
            debug!("Not enough records for compact: {} < {}", self.mem_table.len() as u64 + self.cur_sstable.record_count(), (MAX_MEM_COUNT * MAX_FILE_COUNT) as u64);
            return false;
        }

        // save off the file paths to the old SSTables as it's not nice to delete files that are still open
        let sstable_paths = self.sstables.iter().map(|table| table.file_path()).collect::<Vec<_>>();

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

            let cur_time = get_timestamp();

            let mut it =
                kmerge(ss_its).coalesce(coalesce_records).filter(|rec| {
                    !rec.is_delete() && !rec.is_expired(cur_time) // remove all deleted and expired
                });

            let records_per_file = record_count / MAX_FILE_COUNT as u64;

            debug!("RECORDS PER FILE: {} = {} / {}", records_per_file, record_count, MAX_FILE_COUNT as u64);

            let mut new_sstables = BTreeSet::<SSTable>::new();

            // create all the tables but the last one
            for _i in 0..MAX_FILE_COUNT-1 {
                let sstable = SSTable::new(&self.sstable_path(), &mut it, GROUP_COUNT as u32, Some(records_per_file)).expect(&format!("Error creating SSTable: {:?}", self.sstable_path()));
                self.cur_sstable_num += 1;
                new_sstables.insert(sstable);
            }

            // the last one gets all the rest of the records
            let sstable = SSTable::new(&self.sstable_path(), &mut it, GROUP_COUNT as u32, None).expect(&format!("Error creating SSTable: {:?}", self.sstable_path()));
            self.cur_sstable_num += 1;
            new_sstables.insert(sstable);

            new_sstables
        };

        // remove all the old SSTables
        for sstable_path in sstable_paths.iter() {
            fs::remove_file(&sstable_path).expect(&format!("Error removing old SSTable: {:?}", sstable_path));
        }

        // remove the current SSTable
        fs::remove_file(self.cur_sstable_path(false)).expect(&format!("Error removing current SSTable: {:?}", self.cur_sstable_path(false)));

        // create a new empty current SSTable
        self.cur_sstable = SSTable::new(&self.cur_sstable_path(false), &mut iter::empty::<Record>(), GROUP_COUNT, None).expect(&format!("Error creating blank current SSTable: {:?}", self.cur_sstable_path(false)));

        // remove everything from the mem_table
        self.mem_table.clear();

        // update the WAL file
        self.update_wal_file();

        debug!("Leaving compact");

        true
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        debug!("Called get: {:?}", key);

        let cur_time = get_timestamp();

        debug!("MEM TABLE: {}", self.mem_table.len());

        // first check the mem_table
        if self.mem_table.contains_key(key) {
            let rec = self.mem_table.get(key).unwrap();

            // found an expired or deleted key
            return if rec.is_expired(cur_time) || rec.is_delete() {
                debug!("Found expired or deleted key");
                None
            } else {
                Some(rec.value())
            };
        }

        // next check the current SSTable
        if let Some(rec) = self.cur_sstable.get(key.to_vec()).expect("Error reading from SSTable") {
            return if rec.is_expired(cur_time) || rec.is_delete() {
                debug!("Found expired or deleted key");
                None
            } else {
                Some(rec.value())
            };
        }

        // finally, need to go to SSTables
        for sstable in self.sstables.iter() {
            debug!("SSTABLE: {:?}", sstable);

            let ret_opt = sstable.get(key.to_vec()).expect("Error reading from SSTable");

            // we didn't find the key
            if ret_opt.is_none() {
                continue;
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

    fn insert(&mut self, record: Record) {
        self.wal_file.append_record(&record).expect("Error writing to WAL file");

        // insert into the mem_table
        self.mem_table.insert(record.key(), record);

        // check to see if we need to flush to disk
        if self.mem_table.len() >= MAX_MEM_COUNT {
            // compact won't do anything if it's not needed
            if !self.compact() {
                // see if we need to flush, if a compaction didn't occur
                self.flush(true);
            }
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
//        debug!("Called put: {:?}", key);

        // create a record, and call insert
        let rec = Record::new(key.to_vec(), Some(value));

        self.insert(rec)
    }

    pub fn delete(&mut self, key: &Vec<u8>) {
        debug!("Called delete: {:?}", key);

        // create a record, and call insert
        let rec = Record::new(key.to_vec(), None);

        self.insert(rec)
    }

    /// Returns an upper bound on the number of records
    /// To get an exact count, we'd need to read all the records in searching for deletes
    pub fn count_estimate(&self) -> u64 {
        let mut sum = self.mem_table.len() as u64;

        debug!("mem_table count: {}", sum);

        sum += self.cur_sstable.record_count();

        debug!("cur_sstable count: {}", self.cur_sstable.record_count());

        for sstable in self.sstables.iter() {
            debug!("{:?} count: {}", sstable, sstable.record_count());
            sum += sstable.record_count();
        }

        debug!("SUM: {}", sum);

        return sum;
    }

}

impl Drop for KVS {
    fn drop(&mut self) {
        debug!("KVS Drop");
        // call flush without checking the size
        self.flush(false);
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
    fn put_flush_get() {
        let db_dir = gen_dir();
        let mut kvs = KVS::new(&PathBuf::from(db_dir)).unwrap();

        let key = "KEY".as_bytes();
        let value = "VALUE".as_bytes();

        kvs.put(key.to_vec(), value.to_vec());

        assert_eq!(kvs.count_estimate(), 1);

        kvs.flush(false);

        assert_eq!(kvs.count_estimate(), 1);

        let ret = kvs.get(&key.to_vec());

        assert_eq!(value, ret.unwrap().as_slice());
    }

    #[test]
    fn auto_flush() {
        let db_dir = gen_dir();
        let mut kvs = KVS::new(&PathBuf::from(db_dir)).unwrap();

        for _i in 0..MAX_MEM_COUNT+1 {
            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
            let key = format!("KEY_{}", rnd).as_bytes().to_vec();
            let value = rnd.as_bytes().to_vec();

            kvs.put(key, value);
        }

        assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT+1) as u64);
    }

    #[test]
    fn compact() {
        let db_dir = gen_dir();
        let mut kvs = KVS::new(&PathBuf::from(db_dir)).unwrap();

        for _i in 0..MAX_MEM_COUNT*MAX_FILE_COUNT + 1 {
            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
            let key = format!("KEY_{}", rnd).as_bytes().to_vec();
            let value = rnd.as_bytes().to_vec();

            kvs.put(key, value);
        }

        assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT*MAX_FILE_COUNT + 1) as u64);

        kvs.compact();

        assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT*MAX_FILE_COUNT + 1) as u64);
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

                kvs.put(key, value);
            }

            assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT*MAX_FILE_COUNT + 1) as u64);

            kvs.compact();

            assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT*MAX_FILE_COUNT + 1) as u64);
        }

        let mut kvs = KVS::new(&db_dir).unwrap();

        for _i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
            let key = format!("KEY_{}", rnd).as_bytes().to_vec();
            let value = rnd.as_bytes().to_vec();

            kvs.put(key, value);
        }

        assert_eq!(kvs.count_estimate(), ((MAX_MEM_COUNT*MAX_FILE_COUNT+1)*2) as u64);

        kvs.compact();

        assert_eq!(kvs.count_estimate(), ((MAX_MEM_COUNT*MAX_FILE_COUNT+1)*2) as u64);
    }

    #[test]
    fn delete() {
        let db_dir = gen_dir();
        let mut kvs = KVS::new(&PathBuf::from(db_dir)).unwrap();

        let key = "KEY".as_bytes();
        let value = "VALUE".as_bytes();

        kvs.put(key.to_vec(), value.to_vec());

        assert_eq!(kvs.count_estimate(), 1);

        kvs.delete(&key.to_vec());

        // should still be only 1 record
        assert_eq!(kvs.count_estimate(), 1);

        assert!(kvs.get(&key.to_vec()).is_none(), "Found key after deleting it!");

        kvs.flush(false);

        // should still be only 1 record
        assert_eq!(kvs.count_estimate(), 1);

        assert!(kvs.get(&key.to_vec()).is_none(), "Found key after deleting it!");
    }

    #[test]
    fn put_close_open_get() {
        let db_dir = gen_dir();

        {
            let mut kvs = KVS::new(&db_dir).unwrap();

            // fill half the mem_table, so we're sure we don't flush
            for i in 0..MAX_MEM_COUNT / 2 {
                let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
                let key = format!("KEY_{}", i).as_bytes().to_vec();
                let value = rnd.as_bytes().to_vec();

                kvs.put(key, value);
            }
        }

        {
            let kvs = KVS::new(&db_dir).unwrap();

            for i in 0..MAX_MEM_COUNT / 2 {
                let key = format!("KEY_{}", i).as_bytes().to_vec();

                assert!(kvs.get(&key).is_some(), "Couldn't find key: {}", i);
            }
        }

        {
            let kvs = KVS::new(&db_dir).unwrap();

            for i in 0..MAX_MEM_COUNT / 2 {
                let key = format!("KEY_{}", i).as_bytes().to_vec();

                assert!(kvs.get(&key).is_some(), "Couldn't find key: {}", i);
            }
        }
    }

    #[test]
    fn put_compact_get() {
        let db_dir = gen_dir();

        let mut kvs = KVS::new(&db_dir).unwrap();

        for i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
            let key = format!("KEY_{}", i).as_bytes().to_vec();
            let value = rnd.as_bytes().to_vec();

            kvs.put(key, value);
        }

        assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT*MAX_FILE_COUNT + 1) as u64);

        for i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
            let key = format!("KEY_{}", i).as_bytes().to_vec();

            assert!(kvs.get(&key).is_some(), "Couldn't find key: {}", i);
        }
    }

    #[test]
    fn put_compact_update_get() {
        let db_dir = gen_dir();

        let mut kvs = KVS::new(&db_dir).unwrap();

        for i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
            let key = format!("KEY_{}", i).as_bytes().to_vec();
            let value = rnd.as_bytes().to_vec();

            kvs.put(key, value);
        }

        // compact would have happened here
        assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT*MAX_FILE_COUNT + 1) as u64);

        for i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
            let key = format!("KEY_{}", i).as_bytes().to_vec();
            let value = format!("VALUE_{}", i).as_bytes().to_vec();

            kvs.put(key, value); // update our keys
        }

        for i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
            let key = format!("KEY_{}", i).as_bytes().to_vec();

            let ret = kvs.get(&key);
            assert!(ret.is_some(), "Couldn't find key: {}", i);
            assert_eq!(ret.unwrap(), format!("VALUE_{}", i).as_bytes().to_vec(), "Didn't get update for key: {}", i);
        }
    }

    #[test]
    fn put_compact_delete_get() {
        let db_dir = gen_dir();

        let mut kvs = KVS::new(&db_dir).unwrap();

        for i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
            let key = format!("KEY_{}", i).as_bytes().to_vec();
            let value = rnd.as_bytes().to_vec();

            kvs.put(key, value);
        }

        // compact would have happened here
        assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT*MAX_FILE_COUNT + 1) as u64);

        for i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
            let key = format!("KEY_{}", i).as_bytes().to_vec();

            kvs.delete(&key); // delete our key
        }

        // compact would happen here

        for i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
            let key = format!("KEY_{}", i).as_bytes().to_vec();

            let ret = kvs.get(&key);
            assert!(ret.is_none(), "Found deleted key: {}", i);
        }
    }


}
