use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Error as IOError, ErrorKind};
use std::iter;
use std::path::PathBuf;
use std::mem::swap;
use std::thread::{spawn, yield_now};
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use itertools::kmerge;
use itertools::Itertools;
use rmps::encode::write;
use rmps::decode::from_read;
use regex::Regex;
use walkdir::WalkDir;

use record_file::RecordFile;
use sstable::SSTable;
use record::Record;

const WAL_HEADER: &[u8; 8] = b"WAL!\x01\x00\x00\x00";

// constants for now
const DEFAULT_MEM_COUNT: usize = 100_000;
const DEFAULT_GROUP_COUNT: u32 = 10_000;
const DEFAULT_FILE_COUNT: usize = 6;
const DEFAULT_BUFFER_SIZE: usize = 4096;
const DEFAULT_CACHE_SIZE: usize = 100_000;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KVSOptions {
    max_mem_count: usize,
    group_count: u32,
    file_count: usize,
    rec_file_buffer_size: usize,
    rec_file_cache_size: usize,
    db_dir: PathBuf
}

impl KVSOptions {
    /// Creates a new `KVSOptions` struct with the only required parameter
    ///
    /// See each of the methods of associated defaults.
    /// # Examples
    /// ```
    /// use kvs::KVSOptions;
    /// use std::path::PathBuf;
    ///
    /// let kvs = KVSOptions::new(&PathBuf::from("/tmp/kvs")).create().unwrap();
    /// ```
    pub fn new(db_dir: &PathBuf) -> KVSOptions {
        KVSOptions { max_mem_count: DEFAULT_MEM_COUNT,
            group_count: DEFAULT_GROUP_COUNT,
            file_count: DEFAULT_FILE_COUNT,
            rec_file_buffer_size: DEFAULT_BUFFER_SIZE,
            rec_file_cache_size: DEFAULT_CACHE_SIZE,
            db_dir: db_dir.to_path_buf()
        }
    }

    /// Sets the max number of items that will be kept in memory before persisting to disk.
    ///
    /// Generally you want this size to be as large as possible without taking up too much memory.
    /// It all depends upon the size the keys and values. The memory used per entry is:
    /// `size_of(key) * 2 + size_of(value)`
    ///
    /// Default: 100,000
    pub fn mem_count(&mut self, count: usize) -> &mut KVSOptions {
        self.max_mem_count = count; self
    }

    /// Sets the number of records that are grouped together in the data files.
    ///
    /// The number of `u64` records kept in memory per data file equals: `num_records / group_count`
    ///
    /// Default: 10,000
    pub fn group_count(&mut self, count: u32) -> &mut KVSOptions {
        self.group_count = count; self
    }

    /// The number of files to keep in the database directory.
    ///
    /// More files means faster searches, but slower writes.
    /// Fewer files mean faster writes, but slower reads.
    ///
    /// Default: 6
    pub fn file_count(&mut self, count: usize) -> &mut KVSOptions {
        self.file_count = count; self
    }

    /// The size of the buffer used for writing.
    ///
    /// It's best to keep this size a multiple of a page (4096 on most systems)
    ///
    /// Default: 4096
    pub fn file_buffer(&mut self, size: usize) -> &mut KVSOptions {
        self.rec_file_buffer_size = size; self
    }

    /// The size of the record cache.
    ///
    /// Key/value pairs, and other internal records are kepts in an in-memory cache. This sets
    /// the number of items kept in the cache. The memory usage will be approximately:
    /// size_of(key) + size_of(value) + 16 * this value
    ///
    /// Default: 100,000
    pub fn cache_size(&mut self, count: usize) -> &mut KVSOptions {
        self.rec_file_cache_size = count; self
    }

    /// Creates a `KVS` instance using the configured options.
    ///
    /// **This should only be called when creating a new `KVS` instance, not opening an existing one.**
    /// To open an existing KVS directory/database, use the `KVS::open` function.
    ///
    /// # Examples
    /// ```
    /// use kvs::KVSOptions;
    /// use std::path::PathBuf;
    ///
    /// let kvs = KVSOptions::new(&PathBuf::from("/tmp/kvs")).create().expect("Error creating KVS");
    /// ```
    /// # Panics
    /// If any of the options are nonsensical.
    pub fn create(&self) -> Result<KVS, IOError> {
        if self.max_mem_count < 2 { panic!("mem_count must be greater than 1: {}", self.max_mem_count); }
        if self.group_count < 100 { panic!("group_count is too small, make > 100: {}", self.group_count); }
        if self.file_count < 2 { panic!("file_count is too small, try > 2: {}", self.file_count); }
        if self.rec_file_buffer_size < 4096 { panic!("file_buffer is too small, try > 4096: {}", self.rec_file_buffer_size); }
        if self.rec_file_cache_size < 1 { panic!("cache_size must be greater than 1: {}", self.rec_file_cache_size); }

        KVS::new(self)
    }
}

// Contains all the data for the KVS instance
struct Data {
    wal_file: RecordFile,
    mem_table: BTreeMap<Vec<u8>, Record>,
    cur_sstable: SSTable,
}

/*
 * We only handle the "current" and "previous". When a compaction needs to occur, we have enough room
 * for another set of mem_table & cur_sstable to handle data. If that one fills up, and the first
 * compaction hasn't finished, then we simply have to block.
 */
pub struct KVS {
    options: KVSOptions,
    compaction_running: Arc<AtomicBool>, // indicates a compaction is running
    cur_sstable_num: Arc<AtomicUsize>,   // Arc<u64>,
    cur_data: Data,                      // current data for reading & writing new data
    prev_data: Arc<Option<Data>>,        // previous data that needs to be checked by get() during a compaction
    sstables: Arc<RwLock<BTreeSet<SSTable>>>,
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

/*
 * Files have the following meanings:
 * data.wal       - Write Ahead Log; journal of all put & deletes that are in mem_table
 * table.current  - SSTable with the merges from mem_table
 * table-#.data   - SSTables without overlapping ranges
 * *-new          - A new version of the file with the same name
 */
impl KVS {
    /// Creates a new KVS given a directory to store the files
    fn new(options: &KVSOptions) -> Result<KVS, IOError> {
        let db_dir_path = options.db_dir.to_path_buf();

        // check to see if the directory is empty or not
        if WalkDir::new(&db_dir_path).max_depth(0).into_iter().count() > 0 {
            return Err(IOError::new(ErrorKind::AlreadyExists, "Files already exist in the current directory"));
        }

        // otherwise, just call open which can handle constructing a new instance
        let ret = KVS::do_open(options);

        return match ret {
            Ok(kvs) => {
                let mut fd = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&db_dir_path.join("options.kvs"))?;

                // write out the options to a file
                write(&mut fd, &kvs.options).expect("Error writing options file");

                return Ok(kvs);
            },
            Err(e) => Err(e)
        };
    }

    /// Opens an existing KVS directory/database.
    ///
    /// All of the original options used to create the KVS instance will be used when open.
    ///
    /// # Examples
    /// ```
    /// use std::path::PathBuf;
    /// use kvs::{KVS, KVSOptions};
    ///
    /// let path = PathBuf::from("/tmp/kvs");
    ///
    /// {   // scope so it is dropped after creating
    ///     KVSOptions::new(&path).create().unwrap();
    /// }
    ///
    /// let kvs = KVS::open(&path).unwrap();
    /// ```
    pub fn open(db_dir: &PathBuf) -> Result<KVS, IOError> {
        let db_dir_path = db_dir.to_path_buf();

        let fd = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&db_dir_path.join("options.kvs"))?;

        let options :KVSOptions = from_read(fd).expect("Error reading options file");

        return KVS::do_open(&options);
    }

    fn do_open(options: &KVSOptions) -> Result<KVS, IOError> {
        let db_dir = options.db_dir.to_path_buf();
        let mut mem_table = BTreeMap::new();

        let wal_file = RecordFile::new(&db_dir.join("data.wal"), WAL_HEADER, options.rec_file_buffer_size, options.rec_file_cache_size)?;

        // read back in our WAL file if we have one
        if wal_file.record_count() > 0 {
            for bytes in wal_file.iter() {
                let rec = Record::deserialize(bytes);
                mem_table.insert(rec.key(), rec);
            }
        }

        let sstable_current_path = db_dir.join("table.current");

        let sstable_current = if sstable_current_path.exists() {
            SSTable::open(&sstable_current_path, options.rec_file_buffer_size, options.rec_file_cache_size)
        } else {
            SSTable::new(&sstable_current_path, &mut iter::empty::<Record>(), options.group_count, None, options.rec_file_buffer_size, options.rec_file_cache_size)
        }.expect("Error opening current SSTable");

        let mut sstables = BTreeSet::<SSTable>::new();

        let re = Regex::new(r"^table-(\d+).data$").unwrap();
        let mut max_sstable_num : u64 = 0;

        for entry in WalkDir::new(db_dir.to_path_buf()).max_depth(0) {
            let entry = entry.expect("Error reading directory entry");
            let path = entry.path();

            if path.is_dir() {
                continue
            }

            let file_name = path.file_name().expect("Error getting file name");
            let captures = re.captures(file_name.to_str().expect("Error getting string for file name"));

            if let Some(capture) = captures {
                // add to our set of tables
                sstables.insert(SSTable::open(&path.to_path_buf(), options.rec_file_buffer_size, options.rec_file_cache_size)?);

                // get the number of the table
                let sstable_num = capture.get(1).expect("Error capturing SSTable number").as_str().parse::<u64>().expect("Error parsing number");

                if sstable_num > max_sstable_num {
                    max_sstable_num = sstable_num;
                }
            }
        }
/*
        // gather up all the SSTables in this directory
        for entry in fs::read_dir(db_dir.to_path_buf())? {
            let entry = entry.expect("Error reading directory entry");
            let path = entry.path();

            if path.is_dir() {
                continue
            }

            let file_name = path.file_name().expect("Error getting file name");
            let captures = re.captures(file_name.to_str().expect("Error getting string for file name"));

            if let Some(capture) = captures {
                // add to our set of tables
                sstables.insert(SSTable::open(&path, options.rec_file_buffer_size, options.rec_file_cache_size)?);

                // get the number of the table
                let sstable_num = capture.get(1).expect("Error capturing SSTable number").as_str().parse::<u64>().expect("Error parsing number");

                if sstable_num > max_sstable_num {
                    max_sstable_num = sstable_num;
                }
            }
        }
*/
        let cur_data = Data {
            wal_file: wal_file,
            mem_table: mem_table,
            cur_sstable: sstable_current,
        };

        return Ok(KVS {
            options: options.clone(),
            compaction_running: Arc::new(AtomicBool::new(false)),
            cur_sstable_num: Arc::new(AtomicUsize::new((max_sstable_num + 1) as usize)),
            cur_data: cur_data,
            prev_data: Arc::new(None),
            sstables: Arc::new(RwLock::new(sstables)),
        })
    }

    /// Returns the path to the WAL file (or new one)
    fn wal_file_path(&self, new: bool) -> PathBuf {
        if new {
            self.options.db_dir.join("data.wal-new")
        } else {
            self.options.db_dir.join("data.wal")
        }
    }

    /// Returns the path to the current SSTable (or new one)
    fn cur_sstable_path(&self, new: bool) -> PathBuf {
        if new {
            self.options.db_dir.join("table.current-new")
        } else {
            self.options.db_dir.join("table.current")
        }
    }

    /// Generates the path to the current SSTable
    fn sstable_path(&self) -> PathBuf {
        self.options.db_dir.join(format!("table-{}.data", self.cur_sstable_num.load(Ordering::Relaxed)))
    }

    /// Creates a new WAL file, deletes current WAL file, and renames the new to current
    fn update_wal_file(&mut self) {
        {
            // create a new WAL file
            RecordFile::new(&self.wal_file_path(true), WAL_HEADER, self.options.rec_file_buffer_size, self.options.rec_file_cache_size).expect(&format!("Error creating WAL file: {:?}", self.wal_file_path(true)));
        }

        // remove the old one
        fs::remove_file(&self.wal_file_path(false)).expect(&format!("Error removing: {:?}", self.wal_file_path(false)));

        // rename the new to old
        fs::rename(self.wal_file_path(true), &self.wal_file_path(false)).expect(&format!("Error renaming WAL file: {:?} -> {:?}", self.wal_file_path(true), self.wal_file_path(false)));

        // set the WAL file in cur_data
        self.cur_data.wal_file = RecordFile::new(&self.wal_file_path(false), WAL_HEADER, self.options.rec_file_buffer_size, self.options.rec_file_cache_size).expect(&format!("Error opening WAL file: {:?}", self.wal_file_path(false)));
    }

    /// flush the mem_table to disk
    /// return: true if the flush occurred
    fn flush(&mut self, check_size: bool) -> bool {
        debug!("Starting a flush");

        if check_size && self.cur_data.mem_table.len() < self.options.max_mem_count {
            debug!("Too few records in mem_table: {} < {}", self.cur_data.mem_table.len(), self.options.max_mem_count);
            return false; // don't need to do anything yet
        }

        // update the reference to our current SSTable
        self.cur_data.cur_sstable = {
            let mem_it: Box<Iterator<Item=Record>> = Box::new(self.cur_data.mem_table.values().map(move |r| r.to_owned()));
            let ss_it: Box<Iterator<Item=Record>> = Box::new(self.cur_data.cur_sstable.iter());

            // create an iterator that merge-sorts and also coalesces out similar records
            let mut it = kmerge(vec![mem_it, ss_it]).coalesce(coalesce_records);

            {
                // create and close the new SSTable
                SSTable::new(&self.cur_sstable_path(true), &mut it, self.options.group_count as u32, None, self.options.rec_file_buffer_size, self.options.rec_file_cache_size).expect(&format!("Error creating SSTable: {:?}", &self.cur_sstable_path(true)));
            }

            // remove the old one if it exists
            // in theory, this should *always* exist because we create blank ones
            // however, flush() is called by Drop, so we could be in a funky state during this call
            if self.cur_sstable_path(false).exists() {
                fs::remove_file(&self.cur_sstable_path(false)).expect(&format!("Error removing: {:?}", self.cur_sstable_path(false)));
            }

            // rename the new to old
            fs::rename(&self.cur_sstable_path(true), &self.cur_sstable_path(false)).expect(&format!("Error renaming current SSTable: {:?} -> {:?}", self.cur_sstable_path(true), self.cur_sstable_path(false)));

            SSTable::open(&self.cur_sstable_path(false), self.options.rec_file_buffer_size, self.options.rec_file_cache_size).expect(&format!("Error opening current SSTable: {:?}", self.cur_sstable_path(false)))
        };

        // remove everything in the mem_table
        self.cur_data.mem_table.clear();

        // update the WAL file
        self.update_wal_file();

        debug!("Leaving flush");

        true
    }

    /// Compacts the mem_table, current_sstable, and sstables into new sstables
    /// return: the new cur_sstable_num
    fn compact(prev_data: &Option<Data>, sstables: &RwLock<BTreeSet<SSTable>>, options: &KVSOptions, cur_sstable_num: &AtomicUsize) {
        debug!("Starting a compaction");

        // save off the file paths to the old SSTables as it's not nice to delete files that are still open
        let sstable_paths = { sstables.read().expect("Error getting read lock for SSTables").iter().map(|table| table.file_path()).collect::<Vec<_>>() };

        let new_sstables = {
            let sstables = sstables.read().expect("Error getting read lock for SSTables");

            // create iterators for all the SSTables and the mem_table
            let mem_table_it: Box<Iterator<Item=Record>> = if let Some(pd) = prev_data { Box::new( pd.mem_table.values().map(move |r| r.to_owned())) } else { Box::new(iter::empty::<Record>()) };
            let cur_sstable_it: Box<Iterator<Item=Record>> = if let Some(pd) = prev_data { Box::new(pd.cur_sstable.iter()) } else { Box::new(iter::empty::<Record>()) };
            let mut record_its = Vec::with_capacity(options.file_count + 2); // make space for iterators for each SSTable + cur_sstable + mem_table
            let mut record_count = if let Some(pd) = prev_data { pd.mem_table.len() as u64 + pd.cur_sstable.record_count() } else { 0 };

            record_its.push(mem_table_it);
            record_its.push(cur_sstable_it);

            for sstable in sstables.iter() {
                record_count += sstable.record_count();
                record_its.push(Box::new(sstable.iter()));
            }

            let cur_time = get_timestamp();

            let mut it =
                kmerge(record_its).coalesce(coalesce_records).filter(|rec| {
                    !rec.is_delete() && !rec.is_expired(cur_time) // remove all deleted and expired
                });

            let records_per_file = record_count / options.file_count as u64;

            debug!("RECORDS PER FILE: {} = {} / {}", records_per_file, record_count, options.file_count as u64);

            let mut new_sstables = BTreeSet::<SSTable>::new();

            // create all the tables but the last one
            for _i in 0..options.file_count-1 {
                let sstable_path = options.db_dir.join(format!("table-{}.data", cur_sstable_num.load(Ordering::Relaxed)));
                let sstable = SSTable::new(&sstable_path, &mut it, options.group_count as u32, Some(records_per_file), options.rec_file_buffer_size, options.rec_file_cache_size).expect(&format!("Error creating SSTable: {:?}", sstable_path));
                cur_sstable_num.fetch_add(1, Ordering::Relaxed);
                new_sstables.insert(sstable);
            }

            // the last one gets all the rest of the records
            let sstable_path = options.db_dir.join(format!("table-{}.data", cur_sstable_num.load(Ordering::Relaxed)));
            let sstable = SSTable::new(&sstable_path, &mut it, options.group_count as u32, None, options.rec_file_buffer_size, options.rec_file_cache_size).expect(&format!("Error creating SSTable: {:?}", sstable_path));
            cur_sstable_num.fetch_add(1, Ordering::Relaxed);
            new_sstables.insert(sstable);

            new_sstables
        };

        { // scope this so we own the write lock for as little time as possible
            // lock for writing here before we do the deletes
            let mut sstables = sstables.write().expect("Error getting write lock for SSTables");

            // remove all the old SSTables
            for sstable_path in sstable_paths.iter() {
                fs::remove_file(&sstable_path).expect(&format!("Error removing old SSTable: {:?}", sstable_path));
            }

            *sstables = new_sstables; // this will update self
        }

        debug!("Leaving compact");
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
//        debug!("Called get: {:?}", key);

        let cur_time = get_timestamp();

//        debug!("MEM TABLE: {}", self.cur_data.mem_table.len());

        // first check the mem_table
        if self.cur_data.mem_table.contains_key(key) {
            let rec = self.cur_data.mem_table.get(key).unwrap();

            // found an expired or deleted key
            return if rec.is_expired(cur_time) || rec.is_delete() {
                debug!("Found expired or deleted key");
                None
            } else {
                Some(rec.value())
            };
        }

        // next check the current SSTable
        if let Some(rec) = self.cur_data.cur_sstable.get(key.to_vec()).expect("Error reading from SSTable") {
            return if rec.is_expired(cur_time) || rec.is_delete() {
                debug!("Found expired or deleted key");
                None
            } else {
                Some(rec.value())
            };
        }

        // then go to previous mem_table and SSTable
        if let Some(ref prev_data) = *self.prev_data {
//            debug!("We have prev_data: MEM_TABLE {}  SSTABLE: {}", prev_data.mem_table.len(), prev_data.cur_sstable.record_count());

            // first check the mem_table
            if prev_data.mem_table.contains_key(key) {
                let rec = prev_data.mem_table.get(key).unwrap();

                // found an expired or deleted key
                return if rec.is_expired(cur_time) || rec.is_delete() {
                    debug!("Found expired or deleted key");
                    None
                } else {
                    Some(rec.value())
                };
            }

            // next check the SSTable
            if let Some(rec) = prev_data.cur_sstable.get(key.to_vec()).expect("Error reading from previous SSTable") {
                return if rec.is_expired(cur_time) || rec.is_delete() {
                    debug!("Found expired or deleted key");
                    None
                } else {
                    Some(rec.value())
                };
            }
        }

        // finally, need to go to SSTables
        for sstable in self.sstables.read().expect("Error getting read lock for SSTables").iter() {
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

    #[inline]
    fn compact_needed(&self) -> bool {
        self.cur_data.mem_table.len() as u64 + self.cur_data.cur_sstable.record_count() >= (self.options.max_mem_count * self.options.file_count) as u64
    }

    fn insert(&mut self, record: Record) {
        self.cur_data.wal_file.append_record(&record).expect("Error writing to WAL file");

        // insert into the mem_table
        self.cur_data.mem_table.insert(record.key(), record);

        // check to see if we need to do a compaction
        if self.compact_needed() {

            debug!("COMPACT NEEDED");

            // attempt to get the "mutex" for compaction
            // compare_swap always returns the "previous" value
            // if a compaction is running (value = true), and try to set to true, won't work and return true, so we have to wait
            // if a compaction isn't running (value = false), and try to set to true, will work and return prev value of false
            while self.compaction_running.compare_and_swap(false, true, Ordering::Relaxed) == true {
                debug!("YIELDING PROCESSOR");
                yield_now(); // yield the CPU so we don't busy wait
            }

            // make sure again that we need to do a compaction
            // as another thread could have taken care of it for us
            if self.compact_needed() {
                debug!("DOUBLE-CHECK COMPACT NEEDED");

                // create a new mem_table and save off the old one
                let mut prev_mem_table = BTreeMap::new();
                swap(&mut self.cur_data.mem_table, &mut prev_mem_table); // prev_mem_table = cur_data.mem_table; cur_data.mem_table = BTreeMap::new();

                // create a new WAL file and update
                let prev_wal_path = self.options.db_dir.join("data.wal-prev"); // TODO: update wal_file_path function to handle this
                fs::rename(self.wal_file_path(false), &prev_wal_path).expect("Error renaming data.wal to data.wal-prev");
                let mut prev_wal_file = RecordFile::new(&self.wal_file_path(false), WAL_HEADER, self.options.rec_file_buffer_size, self.options.rec_file_cache_size).expect(&format!("Error creating WAL file: {:?}", self.wal_file_path(false)));
                swap(&mut self.cur_data.wal_file, &mut prev_wal_file);

                // rename table.current -> table.current-prev
                let prev_sstable_path = self.options.db_dir.join("table.current-prev"); // TODO: update sstable_path function to handle this
                fs::rename(self.cur_sstable_path(false), &prev_sstable_path).expect("Error renaming table.current to table.curent-prev");

                // create a new cur_sstable, and save off the old one
                let mut cur_sstable = SSTable::new(&self.cur_sstable_path(false), &mut iter::empty::<Record>(), self.options.group_count, None, self.options.rec_file_buffer_size, self.options.rec_file_cache_size).expect("Error creating new SSTable");
                swap(&mut self.cur_data.cur_sstable, &mut cur_sstable); // cur_sstable = cur_data.cur_sstable; cur_data.cur_sstable = SSTable::new();

                self.prev_data = Arc::new(Some(Data {
                    wal_file: prev_wal_file,
                    mem_table: prev_mem_table,
                    cur_sstable: cur_sstable,
                }));

                let sstables = self.sstables.clone();
                let options = self.options.clone();
                let cur_sstable_num = self.cur_sstable_num.clone();
                let compaction_running = self.compaction_running.clone();
                let prev_data = self.prev_data.clone();

                debug!("SPAWNING COMPACT");

                // do the actual compaction in the background
                spawn(move || {
                    // run the compaction
                    KVS::compact(&prev_data, &sstables, &options, &cur_sstable_num);

                    // remove the previous SSTable
                    fs::remove_file(&prev_sstable_path).expect(&format!("Error removing previous SSTable: {:?}", prev_sstable_path));

                    // remove the prev WAL file
                    fs::remove_file(&prev_wal_path).expect(&format!("Error removing prev WAL file: {:?}", prev_wal_path));

                    // release our mutex so other threads can get in
                    compaction_running.store(false, Ordering::Relaxed);
                });
            }

        } else if self.cur_data.mem_table.len() >= self.options.max_mem_count { // otherwise check for a flush
            self.flush(true);
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
        let mut sum = self.cur_data.mem_table.len() as u64;
        debug!("mem_table count: {}", sum);

        sum += self.cur_data.cur_sstable.record_count();
        debug!("cur_sstable count: {}", self.cur_data.cur_sstable.record_count());

        if let Some(ref prev_data) = *self.prev_data {
            sum += prev_data.mem_table.len() as u64;
            debug!("prev mem_table count: {}", prev_data.mem_table.len());

            sum += prev_data.cur_sstable.record_count();
            debug!("prev cur_sstable count: {}", prev_data.cur_sstable.record_count());
        }

        for sstable in self.sstables.read().expect("Error getting read lock for SSTables").iter() {
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

        // check to see if a compaction is running in the background
        while self.compaction_running.compare_and_swap(false, true, Ordering::Relaxed) == true {
            debug!("YIELDING PROCESSOR");
            yield_now(); // yield the CPU so we don't busy wait
        }

        // call flush without checking the size
        self.flush(false);
    }
}


#[cfg(test)]
mod tests {
    use kvs::{KVSOptions, KVS};
    use std::path::PathBuf;
    use rand::{thread_rng, Rng};
    use std::fs::create_dir;
    use simple_logger;
    use ::LOGGER_INIT;

    const MAX_MEM_COUNT: usize = 100;
    const MAX_FILE_COUNT: usize = 6;

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
        let _kvs = KVSOptions::new(&PathBuf::from(db_dir)).create().unwrap();
    }

    #[test]
    fn put_flush_get() {
        let db_dir = gen_dir();
        let mut kvs = KVSOptions::new(&PathBuf::from(db_dir)).create().unwrap();

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
        let mut kvs = KVSOptions::new(&PathBuf::from(db_dir)).mem_count(MAX_MEM_COUNT).create().unwrap();

        for _i in 0..MAX_MEM_COUNT+1 {
            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
            let key = format!("KEY_{}", rnd).as_bytes().to_vec();
            let value = rnd.as_bytes().to_vec();

            kvs.put(key, value);
        }

        assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT+1) as u64);
    }

//    #[test]
//    fn compact() {
//        let db_dir = gen_dir();
//        let mut kvs = KVSOptions::new(&PathBuf::from(db_dir)).create().unwrap();
//
//        for _i in 0..MAX_MEM_COUNT*MAX_FILE_COUNT + 1 {
//            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
//            let key = format!("KEY_{}", rnd).as_bytes().to_vec();
//            let value = rnd.as_bytes().to_vec();
//
//            kvs.put(key, value);
//        }
//
//        assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT*MAX_FILE_COUNT + 1) as u64);
//
//        kvs.compact();
//
//        assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT*MAX_FILE_COUNT + 1) as u64);
//    }

//    #[test]
//    fn compact2() {
//        let db_dir = gen_dir();
//
//        {
//            let mut kvs = KVSOptions::new(&db_dir).create().unwrap();
//
//            for _i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
//                let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
//                let key = format!("KEY_{}", rnd).as_bytes().to_vec();
//                let value = rnd.as_bytes().to_vec();
//
//                kvs.put(key, value);
//            }
//
//            assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT*MAX_FILE_COUNT + 1) as u64);
//
//            kvs.compact();
//
//            assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT*MAX_FILE_COUNT + 1) as u64);
//        }
//
//        let mut kvs = KVSOptions::new(&db_dir).create().unwrap();
//
//        for _i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
//            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
//            let key = format!("KEY_{}", rnd).as_bytes().to_vec();
//            let value = rnd.as_bytes().to_vec();
//
//            kvs.put(key, value);
//        }
//
//        assert_eq!(kvs.count_estimate(), ((MAX_MEM_COUNT*MAX_FILE_COUNT+1)*2) as u64);
//
//        kvs.compact();
//
//        assert_eq!(kvs.count_estimate(), ((MAX_MEM_COUNT*MAX_FILE_COUNT+1)*2) as u64);
//    }

    #[test]
    fn delete() {
        let db_dir = gen_dir();
        let mut kvs = KVSOptions::new(&PathBuf::from(db_dir)).create().unwrap();

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
            let mut kvs = KVSOptions::new(&db_dir).mem_count(MAX_MEM_COUNT).create().unwrap();

            // fill half the mem_table, so we're sure we don't flush
            for i in 0..MAX_MEM_COUNT / 2 {
                let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
                let key = format!("KEY_{}", i).as_bytes().to_vec();
                let value = rnd.as_bytes().to_vec();

                kvs.put(key, value);
            }
        }

        {
            let kvs = KVSOptions::new(&db_dir).create().unwrap();

            for i in 0..MAX_MEM_COUNT / 2 {
                let key = format!("KEY_{}", i).as_bytes().to_vec();

                assert!(kvs.get(&key).is_some(), "Couldn't find key: {}", i);
            }
        }

        {
            let kvs = KVSOptions::new(&db_dir).create().unwrap();

            for i in 0..MAX_MEM_COUNT / 2 {
                let key = format!("KEY_{}", i).as_bytes().to_vec();

                assert!(kvs.get(&key).is_some(), "Couldn't find key: {}", i);
            }
        }
    }

    #[test]
    fn put_compact_get() {
        let db_dir = gen_dir();

        let mut kvs = KVSOptions::new(&db_dir).mem_count(MAX_MEM_COUNT).file_count(MAX_FILE_COUNT).create().unwrap();

        for i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
            let rnd: String = thread_rng().gen_ascii_chars().take(6).collect();
            let key = format!("KEY_{}", i).as_bytes().to_vec();
            let value = rnd.as_bytes().to_vec();

            kvs.put(key, value);
        }

//        assert_eq!(kvs.count_estimate(), (MAX_MEM_COUNT*MAX_FILE_COUNT + 1) as u64);

        debug!("STARTING GETS");

        for i in 0..MAX_MEM_COUNT * MAX_FILE_COUNT + 1 {
            let key = format!("KEY_{}", i).as_bytes().to_vec();

            assert!(kvs.get(&key).is_some(), "Couldn't find key: {}", i);
        }

        debug!("FINISHED GETS");
    }

    #[test]
    fn put_compact_update_get() {
        let db_dir = gen_dir();

        let mut kvs = KVSOptions::new(&db_dir).mem_count(MAX_MEM_COUNT).file_count(MAX_FILE_COUNT).create().unwrap();

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

        let mut kvs = KVSOptions::new(&db_dir).mem_count(MAX_MEM_COUNT).file_count(MAX_FILE_COUNT).create().unwrap();

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
