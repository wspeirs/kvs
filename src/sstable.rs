use rmps::encode::to_vec;
use rmps::decode::from_slice;

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::cmp::Ordering::{Less, Equal, Greater};
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::io::{Error as IOError, ErrorKind};
use std::iter::IntoIterator;
use std::path::PathBuf;

use record_file::buf2string;
use record_file::RecordFile;
use record::Record;

use serde_utils::{serialize_u64_exact, deserialize_u64_exact};

use U32_SIZE;
use U64_SIZE;

const SSTABLE_HEADER: &[u8; 8] = b"DATA\x01\x00\x00\x00";


#[derive(Serialize, Deserialize, Clone)]
struct SSTableInfo {
    record_count: u64,
    group_count: u32,
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
    pub fn open(file_path: &PathBuf, buffer_size: usize, cache_size: usize) -> Result<SSTable, IOError> {
        if !file_path.exists() {
            return Err(IOError::new(ErrorKind::NotFound, format!("The SSTable {:?} was not found", file_path)));
        }

        let rec_file = RecordFile::new(file_path, SSTABLE_HEADER, buffer_size, cache_size)?;

        let info = from_slice(&rec_file.last_record().expect("Error reading SSTableInfo")).expect("Error decoding SSTableInfo");

        let sstable = SSTable { rec_file: rec_file, info: info };

        debug!("Opened SSTable: {:?}", sstable);

        Ok(sstable)
    }

    /// Creates a new `SSTable` that is immutable once returned.
    /// * file_path - the path to the SSTable to create
    /// * records - an iterator to records that will be inserted into this `SSTable`
    /// * group_count - the number of records to group together for each recorded index
    /// * count - the number of records to pull from the iterator and put into the `SSTable`
    pub fn new<I, B>(file_path: &PathBuf,  records: &mut I, group_count: u32, count: Option<u64>, buffer_size: usize, cache_size: usize) -> Result<SSTable, IOError>
        where I: Iterator<Item=B>, B: Borrow<Record>
    {
        debug!("New SSTable: {:?} group_count: {} count: {:?}", file_path, group_count, count);

        assert_ne!(group_count, 0); // need at least 1 in the group
        if count.is_some() { assert_ne!(count.unwrap(), 0); }

        if file_path.exists() {
            return Err(IOError::new(ErrorKind::AlreadyExists, format!("The SSTable {:?} already exists", file_path)));
        }

        // create the RecordFile that holds all the data for the SSTable
        let mut rec_file = RecordFile::new(file_path, SSTABLE_HEADER, buffer_size, cache_size)?;

        debug!("Created RecordFile: {:?}", rec_file);

        let mut sstable_info = SSTableInfo {
            record_count: 0,
            group_count: group_count,
            indices: vec!(),
            smallest_key: vec!(),
            largest_key: vec!(),
            oldest_ts: 0
        };

        let mut group_indices = vec![0x00 as u64; group_count as usize];
        let mut cur_group_indices_offset;
        let mut cur_key :Vec<u8> = vec![];
        let mut cur_ts ;

        // make space for the record_group_indices
        let record_group_indices_buff = serialize_u64_exact(&group_indices);
        cur_group_indices_offset = rec_file.append(&record_group_indices_buff)?;

        // keep fetching from this iterator
        while let Some(r) = records.next() {
            let rec :&Record = r.borrow();

//            debug!("GOT REC: {:?}", rec);

            // quick sanity check to ensure we're in sorted order
            if sstable_info.record_count != 0 && rec.key() <= cur_key {
                panic!("Got records in un-sorted order: {} <= {}", buf2string(&rec.key()), buf2string(&cur_key));
            }

            // take care of our group_indices
            if sstable_info.record_count != 0 && sstable_info.record_count % group_count as u64 == 0 {
                // write the current record_group_indices to disk
                let record_group_indices_buff = serialize_u64_exact(&group_indices);
                rec_file.write_at(cur_group_indices_offset, &record_group_indices_buff, false)?;

                // reset the record_group_indices, and write it to the new location
                group_indices = vec![0x00 as u64; group_count as usize];
                let record_group_indices_buff = serialize_u64_exact(&group_indices);
                cur_group_indices_offset = rec_file.append(&record_group_indices_buff)?;
            }

            // append the record to the end of the file, without flushing
            let loc = rec_file.append_record(rec)?;

            // add to our group index
            group_indices[(sstable_info.record_count % group_count as u64) as usize] = loc;

            // add to the top-level indices if needed
            if sstable_info.record_count % group_count as u64 == 0 {
                sstable_info.indices.push(loc);
            }

            // record our current key and ts for use later
            cur_key = rec.key();
            cur_ts = rec.created();

            // the first time through we set the smallest key, and oldest time
            if sstable_info.record_count == 0 {
                sstable_info.smallest_key = cur_key.to_vec();
                sstable_info.oldest_ts = cur_ts;
            } else if cur_ts > sstable_info.oldest_ts {
                sstable_info.oldest_ts = cur_ts;
            }

            // update our record count
            sstable_info.record_count += 1;

            // break out if we've reached our limit
            if count.is_some() && count.expect("Error unwrapping Some(count)") <= sstable_info.record_count {
                debug!("Read enough records: {} > {}", count.unwrap(), sstable_info.record_count);
                break;
            }
        }

        // write-out our current group_indices
        let record_group_indices_buff = serialize_u64_exact(&group_indices);
        rec_file.write_at(cur_group_indices_offset, &record_group_indices_buff, false)?;

        // update our largest key
        sstable_info.largest_key = cur_key;

        // append our info as the last record, and flush to disk
        let info_buff = to_vec(&sstable_info).expect("Error serializing SSTableInfo");
        rec_file.append(&info_buff).expect("Error writing SSTableInfo");
        rec_file.flush();

        // create our SSTable
        let sstable = SSTable {
            rec_file: rec_file,
            info: sstable_info
        };

        debug!("Created SSTable: {:?}", sstable);

        Ok(sstable)
    }

    fn binary_search_by<'a, T, F>(array: &'a [T], mut f: F) -> Result<usize, usize>
        where F: FnMut(&'a T) -> Ordering
    {
        let mut base = 0usize;
        let mut s = array;

        loop {
            let (head, tail) = s.split_at(s.len() >> 1);
            if tail.is_empty() {
                return Err(base);
            }
            match f(&tail[0]) {
                Less => {
                    base += head.len() + 1;
                    s = &tail[1..];
                }
                Greater => s = head,
                Equal => return Ok(base + head.len()),
            }
        }
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Record>, IOError> {
        // check if the key is in the range of this SSTable
        if self.info.record_count == 0 || key < self.info.smallest_key || self.info.largest_key < key {
            return Ok(None);
        }

        // binary search using the indices
        let top_index_res = SSTable::binary_search_by(&self.info.indices, |index| {
            let rec_buff = self.rec_file.read_at(*index).expect("Error reading SSTable");
            let rec = Record::deserialize(rec_buff);

            rec.key().cmp(&key)
        });

        let start_offset = self.info.indices[match top_index_res {
            Ok(i) => i,
            Err(i) => i-1
        }];

        debug!("Top-level binary search: {:?} -> {}", top_index_res, start_offset);

        // need to fetch the group indices array from rec_file
        let group_indices_offset = start_offset - ((self.info.group_count as usize * U64_SIZE) + U32_SIZE) as u64;
        let group_indices_buff = self.rec_file.read_at(group_indices_offset)?;
        let mut group_indices = deserialize_u64_exact(&group_indices_buff);

        // chop the array when we find our first zero offset
        group_indices = group_indices.into_iter().take_while(|i| *i != 0x00 as u64).collect::<Vec<_>>();

        // save the record so we don't need to re-read it
        let mut rec :Record = Record::new(Vec::<u8>::new(), Some(Vec::<u8>::new()));

        // binary search through the group indices
        let group_index_res = SSTable::binary_search_by(&group_indices, |index| {
            let rec_buff = self.rec_file.read_at(*index).expect("Error reading SSTable");
            rec = Record::deserialize(rec_buff);

            rec.key().cmp(&key)
        });

        debug!("Group binary search: {:?}", group_index_res);

        // convert from binary_search result to actual result
        let ret = match group_index_res {
            Ok(_) => Some(rec),
            Err(_) => None
        };

        Ok(ret)
    }

    pub fn iter(&self) -> Iter {
        return Iter {
            sstable: self,
            cur_record: 0,
            cur_offset: if self.info.record_count == 0 { 0 } else { self.info.indices[0] }
        }
    }

    pub fn oldest_ts(&self) -> u64 {
        self.info.oldest_ts
    }

    pub fn record_count(&self) -> u64 { self.info.record_count }

    pub fn file_path(&self) -> PathBuf { self.rec_file.file_path() }
}

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
            .field("record_count", &self.record_count)
            .field("group_count", &self.group_count)
            .field("smallest_key", &buf2string(&self.smallest_key))
            .field("largest_key", &buf2string(&self.largest_key))
            .field("oldest_ts", &self.oldest_ts)
            .field("indices", &self.indices)
            .finish()
    }
}


pub struct Iter<'a> {
    sstable: &'a SSTable,
    cur_record: u64,
    cur_offset: u64
}

impl<'a> Iterator for Iter<'a> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_record == self.sstable.info.record_count {
            return None;
        }

        let rec_buff = self.sstable.rec_file.read_at(self.cur_offset).expect("Error reading SSTable");
        let rec_buff_len = rec_buff.len();
        let rec = Record::deserialize(rec_buff);

        self.cur_record += 1;
        self.cur_offset += (rec_buff_len + U32_SIZE) as u64;

        // need to skip over the group index records
        if self.cur_record % self.sstable.info.group_count as u64 == 0 {
            self.cur_offset += ((self.sstable.info.group_count as usize * U64_SIZE) + U32_SIZE) as u64;
        }

        Some(rec)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.sstable.info.record_count as usize, Some(self.sstable.info.record_count as usize))
    }
}


impl PartialOrd for SSTable {
    fn partial_cmp(&self, other: &SSTable) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SSTable {
    fn cmp(&self, other: &SSTable) -> Ordering {
        self.info.smallest_key.cmp(&other.info.smallest_key)
    }
}

impl PartialEq for SSTable {
    fn eq(&self, other: &SSTable) -> bool {
        self.info.smallest_key == other.info.smallest_key &&
        self.info.largest_key  == other.info.largest_key &&
        self.info.record_count == other.info.record_count
    }
}

impl Eq for SSTable { }


#[cfg(test)]
mod tests {
    use sstable::SSTable;
    use record::Record;
    use std::path::PathBuf;
    use std::iter;
    use rand::{thread_rng, Rng};
    use std::fs::create_dir;
    use simple_logger;
    use serde_utils::serialize_u64_exact;
    use ::LOGGER_INIT;

    const BUFFER_SIZE: usize = 4069;
    const CACHE_SIZE: usize = 100;


    fn gen_dir() -> PathBuf {
        LOGGER_INIT.call_once(|| simple_logger::init().unwrap()); // this will panic on error

        let tmp_dir: String = thread_rng().gen_ascii_chars().take(6).collect();
        let ret_dir = PathBuf::from("/tmp").join(format!("kvs_{}", tmp_dir));

        debug!("CREATING TMP DIR: {:?}", ret_dir);

        create_dir(&ret_dir).unwrap();

        return ret_dir;
    }

    fn new_open(num_records: usize, group_size: u32, use_size: bool) -> SSTable {
        let db_dir = gen_dir();
        let mut records = vec![];

        for i in 0..num_records {
            let rec = Record::new(serialize_u64_exact(&vec![i as u64]), Some(serialize_u64_exact(&vec![i as u64])));

            records.push(rec);
        }

        {
            SSTable::new(&db_dir.join("test.data"), &mut records.iter(), group_size, if use_size { Some(num_records as u64) } else { None }, BUFFER_SIZE, CACHE_SIZE).unwrap();
        }

        SSTable::open(&db_dir.join("test.data"), BUFFER_SIZE, CACHE_SIZE).unwrap()
    }

    #[test]
    fn test_new_empty() {
        let db_dir = gen_dir();

        SSTable::new(&db_dir.join("test.data"), &mut iter::empty::<Record>(), 10, None, BUFFER_SIZE, CACHE_SIZE).unwrap();
    }

    #[test]
    fn test_new_100_2() {
        let f1 = new_open(100, 2, false);
        let f2 = new_open(100, 2, true);

        assert_eq!(f1.record_count(), f2.record_count());
    }

    #[test]
    fn test_new_10000_10() {
        let f1 = new_open(10000, 10, false);
        let f2 = new_open(10000, 10, true);

        assert_eq!(f1.record_count(), f2.record_count());
    }

    #[test]
    fn test_new_1_10() {
        let f1 = new_open(1, 10, false);
        let f2 = new_open(1, 10, true);

        assert_eq!(f1.record_count(), f2.record_count());
    }

    #[test]
    fn test_new_1_1() {
        let f1 = new_open(1, 1, false);
        let f2 = new_open(1, 1, true);

        assert_eq!(f1.record_count(), f2.record_count());
    }

    fn get(num_records: usize, group_size: u32) {
        let sstable = new_open(num_records, group_size, false);

        debug!("GET TEST SSTABLE: {:?}", sstable);

        // look for all the records
        for i in 0..num_records {
            debug!("LOOKING FOR: {}", i);
            let ret = sstable.get(serialize_u64_exact(&vec![i as u64])).unwrap();

            assert!(ret.is_some());
            assert_eq!(ret.unwrap(), Record::new(serialize_u64_exact(&vec![i as u64]), Some(serialize_u64_exact(&vec![i as u64]))));
        }
    }

    #[test]
    fn test_get_100_2() {
        get(100, 2);
    }

    #[test]
    fn test_get_10000_10() {
        get(10000, 10);
    }

    #[test]
    fn test_get_1_10() {
        get(1, 10);
    }

    #[test]
    fn test_get_1_1() {
        get(1, 1);
    }

    fn iterate(num_records: usize, group_size: u32) {
        let sstable = new_open(num_records, group_size, false);

        debug!("ITER TEST SSTABLE: {:?}", sstable);

        let mut it = sstable.iter();
        let mut count = 0;

        while let Some(_rec) = it.next() {
            count += 1;
        }

        assert_eq!(num_records, count);
    }

    #[test]
    fn test_iter_0_2() {
        iterate(0, 2);
    }

    #[test]
    fn test_iter_100_2() {
        iterate(100, 2);
    }

    #[test]
    fn test_iter_10000_10() {
        iterate(10000, 10);
    }

    #[test]
    fn test_iter_1_10() {
        iterate(1, 10);
    }

    #[test]
    fn test_iter_1_1() {
        iterate(1, 1);
    }
}