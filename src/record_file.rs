use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use lru_cache::LruCache;
use positioned_io::{ReadAt, WriteAt, WriteBytesExt as PositionedWriteBytesExt, ReadBytesExt as PositionedReadBytesExt};

use std::cell::RefCell;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::fs::{File, OpenOptions};
use std::io::{Error as IOError, ErrorKind, Read, Seek, SeekFrom, Write, BufWriter};
use std::path::PathBuf;
use std::sync::{RwLock, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use record::{Record, VALUE_SENTINEL};

use U32_SIZE;
use U64_SIZE;
use std::borrow::BorrowMut;
use std::ops::DerefMut;

/// This struct represents the on-disk format of the RecordFile
/// |---------------------------|
/// | H E A D E R ...           |
/// |---------------------------|
/// | num records, 4-bytes      |
/// |---------------------------|
/// | last record, 8-bytes      |
/// |---------------------------|
/// | record size, 4-bytes      |
/// |---------------------------|
/// | record ...                |
/// |---------------------------|
/// | ...                       |
/// |---------------------------|

pub const BAD_COUNT: u32 = 0xFFFFFFFF;


/// Record file
pub struct RecordFile {
    fd: File,           // actual file
    writer: RwLock<BufWriter<File>>,  // buffered writer
    file_path: PathBuf, // location of the file on disk
    record_count: u32,  // number of records in the file
    header_len: usize,  // length of the header
    last_record: u64,   // the start of the last record
    record_cache: Mutex<LruCache<u64, Vec<u8>>>,
    cache_hit: AtomicUsize,
    cache_count: AtomicUsize
}

pub fn buf2string(buf: &[u8]) -> String {
    let mut ret = String::new();

    for &b in buf {
        ret.push_str(format!("{:02X} ", b).as_str());
    }

    return ret;
}

fn rec_to_string(size: u32, rec: &[u8]) -> String {
    let mut dbg_buf = String::new();

    dbg_buf.push_str(format!("{:08X} ", size).as_str());
    dbg_buf.push_str(buf2string(&rec).as_str());

    return dbg_buf;
}

impl RecordFile {
    pub fn new(file_path: &PathBuf, header: &[u8], buffer_size: usize, cache_size: usize) -> Result<RecordFile, IOError> {
        debug!("Attempting to open file: {}", file_path.display());

        let mut fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)?;
        let mut record_count = 0;
        let mut last_record = (header.len() + U32_SIZE + U64_SIZE) as u64;

        fd.seek(SeekFrom::Start(0))?;

        // check to see if we're opening a new/blank file or not
        if fd.metadata()?.len() == 0 {
            fd.write(header)?;
            fd.write_u32::<LE>(BAD_COUNT)?; // record count
            fd.write_u64::<LE>(last_record)?;

            debug!(
                "Created new RecordFile {} with count {} and last record {}",
                file_path.display(),
                record_count,
                last_record
            );
        } else {
            let mut header_buff = vec![0; header.len()];

            fd.read_exact(&mut header_buff)?;

            if header != header_buff.as_slice() {
                return Err(IOError::new(
                    ErrorKind::InvalidData,
                    format!("Invalid file header for: {}", file_path.display()),
                ));
            }

            record_count = fd.read_u32::<LE>()?;

            if record_count == BAD_COUNT {
                //TODO: Add a check in here
                panic!("Opened a bad record file; record_count == BAD_COUNT");
            }

            last_record = fd.read_u64::<LE>()?;

            fd.seek(SeekFrom::End(0))?; // go to the end of the file

            debug!(
                "Opened RecordFile {} with count {} and eof {}",
                file_path.display(),
                record_count,
                last_record
            );
        }

        let writer = RwLock::new(BufWriter::with_capacity(buffer_size, fd.try_clone().expect("Unable to create RecordFile writer")));

        Ok(RecordFile {
            fd,
            writer,
            file_path: PathBuf::from(file_path),
            record_count,
            header_len: header.len(),
            last_record,
            record_cache: Mutex::new(LruCache::new(cache_size)),
            cache_hit: AtomicUsize::new(0),
            cache_count: AtomicUsize::new(0)
        })
    }

    /// Returns the number of records in this file
    pub fn record_count(&self) -> u32 {
        self.record_count
    }

    pub fn cache_hit_rate(&self) -> f64 {
        self.cache_hit.load(Ordering::Relaxed) as f64 / self.cache_count.load(Ordering::Relaxed) as f64
    }

    pub fn file_path(&self) -> PathBuf {
        self.file_path.clone()
    }

    pub fn last_record(&self) -> Result<Vec<u8>, IOError> {
        self.read_at(self.last_record)
    }

    /// Appends a record to the end of the file without flushing to disk
    /// Returns the location where the record was written
    pub fn append(&mut self, record: &[u8]) -> Result<u64, IOError> {
        let mut writer = self.writer.write().expect("Error getting write lock for writer");
        let rec_loc = writer.seek(SeekFrom::End(0))?;
        let rec_size = record.len();

        writer.write_u32::<LE>(rec_size as u32)?;
        writer.write(record)?;

        self.record_count += 1;
        self.last_record = rec_loc;

        // add to our cache
        self.record_cache.lock().expect("Error getting write lock for record_cache").insert(rec_loc, record.to_owned());

        Ok(rec_loc)
    }

    pub fn append_record(&mut self, rec: &Record) -> Result<u64, IOError> {
        let mut writer = self.writer.write().expect("Error getting write lock for writer");
        let rec_loc = writer.seek(SeekFrom::End(0))?;

        rec.serialize(writer.deref_mut())?;

        self.record_count += 1;
        self.last_record = rec_loc;

        Ok(rec_loc)
    }

    pub fn flush(&mut self) {
        let mut writer = self.writer.write().expect("Error getting write lock for writer");
        writer.seek(SeekFrom::Start(self.header_len as u64)).expect("Error seeking");
        writer.write_u32::<LE>(self.record_count).expect("Error writing record count"); // cannot return an error, so best attempt
        writer.write_u64::<LE>(self.last_record).expect("Error writing last record");  // write out the end of the file
        writer.flush().expect("Error flushing to disk");
    }

    /// Read a record from a given offset
    pub fn read_at(&self, file_offset: u64) -> Result<Vec<u8>, IOError> {
        self.cache_count.fetch_add(1, Ordering::Relaxed);

        {
            if let Some(ret) = self.record_cache.lock().expect("Error getting read lock for record_cache").get_mut(&file_offset) {
                self.cache_hit.fetch_add(1, Ordering::Relaxed); // increase our hit counter
                return Ok(ret.to_vec());
            }
        }

        // need to flush any existing writes to disk
        { self.writer.write().expect("Error getting write lock for writer").flush()?; }

        // read the record size and create a buffer for it
        let rec_size = self.fd.read_u32_at::<LE>(file_offset)?;
        let mut rec_buff = vec![0; rec_size as usize];

//        debug!("ATTEMPTING TO READ RECORD OF SIZE {} FROM {}", rec_size, file_offset);

        // read in that buffer
        self.fd.read_exact_at(file_offset + U32_SIZE as u64, &mut rec_buff)?;

        // add to our cache
        self.record_cache.lock().expect("Error getting write lock for record_cache").insert(file_offset, rec_buff.to_owned());

        Ok(rec_buff)
    }

    /// Writes a record at a given offset... this is potentially VERY dangerous
    pub fn write_at(&mut self, file_offset: u64, record: &[u8], size_check: bool) -> Result<(), IOError> {
        if size_check {
            let rec = self.read_at(file_offset)?;
//            debug!("{} {}", rec_to_string(rec.len() as u32, &rec), rec_to_string(record.len() as u32, record));
            assert_eq!(rec.len(), record.len());
        }

        self.fd.write_u32_at::<LE>(file_offset, record.len() as u32)?;
        self.fd.write_all_at(file_offset + U32_SIZE as u64, &record)?;

        // add to our cache
        self.record_cache.lock().expect("Error getting write lock for record_cache").insert(file_offset, record.to_owned());

        Ok( () )
    }

    pub fn iter(&self) -> Iter {
        Iter {
            record_file: self,
            cur_offset: Some((self.header_len + U32_SIZE + U64_SIZE) as u64)
        }
    }

    /// Creates an iterator from a given offset
    pub fn iter_from(&self, offset: u64) -> Iter {
        Iter {
            record_file: self,
            cur_offset: Some(offset)
        }
    }

}

impl Drop for RecordFile {
    fn drop(&mut self) {
        self.flush();

        debug!("DROP: {:?}: records: {}; last record: {}", self.file_path, self.record_count, self.last_record);
    }
}

impl Debug for RecordFile {
    fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
        formatter.debug_struct("RecordFile")
            .field("file_path", &self.file_path)
            .field("record_count", &self.record_count)
            .field("last_record", &self.last_record)
            .finish()
    }
}

pub struct Iter<'a> {
    record_file: &'a RecordFile,
    cur_offset: Option<u64>
}

impl<'a> Iterator for Iter<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_offset.is_none() {
            return None;
        }

        let rec = match self.record_file.read_at(self.cur_offset.unwrap()) {
                Err(e) => panic!("Error reading file at {}: {}", self.cur_offset.unwrap(), e.to_string()),
                Ok(r) => r
        };

        // update our current record pointer
        self.cur_offset = Some((self.cur_offset.unwrap() as usize + rec.len() + U32_SIZE) as u64);

        if self.cur_offset.unwrap() == self.record_file.last_record {
            self.cur_offset = None;
        }

        Some(rec)
    }
}

pub struct RecordFileIterator {
    record_file: RefCell<RecordFile>,
    cur_record: u32,
}

impl IntoIterator for RecordFile {
    type Item = Vec<u8>;
    type IntoIter = RecordFileIterator;

    fn into_iter(self) -> Self::IntoIter {
        debug!("Created RecordFileIterator");

        RecordFileIterator {
            record_file: RefCell::new(self),
            cur_record: 0,
        }
    }
}

impl Iterator for RecordFileIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        // move to the start of the records if this is the first time through
        if self.cur_record == 0 {
            self.record_file.get_mut().writer.write().expect("Error getting write lock for record_file").flush().expect("Failed to flush writer to disk");
            let offset = (self.record_file.borrow().header_len as usize + U32_SIZE + U64_SIZE) as u64;
            self.record_file.get_mut().fd.seek(SeekFrom::Start(offset)).unwrap();
        }

        // invariant when we've reached the end of the records
        if self.cur_record >= self.record_file.borrow().record_count {
            return None;
        }

        let rec_size = match self.record_file.get_mut().fd.read_u32::<LE>() {
            Err(e) => {
                panic!("Error reading record file: {}", e.to_string());
            }
            Ok(s) => s,
        };

        let mut msg_buff = vec![0; rec_size as usize];

        debug!("Reading record of size {}", rec_size);

        if let Err(e) = self.record_file.get_mut().fd.read_exact(&mut msg_buff) {
            panic!("Error reading record file: {}", e.to_string());
        }

        self.cur_record += 1; // up the count of records read

        Some(msg_buff)
    }
}

pub struct MutRecordFileIterator<'a> {
    record_file: RefCell<&'a mut RecordFile>,
    cur_record: u32,
}

impl<'a> IntoIterator for &'a mut RecordFile {
    type Item = Vec<u8>;
    type IntoIter = MutRecordFileIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        debug!("Created RecordFileIterator");

        MutRecordFileIterator {
            record_file: RefCell::new(self),
            cur_record: 0,
        }
    }
}

impl<'a> Iterator for MutRecordFileIterator<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        // move to the start of the records if this is the first time through
        if self.cur_record == 0 {
            let offset = (self.record_file.borrow().header_len as usize + U32_SIZE + U64_SIZE) as u64;
            self.record_file
                .get_mut()
                .fd
                .seek(SeekFrom::Start(offset))
                .unwrap();
        }

        // invariant when we've reached the end of the records
        if self.cur_record >= self.record_file.borrow().record_count {
            return None;
        }

        let rec_size = match self.record_file.get_mut().fd.read_u32::<LE>() {
            Err(e) => {
                panic!("Error reading record file: {}", e.to_string());
            }
            Ok(s) => s,
        };

        let mut msg_buff = vec![0; rec_size as usize];

        debug!("Reading record of size {}", rec_size);

        if let Err(e) = self.record_file.get_mut().fd.read_exact(&mut msg_buff) {
            panic!("Error reading record file: {}", e.to_string());
        }

        self.cur_record += 1; // up the count of records read

        Some(msg_buff)
    }
}

#[cfg(test)]
mod tests {
    use record_file::RecordFile;

    use simple_logger;
    use std::path::PathBuf;
    use std::io::{Seek, SeekFrom, Write};
    use rand::{thread_rng, Rng};
    use ::LOGGER_INIT;

    const BUFFER_SIZE: usize = 4069;
    const CACHE_SIZE: usize = 100;

    fn gen_file() -> PathBuf {
        LOGGER_INIT.call_once(|| simple_logger::init().unwrap()); // this will panic on error

        let tmp_name: String = thread_rng().gen_ascii_chars().take(6).collect();
        let ret_file = PathBuf::from("/tmp").join(format!("rec_file_{}.data", tmp_name));

        debug!("CREATING TMP FILE: {:?}", ret_file);

        return ret_file;
    }

    #[test]
    fn new() {
        let file = gen_file();

        let mut rec_file = RecordFile::new(&file, "ABCD".as_bytes(), BUFFER_SIZE, CACHE_SIZE).unwrap();

        rec_file.fd.seek(SeekFrom::End(0)).unwrap();
        rec_file.fd.write("TEST".as_bytes()).unwrap();
    }

    #[test]
    fn new_open() {
        let file = gen_file();

        {
            let mut rec_file = RecordFile::new(&file, "ABCD".as_bytes(), BUFFER_SIZE, CACHE_SIZE).unwrap();

            rec_file.fd.seek(SeekFrom::End(0)).unwrap();
            rec_file.fd.write("TEST".as_bytes()).unwrap();
        }

        RecordFile::new(&file, "ABCD".as_bytes(), BUFFER_SIZE, CACHE_SIZE).unwrap();
    }

    #[test]
    fn append() {
        let file = gen_file();

        let mut rec_file = RecordFile::new(&file, "ABCD".as_bytes(), BUFFER_SIZE, CACHE_SIZE).unwrap();

        // put this here to see if it messes with stuff
        rec_file.fd.seek(SeekFrom::End(0)).unwrap();
        rec_file.fd.write("TEST".as_bytes()).unwrap();

        let rec = "THE_RECORD".as_bytes();

        let loc = rec_file.append(rec).unwrap();
        assert_eq!(loc, rec_file.last_record as u64);

        let loc2 = rec_file.append(rec).unwrap();
        assert_eq!(loc2, rec_file.last_record as u64);
    }

    #[test]
    fn read_at() {
        let file = gen_file();

        let mut rec_file = RecordFile::new(&file, "ABCD".as_bytes(), BUFFER_SIZE, CACHE_SIZE).unwrap();
        let rec = "THE_RECORD".as_bytes();

        rec_file.append(rec).unwrap();
        let loc = rec_file.append(rec).unwrap();

        let rec_read = rec_file.read_at(loc).unwrap();

        assert_eq!(rec, rec_read.as_slice());
    }

    #[test]
    fn iterate() {
        let file = gen_file();

        let mut rec_file = RecordFile::new(&file, "ABCD".as_bytes(), BUFFER_SIZE, CACHE_SIZE).unwrap();

        let rec = "THE_RECORD".as_bytes();

        let loc = rec_file.append(rec).unwrap();
        assert_eq!(loc, rec_file.last_record as u64);

        let loc2 = rec_file.append(rec).unwrap();
        assert_eq!(loc2, rec_file.last_record as u64);

        for rec in rec_file.into_iter() {
            assert_eq!("THE_RECORD".as_bytes(), rec.as_slice());
        }
    }
}
