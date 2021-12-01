use std::cmp::Ordering;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::io::{Cursor, Error as IOError, Write, Read};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};

use crate::U32_SIZE;
use crate::U64_SIZE;

use crate::record_file::buf2string;

use crate::kvs::get_timestamp;

pub const VALUE_SENTINEL: u64 = 0xFFFF_FFFF_FFFF_FFFF;

#[derive(Serialize, Deserialize, Clone)]
pub struct Record {
    key: Vec<u8>,
    value: Option<Vec<u8>>, // if None, means we're deleting this key
    created: u64, // timestamp of when the record was created
    ttl: u64 // timestamp when this record should be deleted
}

impl Record {
    pub fn new(key: Vec<u8>, value: Option<Vec<u8>>) -> Record {
        Record::new_with_ttl(key, value, u64::max_value())
    }

    pub fn new_with_ttl(key: Vec<u8>, value: Option<Vec<u8>>, ttl: u64) -> Record {
        Record {
            key,
            value,
            created: get_timestamp(),
            ttl
        }
    }

    /// Computes the size of the record when serialized without actually serializing it
    pub fn size(&self) -> u32 {
        (U64_SIZE + self.key.len() + // size of the key
            U64_SIZE + if self.value.is_some() { self.value.to_owned().unwrap().len() } else { 0 } + // size of the value
            U64_SIZE + // size of created
            U64_SIZE) as u32 // size of ttl
    }

    /// Serializes the record into a write, appending first the total size of the record
    /// Returns the size of all the data written (size + record), or an error
    pub fn serialize<W>(&self, writer: &mut W) -> Result<u32, IOError> where W: Write {
        writer.write_u32::<LE>(self.size())?; // write out the total size of this serialization

        // the serialization
        writer.write_u64::<LE>(self.key.len() as u64)?; // length of key
        writer.write_all(&self.key)?; // write the actual key's data

        if self.value.is_some() {
            let value = self.value.to_owned().unwrap();

            writer.write_u64::<LE>(value.len() as u64)?; // write the size of the value
            writer.write_all(&value)?;
        } else {
            writer.write_u64::<LE>(VALUE_SENTINEL)?; // sentinel value for no value
        }

        writer.write_u64::<LE>(self.created)?;
        writer.write_u64::<LE>(self.ttl)?;

        Ok(U32_SIZE as u32 + self.size())
    }

    pub fn deserialize(bytes: Vec<u8>) -> Record {
        let mut cursor = Cursor::new(bytes);

        let key_len = cursor.read_u64::<LE>().expect("Error reading key length");
        let mut key = vec![0x00; key_len as usize];

        cursor.read_exact(&mut key).expect("Error reading key");

        let value_len = cursor.read_u64::<LE>().expect("Error reading value length");

        let value = if value_len == VALUE_SENTINEL {
            None
        } else {
            let mut val_buff = vec![0x00; value_len as usize];

            cursor.read_exact(&mut val_buff).expect("Error reading value");

            Some(val_buff)
        };

        let created = cursor.read_u64::<LE>().expect("Error reading created");
        let ttl = cursor.read_u64::<LE>().expect("Error reading ttl");

        Record{ key, value, created, ttl }
    }

    pub fn is_expired(&self, ts: u64) -> bool {
        self.ttl <= ts
    }

    pub fn created(&self) -> u64 {
        self.created
    }

    pub fn ttl(&self) -> u64 {
        self.ttl
    }

    pub fn is_delete(&self) -> bool {
        self.value.is_none()
    }

    pub fn key(&self) -> Vec<u8> {
        self.key.to_owned()
    }

    pub fn value(&self) -> Vec<u8> {
        self.value.to_owned().expect("Tried to get value of delete record")
    }
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Record) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Record) -> Ordering {
        let key_ord = self.key.cmp(&other.key);

        match key_ord {
            Ordering::Equal => self.created.cmp(&other.created),
            Ordering::Less | Ordering::Greater => key_ord
        }
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Record) -> bool {
        self.key == other.key && self.created == self.created
    }
}

impl Eq for Record { }

impl Debug for Record {
    fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
        formatter.debug_struct("Record")
            .field("key", &buf2string(&self.key))
            .field("value", &match &self.value { &None => String::from("None"), &Some(ref v) => buf2string(&v) })
            .field("created", &self.created)
            .field("ttl", &self.ttl)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::record::Record;
    use crate::U32_SIZE;

    #[test]
    fn serialize_value() {
        let rec = Record{ key: vec![123; 8], value: Some(vec![21; 12]), created: 1234, ttl: 6789 };

        let buff = vec![0x00 as u8; rec.size() as usize + U32_SIZE];
        let mut cursor = Cursor::new(buff);

        let ret = rec.serialize(&mut cursor).unwrap();
        let buff_d = cursor.into_inner();

        println!("{:?}", buff_d);

        let rec_d = Record::deserialize(buff_d[4..].to_vec());

        assert_eq!(rec.key, rec_d.key);
        assert_eq!(rec.value, rec_d.value);
        assert_eq!(rec.created, rec_d.created);
        assert_eq!(rec.ttl, rec_d.ttl);
    }

    #[test]
    fn serialize_no_value() {
        let rec = Record{ key: vec![123; 8], value: None, created: 1234, ttl: 6789 };

        let buff = vec![0x00 as u8; rec.size() as usize + U32_SIZE];
        let mut cursor = Cursor::new(buff);

        rec.serialize(&mut cursor).unwrap();
        let buff_d = cursor.into_inner();

        println!("{:?}", buff_d);

        let rec_d = Record::deserialize(buff_d[4..].to_vec());

        assert_eq!(rec.key, rec_d.key);
        assert_eq!(rec.value, rec_d.value);
        assert_eq!(rec.created, rec_d.created);
        assert_eq!(rec.ttl, rec_d.ttl);
    }

}
