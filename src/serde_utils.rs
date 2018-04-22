use std::io::Cursor;

use byteorder::{ReadBytesExt, WriteBytesExt, BE};

use U64_SIZE;

pub fn serialize_u64_exact(array: &Vec<u64>) -> Vec<u8> {
    let ret = vec![0x00 as u8; array.len() * U64_SIZE];
    let mut cursor = Cursor::new(ret);

    for a in array {
        cursor.write_u64::<BE>(*a);
    }

    return cursor.into_inner();
}

pub fn deserialize_u64_exact(buff: &Vec<u8>) -> Vec<u64> {
    if buff.len() % U64_SIZE != 0 {
        panic!("Buffer is not an exact multiple of u64: {} % {} != 0", buff.len(), U64_SIZE);
    }

    let mut cursor = Cursor::new(buff);
    let mut ret = Vec::<u64>::new();

    while cursor.position() < buff.len() as u64 {
        ret.push(cursor.read_u64::<BE>().expect("Error deserializing u64"));
    }

    return ret;
}


#[cfg(test)]
mod tests {
    use serde_utils::{serialize_u64_exact, deserialize_u64_exact};
    use record_file::buf2string;

    #[test]
    fn serialize() {
        let array1 = vec![0xFFFFFFFFFFFFFFFF as u64; 8];
        let array2 = vec![0xFFFFFFFFFFFFFF as u64; 8];
        let array3 = vec![0xFFFFFFFF as u64; 8];
        let array4 = vec![0 as u64; 8];

        let ret1 = serialize_u64_exact(&array1);
        let ret2 = serialize_u64_exact(&array2);
        let ret3 = serialize_u64_exact(&array3);
        let ret4 = serialize_u64_exact(&array4);

        assert_eq!(ret1.len(), ret2.len());
        assert_eq!(ret2.len(), ret3.len());
        assert_eq!(ret3.len(), ret4.len());
    }


    #[test]
    fn deserialize() {
        let buff1 = vec![0xFF as u8; 8];
        let buff2 = vec![0xFF as u8; 16];

        let ret1 = deserialize_u64_exact(&buff1);
        let ret2 = deserialize_u64_exact(&buff2);

        assert_eq!(1, ret1.len());
        assert_eq!(0xFFFFFFFFFFFFFFFF, ret1[0]);

        assert_eq!(2, ret2.len());
        assert_eq!(0xFFFFFFFFFFFFFFFF, ret2[0]);
        assert_eq!(0xFFFFFFFFFFFFFFFF, ret2[1]);
    }

}