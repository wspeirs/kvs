

pub struct KVS {

}

impl KVS {
    pub fn new() -> KVS {
        return KVS {}
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