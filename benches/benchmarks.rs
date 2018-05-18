#![feature(test)]
#[cfg(test)]
extern crate kvs;
extern crate test;

use kvs::KVS;

extern crate elapsed;
extern crate rand;
extern crate simple_logger;
extern crate log;

use elapsed::measure_time;
use std::path::PathBuf;
use rand::{thread_rng, Rng};
use std::fs::create_dir;
use simple_logger as sl;
use log::Level;

use test::Bencher;


fn put(start: u64, end: u64, db: &mut KVS, is_update: bool) -> u64 {
    let range = start..end;

    let (elapsed, _) = measure_time(|| {
        for i in range {
            let key = format!("KEY_{}", i).as_bytes().to_vec();
            let value = if is_update { format!("{}_VALUE", i) } else { format!("VALUE_{}", i) }.as_bytes().to_vec();

            db.put(key, value);
        }
    });

    println!("Took {} to {} {} records", elapsed, if is_update {"UPDATE"} else {"PUT"}, (end-start));

    elapsed.millis()
}

fn get(start: u64, end: u64, db: &KVS) -> u64 {
    let range = if start < end {
        Box::new(start..end) as Box<Iterator<Item=_>>
    } else {
        Box::new((end..start).rev())
    };

    let (elapsed, _) = measure_time(|| {
        for i in range {
            let key = format!("KEY_{}", i).as_bytes().to_vec();

            let value = db.get(&key).expect(&format!("KEY_{} ({:?}) NOT FOUND", i, key));
        }
    });

    println!("Took {} to GET {} records", elapsed, if start<end {end-start} else {start-end});

    elapsed.millis()
}

fn delete(start: u64, end: u64, db: &mut KVS) -> u64 {
    let range = start..end;

    let (elapsed, _) = measure_time(|| {
        for i in range {
            let key = format!("KEY_{}", i).as_bytes().to_vec();

            db.delete(&key);
        }
    });

    println!("Took {} to DELETE {} records", elapsed, (end-start));

    elapsed.millis()
}


#[bench]
fn full(bencher: &mut Bencher) {
//    sl::init_with_level(Level::Info).unwrap();
//    sl::init_with_level(Level::Debug).unwrap();

    let tmp_dir: String = thread_rng().gen_ascii_chars().take(6).collect();
    let ret_dir = PathBuf::from("/tmp").join(format!("kvs_{}", tmp_dir));

    println!("CREATING TMP DIR: {:?}", ret_dir);

    create_dir(&ret_dir).unwrap();

    let mut kvs = KVS::new(&PathBuf::from(ret_dir)).expect("Error creating KVS");

    let num: u64 = 1_000_000;

    // Working roughly off this: https://www.influxdata.com/blog/benchmarking-leveldb-vs-rocksdb-vs-hyperleveldb-vs-lmdb-performance-for-influxdb/
    // Benchmarks for each:
    //   Put 100M key/value of: KEY_X & VALUE_X
    //   Get 100M keys in order
    //   Get 100M keys in reverse order
    //   Delete 50M keys
    //   Get 50M keys in order
    //   Get 50M keys in reverse order
    //   Put (update) 50M key/value: KEY_X & X_VALUE
    //   Get 50M keys in order
    //   Get 50M keys in reverse order
//    put(0, num, &mut kvs, false);

//    get(0, num, &kvs);
//    get(num, 0, &kvs);
//
//    delete(0, num/2, &mut kvs);
//
//    get(num/2, num, &kvs);
//    get(num, num/2, &kvs);
//
//    put(num/2, num, &mut kvs, true);
//
//    get(num/2, num, &kvs);
//    get(num, num/2, &kvs);
}

#[bench]
fn gets(bencher: &mut Bencher) {
    sl::init_with_level(Level::Info).unwrap();
//    sl::init_with_level(Level::Debug).unwrap();

    let tmp_dir: String = thread_rng().gen_ascii_chars().take(6).collect();
    let ret_dir = PathBuf::from("/tmp").join(format!("kvs_{}", tmp_dir));

    println!("CREATING TMP DIR: {:?}", ret_dir);

    create_dir(&ret_dir).unwrap();

    let mut kvs = KVS::new(&PathBuf::from(ret_dir)).expect("Error creating KVS");

    let num: u64 = 100_000;

    put(0, num, &mut kvs, false);

    let mut sum = 0;

    sum += get(0, num, &kvs);
    sum += get(0, num, &kvs);
    sum += get(0, num, &kvs);

    println!("Avg: {}ms", sum / 3);

//    get(num, 0, &kvs);
}
