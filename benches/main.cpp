#include <chrono>
#include <iostream>
#include <string>

#include <stdlib.h>

#include "leveldb/db.h"

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"


using std::cout;
using std::cerr;
using std::endl;
using std::to_string;
using std::string;

using Clock = std::chrono::steady_clock;
using std::chrono::time_point;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

//
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

template <typename D, typename O, typename S>
void put(long num, string db_name, D *db, O options) {
    time_point<Clock> start = Clock::now();

    for(long i=0; i < num; ++i) {
        string key = "KEY_" + to_string(i);
        string value = "VALUE_" + to_string(i);

        S status = db->Put(options, key, value);

        if(!status.ok()) {
            cerr << "ERROR WRITING TO " << db_name << " DB: " << status.ToString() << endl;
        }
    }

    time_point<Clock> end = Clock::now();
    milliseconds diff = duration_cast<milliseconds>(end - start);

    cout << "Took " << diff.count() << "ms to insert 1000 records into " << db_name << endl;
}

int main(int argc, char **argv) {
    // create a temp directory for use
    char dir_template[] = "/tmp/benchmark_XXXXXX";
    char *tmp_dir = mkdtemp(dir_template);

    cout << "Create benchmark directory: " << tmp_dir << endl;

    string leveldb_tmp_dir = tmp_dir + string("/leveldb");
    string rocksdb_tmp_dir = tmp_dir + string("/rocksdb");

    // create the LevelDB instance
    leveldb::DB* level_db;
    leveldb::Options leveldb_options;
    leveldb_options.create_if_missing = true;

    leveldb::Status leveldb_status = leveldb::DB::Open(leveldb_options, leveldb_tmp_dir, &level_db);

    if(!leveldb_status.ok()) {
        cerr << "Error opening LevelDB: " << leveldb_status.ToString() << endl;
    }

    // create the RocksDB instance
    rocksdb::DB* rocks_db;
    rocksdb::Options rocksdb_options;
    rocksdb_options.create_if_missing = true;
    rocksdb_options.IncreaseParallelism();
    rocksdb_options.OptimizeLevelStyleCompaction();

    rocksdb::Status rocksdb_status = rocksdb::DB::Open(rocksdb_options, rocksdb_tmp_dir, &rocks_db);

    if(!rocksdb_status.ok()) {
        cerr << "Error opening RocksDB: " << rocksdb_status.ToString() << endl;
    }


    leveldb::WriteOptions leveldb_write_options;
    leveldb_write_options.sync = false; // this will ensure it's sync to disk

//    rocksdb::WriteOptions rocksdb_write_options;
//    rocksdb_write_options.sync = false;

    put<leveldb::DB, leveldb::WriteOptions, leveldb::Status>(1000, "LevelDB", level_db, leveldb_write_options);
//    put<rocksdb::DB, rocksdb::WriteOptions, rocksdb::Status>(1000, "RocksDB", rocks_db, rocksdb_write_options);

//    leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
//    if (s.ok()) s = db->Put(leveldb::WriteOptions(), key2, value);
//    if (s.ok()) s = db->Delete(leveldb::WriteOptions(), key1);

    delete level_db;
//    delete rocks_db;

    return 0;
}
