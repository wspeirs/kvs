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

template <typename D, typename O, typename S>
void put(long start, long end, string db_name, D *db, O options, bool is_update=false) {
    time_point<Clock> t_start = Clock::now();

    for(long i=start; i < end; ++i) {
        string key = "KEY_" + to_string(i);
        string value = is_update ? (to_string(i) + "_VALUE") : ("VALUE_" + to_string(i));

        S status = db->Put(options, key, value);

        if(!status.ok()) {
            cerr << "ERROR WITH PUT INTO DB: " << db_name << " : " << status.ToString() << key << endl;
            return;
        }
    }

    time_point<Clock> t_end = Clock::now();
    milliseconds diff = duration_cast<milliseconds>(t_end - t_start);

    cout << "Took " << diff.count() << (is_update ? "ms to UPDATE " : "ms to PUT ") << (end-start) << " records into " << db_name << endl;
}


template<typename D, typename O, typename S>
void get(long start, long end, string db_name, D *db, O options) {
    time_point<Clock> t_start = Clock::now();

    if(start < end) {
        for(long i=start; i < end; ++i) {
            string key = "KEY_" + to_string(i);
            string value;

            S status = db->Get(options, key, &value);

            if(!status.ok()) {
                cerr << "ERROR WITH GET FROM: " << db_name << " : " << status.ToString() << key << endl;
                return;
            }
        }
    } else {
        for(long i=start-1; i >= end; --i) {
            string key = "KEY_" + to_string(i);
            string value;

            S status = db->Get(options, key, &value);

            if(!status.ok()) {
                cerr << "ERROR WITH REV GET FROM: " << db_name << " : " << status.ToString() << key << endl;
                return;
            }
        }
    }

    time_point<Clock> t_end = Clock::now();
    milliseconds diff = duration_cast<milliseconds>(t_end - t_start);

    cout << "Took " << diff.count() << "ms to GET " << (start<end ? end-start : start-end) << " records from " << db_name << (start<end ? " in order" : " in reverse order") << endl;
}


template <typename D, typename O, typename S>
void del(long start, long end, string db_name, D *db, O options) {
    time_point<Clock> t_start = Clock::now();

    for(long i=start; i < end; ++i) {
        string key = "KEY_" + to_string(i);

        S status = db->Delete(options, key);

        if(!status.ok()) {
            cerr << "ERROR WITH DELETE FROM: " << db_name << " : " << status.ToString() << key << endl;
            return;
        }
    }

    time_point<Clock> t_end = Clock::now();
    milliseconds diff = duration_cast<milliseconds>(t_end - t_start);

    cout << "Took " << diff.count() << "ms to DELETE " << (end-start) << " records from " << db_name << endl;
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
//    rocksdb::DB* rocks_db;
//    rocksdb::Options rocksdb_options;
//    rocksdb_options.create_if_missing = true;
//    rocksdb_options.IncreaseParallelism();
//    rocksdb_options.OptimizeLevelStyleCompaction();
//
//    rocksdb::Status rocksdb_status = rocksdb::DB::Open(rocksdb_options, rocksdb_tmp_dir, &rocks_db);
//
//    if(!rocksdb_status.ok()) {
//        cerr << "Error opening RocksDB: " << rocksdb_status.ToString() << endl;
//    }


    leveldb::WriteOptions leveldb_write_options;
    leveldb_write_options.sync = false; // sync to disk or not

//    rocksdb::WriteOptions rocksdb_write_options;
//    rocksdb_write_options.sync = false;

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

    const int num = 1000000;

    put<leveldb::DB, leveldb::WriteOptions, leveldb::Status>(0, num, "LevelDB", level_db, leveldb_write_options);

    get<leveldb::DB, leveldb::ReadOptions, leveldb::Status>(0, num, "LevelDB", level_db, leveldb::ReadOptions());
    get<leveldb::DB, leveldb::ReadOptions, leveldb::Status>(num, 0, "LevelDB", level_db, leveldb::ReadOptions());

    del<leveldb::DB, leveldb::WriteOptions, leveldb::Status>(0, num/2, "LevelDB", level_db, leveldb_write_options);

    get<leveldb::DB, leveldb::ReadOptions, leveldb::Status>(num/2, num, "LevelDB", level_db, leveldb::ReadOptions());
    get<leveldb::DB, leveldb::ReadOptions, leveldb::Status>(num, num/2, "LevelDB", level_db, leveldb::ReadOptions());

    put<leveldb::DB, leveldb::WriteOptions, leveldb::Status>(num/2, num, "LevelDB", level_db, leveldb_write_options, true);

    get<leveldb::DB, leveldb::ReadOptions, leveldb::Status>(num/2, num, "LevelDB", level_db, leveldb::ReadOptions());
    get<leveldb::DB, leveldb::ReadOptions, leveldb::Status>(num, num/2, "LevelDB", level_db, leveldb::ReadOptions());

    delete level_db;

    return 0;
}
