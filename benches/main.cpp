#include <chrono>
#include <iostream>
#include <string>

#include <stdlib.h>

#include "leveldb/db.h"

using std::cout;
using std::cerr;
using std::endl;
using std::to_string;
using std::string;

using Clock = std::chrono::steady_clock;
using std::chrono::time_point;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

int main(int argc, char **argv) {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;

    // create a temp directory for use
    char dir_template[] = "/tmp/benchmark_XXXXXX";
    char *tmp_dir = mkdtemp(dir_template);
    string leveldb_tmp_dir = tmp_dir + string("/leveldb");

    cout << "Create benchmark directory: " << tmp_dir << endl;
    leveldb::Status status = leveldb::DB::Open(options, leveldb_tmp_dir, &db);

    if(!status.ok()) {
        cerr << "Error opening LevelDB" << endl;
    }

    std::string key;
    std::string value;

    leveldb::WriteOptions write_options;

    write_options.sync = false; // this will ensure it's sync to disk

    time_point<Clock> start = Clock::now();

    for(int i = 0; i < 1000; ++i) {
        string key = "KEY_" + to_string(i);
        string value = "VALUE_" + to_string(i);

        status = db->Put(write_options, key, value);
    }

    time_point<Clock> end = Clock::now();
    milliseconds diff = duration_cast<milliseconds>(end - start);

    cout << "Took " << diff.count() << "ms to insert 1000 records" << endl;


//    leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
//    if (s.ok()) s = db->Put(leveldb::WriteOptions(), key2, value);
//    if (s.ok()) s = db->Delete(leveldb::WriteOptions(), key1);

    delete db;

    return 0;
}
