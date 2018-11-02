//
// Created by wujy on 9/9/18.
//

#ifndef LEVELDB_EXPDB_H
#define LEVELDB_EXPDB_H

#include "db.h"
#include <string>
#include <vector>

namespace leveldb {

    struct LEVELDB_EXPORT ExpOptions : Options {
        int numThreads = 32;
        int numOpenfile = 50000;
        size_t vlogMemSize = 4<<24;
    };

    class ExpDB {
    public:
        /* dbname is index lsm-tree, vlogdir is vlog file directory */
        static Status Open(ExpOptions options, const std::string &dbname, const std::string &vlogdir, ExpDB **db);

        ExpDB() = default;
        ExpDB(const ExpDB&) = delete;
        DB& operator=(const ExpDB&) = delete;

        virtual ~ExpDB();


        /*
        * indexDB: <key,vlogfilenumber+offset+value size>
        * vlog: <key size, value size, key, value>
        * use '$' to seperate vlogfilenumber and offset, offset and value size, key size and value size, value size and key
        */
        virtual Status Put(const WriteOptions writeOptions, const std::string &key, const std::string &val) = 0;

        virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;

        /*
        * Get value offset and value size from indexDB
        * Get value from vlog files
        */
        virtual Status Get(const ReadOptions readOptions, const std::string &key, std::string *val) = 0;

        virtual Status Delete(const WriteOptions writeOptions, const std::string &key) = 0;

        /*
         * multi-threading range query
         * return num scan keys
        */
        virtual size_t Scan(const ReadOptions readOptions, const std::string &start, size_t num, std::vector<std::string> &keys,
                            std::vector<std::string> &values) = 0;

        virtual bool GetProperty(const Slice& property, std::string* value)=0;
    };
}

#endif //LEVELDB_EXPDB_H
