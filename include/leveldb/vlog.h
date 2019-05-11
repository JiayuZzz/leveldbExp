//
// Created by wujy on 8/21/18.
//

#ifndef MULTI_BF_LSM_VLOG_H
#define MULTI_BF_LSM_VLOG_H

#include "db.h"
#include <string>
#include <vector>

namespace leveldb {

    struct LEVELDB_EXPORT VlogOptions : Options {
        int numThreads = 32;
        size_t vlogSize = 128*1024*1024;   // single vlog file max size
        size_t numOpenfile = 50000;
        size_t gcAfterExe = 0;
    };

    class VlogDB {
    public:
        /* dbname is index lsm-tree, vlogname is vlog filename */
        static Status Open(VlogOptions options, const std::string &dbname, const std::string &vlogname, VlogDB **db);

        VlogDB() = default;
        VlogDB(const VlogDB&) = delete;
        DB& operator=(const VlogDB&) = delete;

        virtual ~VlogDB();


        /*
        * indexDB: <key,offset+value size>
        * vlog: <key size, value size, key, value>
        * use '~' to seperate offset and value size, key size and value size, value size and key
        */
        virtual Status Put(const WriteOptions writeOptions, const std::string &key, const std::string &val) = 0;

        /*
        * Get value offset and value size from indexDB
        * Get value from vlog
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

#endif //MULTI_BF_LSM_VLOG_H