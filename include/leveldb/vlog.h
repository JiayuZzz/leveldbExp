//
// Created by wujy on 8/21/18.
//

#ifndef MULTI_BF_LSM_VLOG_H
#define MULTI_BF_LSM_VLOG_H

#include "db.h"
#include <string>
#include <vector>

using std::string;

namespace leveldb {

    struct LEVELDB_EXPORT VlogOptions : Options {
        int numThreads = 32;
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
        * use '$' to seperate offset and value size, key size and value size, value size and key
        */
        virtual Status Put(const WriteOptions writeOptions, const string &key, const string &val) = 0;

        /*
        * Get value offset and value size from indexDB
        * Get value from vlog
        */
        virtual Status Get(const ReadOptions readOptions, const string &key, string *val) = 0;

        virtual Status Delete(const WriteOptions writeOptions, const string &key) = 0;

        /*
         * multi-threading range query
         * return num scan keys
        */
        virtual size_t Scan(const ReadOptions readOptions, const string &start, size_t num, std::vector<string> &keys,
                    std::vector<string> &values) = 0;

        virtual bool GetProperty(const Slice& property, std::string* value)=0;
    };
}

#endif //MULTI_BF_LSM_VLOG_H