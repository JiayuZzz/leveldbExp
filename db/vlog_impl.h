//
// Created by wujy on 9/8/18.
//

#ifndef LEVELDB_VLOG_IMPL_H
#define LEVELDB_VLOG_IMPL_H

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/vlog.h"
#include "leveldb/threadpool.h"
#include "leveldb/statistics.h"

namespace leveldb {
    class VlogDBImpl : public VlogDB {
    public:
        VlogDBImpl(VlogOptions &options, const std::string &dbname, const std::string &vlogname, Status &s);

        // Implementations of the VlogDB interface
        virtual Status Put(const WriteOptions writeOptions, const string &key, const string &val);
        virtual Status Get(const ReadOptions readOptions, const string &key, string *val);
        virtual Status Delete(const WriteOptions writeOptions, const string &key);
        virtual size_t Scan(const ReadOptions readOptions, const string &start, size_t num, std::vector<string> &keys,
                            std::vector<string> &values);
        virtual bool GetProperty(const Slice& property, std::string* value);

        ~VlogDBImpl();

    private:
        DB* indexDB_; //LSM-Tree to store key-valueAddr
        ThreadPool* threadPool_;
        FILE* vlog_;
        VlogOptions options_;

        // read value from vlog according to valueInfo read from index tree
        Status readValue(string &valueInfo, string *val);
    };
}

#endif //LEVELDB_VLOG_IMPL_H
