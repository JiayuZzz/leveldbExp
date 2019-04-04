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
        virtual Status Put(const WriteOptions writeOptions, const std::string &key, const std::string &val);
        virtual Status Get(const ReadOptions readOptions, const std::string &key, std::string *val);
        virtual Status Delete(const WriteOptions writeOptions, const std::string &key);
        virtual size_t Scan(const ReadOptions readOptions, const std::string &start, size_t num, std::vector<std::string> &keys,
                            std::vector<std::string> &values);
        virtual bool GetProperty(const Slice& property, std::string* value);

        ~VlogDBImpl();

    private:
        DB* indexDB_; //LSM-Tree to store key-valueAddr
        ThreadPool* threadPool_;
        std::string vlogDir_;
        int headVLogNum_;
        int lastVlogNum_;
        VlogOptions options_;
        std::vector<FILE*> openedVlog_;

        // read value from vlog according to valueInfo read from index tree
        Status readValue(std::string &valueInfo, std::string *val);
        Status ReadValuesForScan(const std::vector<std::string> &valueInfos, int begin, int end,
                                             std::vector<std::string> &vals);
        FILE* OpenVlog(int vlogNum);
        size_t VlogSize(int vlogNum);
        void parseValueInfo(const std::string &valueInfo, int &vlogNum, size_t &offset, size_t &valueSize);
        void Recover();
        Status DeleteVlog(int vlogNum);
        // gc at least size space
        Status GarbageCollect(size_t size);
    };
}

#endif //LEVELDB_VLOG_IMPL_H
