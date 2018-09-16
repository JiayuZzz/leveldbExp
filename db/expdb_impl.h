//
// Created by wujy on 9/9/18.
//

#ifndef LEVELDB_EXPDB_IMPL_H
#define LEVELDB_EXPDB_IMPL_H

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/expdb.h"
#include "leveldb/threadpool.h"
#include "leveldb/statistics.h"
#include <string>
#include <port/port.h>
#include "db/dbformat.h"


namespace leveldb {
    class MemTable;

    class ExpDBImpl : public ExpDB {
    public:

        ExpDBImpl(ExpOptions options, const std::string &dbname, const std::string &vlogdir, Status& s);
        ~ExpDBImpl();
        // Implementations of the VlogDB interface
        virtual Status Put(const WriteOptions writeOptions, const string &key, const string &val);

        //write batch to memtable
        virtual Status Write(const leveldb::WriteOptions &options, leveldb::WriteBatch *updates);

        virtual Status Get(const ReadOptions readOptions, const string &key, string *val);

        virtual Status Delete(const WriteOptions writeOptions, const string &key);

        virtual size_t Scan(const ReadOptions readOptions, const string &start, size_t num, std::vector<string> &keys,
                            std::vector<string> &values);

        virtual bool GetProperty(const Slice &property, std::string *value);

    private:
        struct ScanMeta;
        struct Writer;
        DB *indexDB_;
        ExpOptions options_;
        ThreadPool *threadPool_;
        const InternalKeyComparator internal_comparator_;
        MemTable* mem_;
        MemTable* imm_;
        port::AtomicPointer has_imm_;
        const std::string vlogDir_;
        Env* env_;
        port::Mutex mutex_;
        std::deque<Writer*> writers_ GUARDED_BY(mutex_);
        WriteBatch* tmp_batch_ GUARDED_BY(mutex_);
        port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
        std::atomic<int> next_vlog_num_;
        bool background_compaction_scheduled_;
        std::vector<FILE*> openedVlog;

        FILE* OpenVlog(int vlogNum);  // open a vlog file
        WriteBatch* BuildBatchGroup(Writer** last_writer)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);
        Status MakeRoomForWrite(bool force /* compact even if there is room? */)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);
        void MaybeScheduleCompaction();
        Status CompactMemtable();
        Status WriteVlog(MemTable* mem);
        Status readValue(string &valueInfo, string *val);
    };
}


#endif //LEVELDB_EXPDB_IMPL_H
