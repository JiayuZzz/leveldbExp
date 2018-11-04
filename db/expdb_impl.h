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
#include <set>


namespace leveldb {
    class MemTable;

    class ExpDBImpl : public ExpDB {
    public:

        ExpDBImpl(ExpOptions options, const std::string &dbname, const std::string &vlogdir, Status& s);
        ~ExpDBImpl();
        // Implementations of the VlogDB interface
        virtual Status Put(const WriteOptions writeOptions, const std::string &key, const std::string &val);

        //write batch to memtable
        virtual Status Write(const leveldb::WriteOptions &options, leveldb::WriteBatch *updates);

        virtual Status Get(const ReadOptions readOptions, const std::string &key, std::string *val);

        virtual Status Delete(const WriteOptions writeOptions, const std::string &key);

        virtual size_t Scan(const ReadOptions readOptions, const std::string &start, size_t num, std::vector<std::string> &keys,
                            std::vector<std::string> &values);

        virtual bool GetProperty(const Slice &property, std::string *value);

    private:
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
        std::atomic<int> nextVlogNum_;
        bool background_compaction_scheduled_;
        std::vector<FILE*> openedVlog_;
        uint64_t lastSequence_;
        //for record read average files read during scan
        std::set<int> visited;
        uint64_t numVisited;

        FILE* OpenVlog(int vlogNum);  // open a vlog file
        WriteBatch* BuildBatchGroup(Writer** last_writer)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);
        Status MakeRoomForWrite(bool force /* compact even if there is room? */)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);
        void MaybeScheduleCompaction();
        Status CompactMemtable();
        static void BGWork(void *db);
        void BackgroundCall();
        Status WriteVlog(MemTable* mem);
        Status readValue(std::string &valueInfo, std::string *val);
        Status ReadValuesForScan(const std::vector<std::string> &valueInfos, int begin, int end,
                                 std::vector<std::string>& vals);
        void parseValueInfo(const std::string &valueInfo, int& vlogNum, size_t& offset, size_t& valueSize);
        void Recover();
        Status GarbageCollect(size_t size);
        Status DeleteVlog(int vlogNum);
    };
}


#endif //LEVELDB_EXPDB_IMPL_H
