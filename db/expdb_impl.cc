//
// Created by wujy on 9/9/18.
//

#include <zconf.h>
#include <util/mutexlock.h>
#include "expdb_impl.h"
#include "write_batch_internal.h"
#include "db/memtable.h"

namespace leveldb {
    struct ExpDBImpl::Writer {
        Status status;
        WriteBatch *batch;
        bool sync;
        bool done;
        port::CondVar cv;

        explicit Writer(port::Mutex *mu) : cv(mu) {}
    };

    Status ExpDB::Open(leveldb::ExpOptions options, const std::string &dbname, const std::string &vlogdir,
                       leveldb::ExpDB **db) {
        *db = nullptr;
        Status s;
        *db = new ExpDBImpl(options,dbname,vlogdir,s);
        return s;
    }

    ExpDB::~ExpDB() {}

    ExpDBImpl::ExpDBImpl(ExpOptions raw_options, const std::string &dbname, const std::string &vlogdir, Status &s)
            : options_(raw_options), vlogDir_(vlogdir), env_(raw_options.env),
              internal_comparator_(raw_options.comparator),
              background_work_finished_signal_(&mutex_) {
        mem_ = new MemTable(internal_comparator_);
        has_imm_.Release_Store(nullptr);
        threadPool_ = new ThreadPool(options_.numThreads);
        s = DB::Open(raw_options, dbname, &indexDB_);
        if (!s.ok()) return;
        if (access(vlogDir_.c_str(), F_OK) != 0 && options_.create_if_missing) {
            env_->CreateDir(vlogDir_);
        }
        return;
    }

    ExpDBImpl::~ExpDBImpl() {
        mutex_.Lock();
        while(background_compaction_scheduled_){
            background_work_finished_signal_.Wait();
        }
        mutex_.Unlock();
        delete indexDB_;
        delete threadPool_;
    }

    Status ExpDBImpl::Write(const leveldb::WriteOptions &options, leveldb::WriteBatch *my_batch) {
        Writer w(&mutex_);
        w.batch = my_batch;
        w.sync = options.sync;
        w.done = false;
        MutexLock l(&mutex_);
        writers_.push_back(&w);
        while (!w.done && &w != writers_.front()) {
            w.cv.Wait();
        }
        if (w.done) {
            return w.status;
        }
        Status status = MakeRoomForWrite(my_batch == nullptr);//make room for writes here
        Writer *last_writer = &w;
        if (status.ok() && my_batch != nullptr) {
            WriteBatch *updates = BuildBatchGroup(&last_writer);
            {
                mutex_.Unlock();
                status = WriteBatchInternal::InsertInto(updates, mem_);
                mutex_.Lock();
            }
            if (updates == tmp_batch_) tmp_batch_->Clear();
        }

        while (true) {
            Writer *ready = writers_.front();
            writers_.pop_front();
            if (ready != &w) {
                ready->status = status;
                ready->done = true;
                ready->cv.Signal();
            }
            if (ready == last_writer) break;
        }

        if (!writers_.empty()) {
            writers_.front()->cv.Signal();
        }

        return status;
    }

    Status ExpDBImpl::Put(const leveldb::WriteOptions writeOptions, const string &key, const string &val) {
        WriteBatch batch;
        batch.Put(key, val);
        return Write(writeOptions, &batch);
    }

    Status ExpDBImpl::Get(const leveldb::ReadOptions readOptions, const string &key, string *val) {return Status();}
    size_t ExpDBImpl::Scan(const leveldb::ReadOptions readOptions, const string &start, size_t num,
                           std::vector<string> &keys, std::vector<string> &values) {return 0;}
    bool ExpDBImpl::GetProperty(const leveldb::Slice &property, std::string *value) {return true;}
    Status ExpDBImpl::Delete(const leveldb::WriteOptions writeOptions, const string &key) {
        return indexDB_->Delete(writeOptions,key);
    }


    void ExpDBImpl::MaybeScheduleCompaction() {
        mutex_.AssertHeld();
        if (background_compaction_scheduled_) {
            //Already scheduled
        } else if (imm_ == nullptr) {
            //nothing to do
        } else {
            background_compaction_scheduled_ = true;
            threadPool_->addTask(&ExpDBImpl::CompactMemtable,this);
        }
    }

    Status ExpDBImpl::MakeRoomForWrite(bool force) {
        mutex_.AssertHeld();
        assert(!writers_.empty());
        bool allow_delay = !force;
        Status s;
        while (true) {
            if (!force && (mem_->ApproximateMemoryUsage() <= options_.vlogMemSize)) {
                break;
            } else if (imm_ != nullptr) {
                background_work_finished_signal_.Wait();
            } else {
                imm_ = mem_;
                has_imm_.Release_Store(imm_);
                mem_ = new MemTable(internal_comparator_);
                mem_->Ref();
                force = false;
                MaybeScheduleCompaction();
            }
        }
        return s;
    }

    WriteBatch *ExpDBImpl::BuildBatchGroup(leveldb::ExpDBImpl::Writer **last_writer) {
        mutex_.AssertHeld();
        assert(!writers_.empty());
        Writer *first = writers_.front();
        WriteBatch *result = first->batch;
        assert(result != nullptr);

        size_t size = WriteBatchInternal::ByteSize(first->batch);

        // Allow the group to grow up to a maximum size, but if the
        // original write is small, limit the growth so we do not slow
        // down the small write too much.
        size_t max_size = 1 << 20;
        if (size <= (128 << 10)) {
            max_size = size + (128 << 10);
        }

        *last_writer = first;
        std::deque<Writer *>::iterator iter = writers_.begin();
        ++iter;  // Advance past "first"
        for (; iter != writers_.end(); ++iter) {
            Writer *w = *iter;
            if (w->sync && !first->sync) {
                // Do not include a sync write into a batch handled by a non-sync write.
                break;
            }

            if (w->batch != nullptr) {
                size += WriteBatchInternal::ByteSize(w->batch);
                if (size > max_size) {
                    // Do not make batch too big
                    break;
                }

                // Append to *result
                if (result == first->batch) {
                    // Switch to temporary batch instead of disturbing caller's batch
                    result = tmp_batch_;
                    assert(WriteBatchInternal::Count(result) == 0);
                    WriteBatchInternal::Append(result, first->batch);
                }
                WriteBatchInternal::Append(result, w->batch);
            }
            *last_writer = w;
        }
        return result;
    }


    Status ExpDBImpl::CompactMemtable() {
        mutex_.AssertHeld();
        assert(imm_ != nullptr);
        Status s = WriteVlog(imm_);
        if(s.ok()){
            imm_ = nullptr;
            has_imm_.Release_Store(nullptr);
            background_work_finished_signal_.SignalAll();
            background_compaction_scheduled_ = false;
        }
        return s;
    }

    Status ExpDBImpl::WriteVlog(MemTable *mem) {
        Status s;
        mutex_.AssertHeld();
        Iterator *iter = mem->NewIterator();
        mutex_.Unlock();
        iter->SeekToFirst();
        uint64_t vlogNum = next_vlog_num_;
        std::string vlogNmae = vlogDir_ +"/"+ std::to_string(vlogNum);
        FILE *f;
        if (iter->Valid()) {
            f = fopen(vlogNmae.c_str(), "a+");
            next_vlog_num_++;
        } else {
            return s.NotFound("Invalid Mem Iterator");
        }

        /* write k-v to vlog file
         * format: <key size, value size, key, value>
        * use '$' to seperate offset and value size, key size and value size, value size and key
        */
        for (; iter->Valid(); iter->Next()) {
            // TODO: debug key sequence number
            std::string key = iter->key().ToString();
            key = key.substr(0,key.size()-8);
            std::string val = iter->value().ToString();
            size_t keySize = key.size();
            size_t valueSize = val.size();
            std::string keySizeStr = std::to_string(keySize);
            std::string valueSizeStr = std::to_string(valueSize);

            std::string vlogStr = keySizeStr + "$" + valueSizeStr + "$" + key + val; // | key size | value size | key | value |
            fwrite(vlogStr.c_str(), vlogStr.size(), 1, f);
            size_t vlogOffset = ftell(f) - val.size();
            std::string vlogOffsetStr = std::to_string(vlogOffset);
            std::string indexStr = vlogNum + "$" + vlogOffsetStr + "$" + valueSizeStr; // |vlog file number|offset|size|
            s = indexDB_->Put(WriteOptions(),key,indexStr);
            if(!s.ok()) return s;
        }
        fclose(f);
        return s;
    }
}