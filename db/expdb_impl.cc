//
// Created by wujy on 9/9/18.
//

#include <zconf.h>
#include <util/mutexlock.h>
#include "expdb_impl.h"
#include "write_batch_internal.h"
#include "db/memtable.h"
#include "funcs.h"
#include <fcntl.h>
#include "gctable.h"

extern leveldb::Gctable* gctable_;

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
        *db = new ExpDBImpl(options, dbname, vlogdir, s);
        return s;
    }

    ExpDB::~ExpDB() {}

    ExpDBImpl::ExpDBImpl(ExpOptions raw_options, const std::string &dbname, const std::string &vlogdir, Status &s)
            : options_(raw_options), vlogDir_(vlogdir), env_(raw_options.env),
              internal_comparator_(raw_options.comparator), lastSequence_(0),
              background_work_finished_signal_(&mutex_),numVisited(0),background_compaction_scheduled_(false),tmp_batch_(new WriteBatch) {
        nextVlogNum_ = 0;
        visited = std::set<int>();
        openedVlog_ = std::vector<FILE *>(options_.numOpenfile);
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
        has_imm_.Release_Store(nullptr);
        threadPool_ = new ThreadPool(options_.numThreads);
        s = DB::Open(raw_options, dbname, &indexDB_);
        if (!s.ok()) return;
        if (access(vlogDir_.c_str(), F_OK) != 0 && options_.create_if_missing) {
            env_->CreateDir(vlogDir_);
        }

        Recover();  //recover nextvlognum and lastsequence
        return;
    }

    ExpDBImpl::~ExpDBImpl() {
        uint32_t numScan = STATS::GetInstance()->scanVlogStat.first;
        if(options_.numThreads==1&&numScan>0)
        std::cerr<<"######scan read "<<numVisited/numScan<<" vlogfiles in average#######\n";
        indexDB_->Put(leveldb::WriteOptions(),"lastSequence",std::to_string(lastSequence_));
        mutex_.Lock();
        while (background_compaction_scheduled_) {
            background_work_finished_signal_.Wait();
        }
        mutex_.Unlock();
        if (mem_ != nullptr) mem_->Unref();
        if (imm_ != nullptr) imm_->Unref();
        for (FILE *f:openedVlog_) {
            if (f) fclose(f);
        }
        delete indexDB_;
        delete threadPool_;
    }

    void ExpDBImpl::Recover() {
        std::string nextVlogNumStr, lastSequenceStr;
        //restore next vlognumber and lastsequence
        indexDB_->Get(leveldb::ReadOptions(),"nextVlogNum",&nextVlogNumStr);
        indexDB_->Get(leveldb::ReadOptions(),"lastSequence",&lastSequenceStr);
        if(nextVlogNumStr!="") nextVlogNum_ = std::stoi(nextVlogNumStr);
        if(lastSequenceStr!="") lastSequence_ = std::stoul(lastSequenceStr);
        std::cerr<<"next vlog num:"<<nextVlogNum_<<std::endl;
        std::cerr<<"last sequence:"<<lastSequence_<<std::endl;
    }

    Status ExpDBImpl::Write(const leveldb::WriteOptions &options, leveldb::WriteBatch *my_batch) {
        Writer w(&mutex_);
        w.batch = my_batch;
        w.sync = options.sync;
        w.done = false;
//        std::cerr << "writer lock\n";
        MutexLock l(&mutex_);
//        std::cerr << "done lock\n";
        writers_.push_back(&w);
        while (!w.done && &w != writers_.front()) {
            std::cerr << "writer wait\n";
            w.cv.Wait();
            std::cerr << "wait done\n";
        }
        if (w.done) {
            return w.status;
        }
        Status status = MakeRoomForWrite(my_batch == nullptr);//make room for writes here
//        std::cerr << "done make room\n";
        uint64_t lastSequence = lastSequence_;
        Writer *last_writer = &w;
        if (status.ok() && my_batch != nullptr) {
            WriteBatch *updates = BuildBatchGroup(&last_writer);
            WriteBatchInternal::SetSequence(updates, lastSequence+1);
            lastSequence+=WriteBatchInternal::Count(updates);
            {
                mutex_.Unlock();
                status = WriteBatchInternal::InsertInto(updates, mem_);
//                std::cerr << "memlock\n";
                mutex_.Lock();
//                std::cerr << "mem done lock\n";
            }
            if (updates == tmp_batch_) tmp_batch_->Clear();
            if(lastSequence>lastSequence_)
                lastSequence_ = lastSequence;
//            std::cerr<<"done write mem\n";
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

    Status ExpDBImpl::Put(const leveldb::WriteOptions writeOptions, const std::string &key, const std::string &val) {
        WriteBatch batch;
        batch.Put(key, val);
        return Write(writeOptions, &batch);
    }

    Status ExpDBImpl::Get(const leveldb::ReadOptions readOptions, const std::string &key, std::string *val) {
        // TODO: search memtable
        uint64_t startMicros = NowMiros();
        std::string valueInfo;
        // get value metadata from index db
        Status s = indexDB_->Get(readOptions, key, &valueInfo);
        if (s.ok()) {
            s = readValue(valueInfo, val);
        }
        STATS::TimeAndCount(STATS::GetInstance()->readVlogStat, startMicros, NowMiros());
        return s;
    }

/*
    size_t ExpDBImpl::Scan(const leveldb::ReadOptions readOptions, const std::string &start, size_t num,
                           std::vector<std::string> &keys, std::vector<std::string> &values) {
        uint64_t startMicros = NowMiros();
        size_t i = 0;
        if (keys.size() < num)
            keys.resize(num);
        if (values.size() < num)
            values.resize(num);
        std::vector<std::string> valueInfos(num);

        auto iter = indexDB_->NewIterator(readOptions);
        iter->Seek(start);

        if (iter->Valid()) {
            // for thread join
            std::future<Status> s[num];
//            std::cerr << "begin scan\n";
            uint64_t iterStart = NowMiros();
            for (; i < num && iter->Valid(); i++) {
                keys[i] = iter->key().ToString();
                valueInfos[i] = iter->value().ToString();
//                std::cerr<<"add task\n";
                uint64_t assignStart = NowMiros();
                s[i] = threadPool_->addTask(&ExpDBImpl::readValue, this, std::ref(valueInfos[i]), &values[i]);
                STATS::Time(STATS::GetInstance()->assignThread,assignStart,NowMiros());
//                std::cerr<<"finish add\n";
                iter->Next();
                if (!iter->Valid()) std::cerr << "not valid\n";
            }
            delete(iter);
            STATS::Time(STATS::GetInstance()->scanVlogIter,iterStart,NowMiros());
            // wait for all threads to complete
            uint64_t wait = NowMiros();
            for(auto &fs:s) {
                try {
                    fs.wait();
                // if no that many keys
                } catch (const std::future_error &e){
                    break;
                }
            }
            STATS::Time(STATS::GetInstance()->waitScanThreadsFinish, wait, NowMiros());
//            std::cerr<<"end scan\n";
        }
        STATS::TimeAndCount(STATS::GetInstance()->scanVlogStat,startMicros,NowMiros());
        numVisited += visited.size();
        visited.clear();
        return i;
    }
    */

    size_t ExpDBImpl::Scan(const leveldb::ReadOptions readOptions, const std::string &start, size_t num,
                           std::vector<std::string> &keys, std::vector<std::string> &values) {
        uint64_t startMicros = NowMiros();
        size_t i = 0,j=0;
        if (keys.size() < num)
            keys.resize(num);
        if (values.size() < num)
            values.resize(num);
        std::vector<std::string> valueInfos(num);

        auto iter = indexDB_->NewIterator(readOptions);
        iter->Seek(start);
        int sep = num/(options_.numThreads*4);
        if (iter->Valid()) {
            // for thread join
            std::vector<std::future<Status>> retStatus;
//            std::cerr << "begin scan\n";
            uint64_t iterStart = NowMiros();
            for (; i < num && iter->Valid(); i++) {
                keys[i] = iter->key().ToString();
                valueInfos[i] = iter->value().ToString();
//                std::cerr<<"add task\n";
                if((i%(sep)==0&&i!=0)||i==num-1) {
                    uint64_t assignStart = NowMiros();
                    retStatus.emplace_back(
                            threadPool_->addTask(
                            &ExpDBImpl::ReadValuesForScan, this, std::ref(valueInfos), j, i, std::ref(values)));
                    STATS::Time(STATS::GetInstance()->assignThread, assignStart, NowMiros());
                    j = i;
                }
//                std::cerr<<"finish add\n";
                iter->Next();
                if (!iter->Valid()) std::cerr << "not valid\n";
            }
            delete(iter);
            STATS::Time(STATS::GetInstance()->scanVlogIter,iterStart,NowMiros());
            // wait for all threads to complete
            uint64_t wait = NowMiros();
            for(auto &fs:retStatus) {
                try {
                    fs.wait();
                    // if no that many keys
                } catch (const std::future_error &e){
                    break;
                }
            }
            STATS::Time(STATS::GetInstance()->waitScanThreadsFinish, wait, NowMiros());
//            std::cerr<<"end scan\n";
        }
        STATS::TimeAndCount(STATS::GetInstance()->scanVlogStat,startMicros,NowMiros());
        numVisited += visited.size();
        visited.clear();
        return i;
    }

    Status ExpDBImpl::ReadValuesForScan(const std::vector<std::string> &valueInfos, int begin, int end,
                                        std::vector<std::string>& vals) {
        int vlogNum;
        size_t offset;
        size_t valueSize;
        Status s;
        for(int i=begin;i<end;i++){
            // get file number, offset and value size
            parseValueInfo(valueInfos[i], vlogNum, offset, valueSize);

            char value[valueSize];
            FILE *f = OpenVlog(vlogNum);
            if(options_.numThreads==1)
                if(visited.find(vlogNum)==visited.end()) visited.insert(vlogNum);
            readahead(fileno(f),(offset/4096)*4096,4096);
            size_t got = pread(fileno(f), value, valueSize, offset);
            if (got != valueSize) {
                s.IOError("get value error\n");
                std::cerr << "get value error" << std::endl;
            }
            vals[i].assign(value, valueSize);
            if(!s.ok()) return s;
        }
        return s;
    }


bool ExpDBImpl::GetProperty(const leveldb::Slice &property, std::string *value) {
    if(options_.gcAfterExe>0){
        GarbageCollect(options_.gcAfterExe);
    }
        return indexDB_->GetProperty(property,value);
    }

Status ExpDBImpl::Delete(const leveldb::WriteOptions writeOptions, const std::string &key) {
    return indexDB_->Delete(writeOptions, key);
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
//        env_->Schedule(&ExpDBImpl::BGWork,this);
//        threadPool_->addTask(&ExpDBImpl::CompactMemtable, this);
    }
}

// not used
void ExpDBImpl::BGWork(void *db) {
    reinterpret_cast<ExpDBImpl*>(db)->BackgroundCall();
}


// not used
void ExpDBImpl::BackgroundCall() {
//    std::cerr<<"backgroundcall\n";
    MutexLock l(&mutex_);
    assert(background_compaction_scheduled_);

    if(imm_!=nullptr) {
//        std::cerr<<"compactmem\n";
        CompactMemtable();
//        std::cerr<<"done compact\n";
    }
    background_work_finished_signal_.SignalAll();
    background_compaction_scheduled_ = false;
//    std::cerr<<"before schedule\n";
    MaybeScheduleCompaction();
//    std::cerr<<"end schedule\n";
    background_work_finished_signal_.SignalAll();
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
//            std::cerr << "wait background finish\n";
            background_work_finished_signal_.Wait();
//            std::cerr << "wait finish done\n";
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


Status ExpDBImpl::CompactMemtable() {
    mutex_.AssertHeld();
    assert(imm_ != nullptr);
//    std::cerr<<"begin write vlog\n";
    Status s = WriteVlog(imm_);
//    std::cerr<<"done write vlog\n";
    if (s.ok()) {
        imm_->Unref();
        imm_ = nullptr;
        has_imm_.Release_Store(nullptr);
    } else {
        std::cerr<<"write vlog error\n";
        std::cerr<<s.ToString()<<std::endl;
    }
    background_compaction_scheduled_ = false;
    background_work_finished_signal_.SignalAll();
    return s;
}

Status ExpDBImpl::WriteVlog(MemTable *mem) {
    Status s;
    mutex_.AssertHeld();
    Iterator *iter = mem->NewIterator();
    mutex_.Unlock();
    iter->SeekToFirst();
    int vlogNum = nextVlogNum_;
    FILE *f;
    if (iter->Valid()) {
        f = OpenVlog(vlogNum);
        nextVlogNum_++;
        // persist in lsm
        indexDB_->Put(leveldb::WriteOptions(),"nextVlogNum",std::to_string(nextVlogNum_));
    } else {
        return s.NotFound("Invalid Mem Iterator");
    }

    /* write k-v to vlog file
     * format: <key size, value size, key, value>
    * use '$' to seperate offset and value size, key size and value size, value size and key
    */
    std::future<Status> ret;
    while(iter->Valid()) {
        // TODO: debug key sequence number
        std::string key = iter->key().ToString();
        key = key.substr(0, key.size() - 8); //last 8 byte is sequence number
        std::string val = iter->value().ToString();
        iter->Next();\
        /*
        // deduplicate same keys
        while(iter->Valid()){
            std::string key1 = iter->key().ToString();
            key1 = key1.substr(0, key1.size() - 8);
            if(key1==key) {
                val = iter->value().ToString();
                iter->Next();
            } else {
                break;
            }
        }
        */
        size_t keySize = key.size();
        size_t valueSize = val.size();
        std::string keySizeStr = std::to_string(keySize);
        std::string valueSizeStr = std::to_string(valueSize);

        std::string vlogStr = "";
        appendStr(vlogStr,{keySizeStr,"$",valueSizeStr,"$",key,val}); // | key size | value size | key | value |
        fwrite(vlogStr.c_str(), vlogStr.size(), 1, f);
        size_t vlogOffset = ftell(f) - val.size();
        std::string vlogOffsetStr = std::to_string(vlogOffset);
        std::string indexStr = "";
        appendStr(indexStr,{std::to_string(vlogNum),"$",vlogOffsetStr,"$",valueSizeStr});// |vlog file number|offset|size|
         s = indexDB_->Put(WriteOptions(), key, indexStr);
        if (!s.ok()) return s;
    }
    return s;
}

FILE *ExpDBImpl::OpenVlog(int vlogNum) {
    std::string filename = vlogDir_ + "/" + std::to_string(vlogNum);
    assert(vlogNum <= options_.numOpenfile);
    if (openedVlog_[vlogNum] == nullptr) {
        openedVlog_[vlogNum] = fopen(filename.c_str(), "a+");
    }
    return openedVlog_[vlogNum];
}

Status ExpDBImpl::readValue(std::string &valueInfo, std::string *val) {
    Status s;
    // get file number, offset and value size
    int vlogNum;
    size_t offset;
    size_t valueSize;
    parseValueInfo(valueInfo, vlogNum, offset, valueSize);

    char value[valueSize];
    FILE *f = OpenVlog(vlogNum);
    size_t got = pread(fileno(f), value, valueSize, offset);
    if (got != valueSize) {
        s.IOError("get value error\n");
        std::cerr << "get value error" << std::endl;
    }
    val->assign(value, valueSize);
    return s;
}

void ExpDBImpl::parseValueInfo(const std::string &valueInfo, int &vlogNum, size_t &offset, size_t &valueSize) {
    size_t offsetSep = valueInfo.find('$');
    size_t sizeSep = valueInfo.rfind('$');
    std::string vlogNumStr = valueInfo.substr(0, offsetSep);
    std::string offsetStr = valueInfo.substr(offsetSep + 1, sizeSep - offsetSep - 1);
    std::string valueSizeStr = valueInfo.substr(sizeSep + 1, valueInfo.size() - sizeSep - 1);
    vlogNum = std::stoi(vlogNumStr);
    offset = std::stoul(offsetStr);
    valueSize = std::stoul(valueSizeStr);
}

    Status ExpDBImpl::GarbageCollect(size_t size) {
        uint64_t startMicros = NowMiros();
        size_t done = 0;
        int keySize;
        int valueSize;
        size_t offset;
        std::string valueInfo;
        Status s;
        while (!gctable_->IsEmpty()) {
            // garbage stats of vlog which to be gc
            Gcnode *n = gctable_->GetLast();
            gctable_->DropLast();
            done += n->gSize;
            std::cerr<<"gc "<<n->gSize<<" "<<n->vlogNum<<"\n";
            FILE *gcVlog = OpenVlog(n->vlogNum);
            fseek(gcVlog,0,SEEK_SET);
            auto iter = n->offsets.begin();
            while (2 == fscanf(gcVlog, "%d$%d$", &keySize, &valueSize)) {
                offset = ftell(gcVlog) + keySize;
                if (iter==n->offsets.end()||offset != (*iter)) {
                    //valid, write back
                    char key[keySize];
                    fread(key, keySize, 1, gcVlog);
                    char value[valueSize];
                    fread(value, valueSize, 1, gcVlog);
                    uint64_t startPut = NowMiros();
                    Put(leveldb::WriteOptions(), key, value);
                    STATS::Time(STATS::GetInstance()->gcPutBack,startPut,NowMiros());
                } else {
                    // drop
                    fseek(gcVlog, valueSize + keySize, SEEK_CUR);
                    iter++;
                }
            }
            DeleteVlog(n->vlogNum);
            if(done>size) break;
        }
        STATS::Time(STATS::GetInstance()->gcTime,startMicros,NowMiros());
        return s;
    }

    Status ExpDBImpl::DeleteVlog(int vlogNum) {
        std::string filename = vlogDir_ + "/" + std::to_string(vlogNum);
        return remove(filename.c_str())==0?Status():Status().IOError("delete vlog error\n");
    }
}
