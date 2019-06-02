//
// Created by wujy on 9/8/18.
//

#include "db/vlog_impl.h"
#include "unistd.h"
#include "funcs.h"
#include <sys/stat.h>
#include <fcntl.h>

FILE *offsetsFile = fopen("./offsets", "w");
uint64_t insertCnt = 0;

namespace leveldb {

    /* dbname is index lsm-tree, vlogname is vlog filename */
    Status VlogDB::Open(VlogOptions options, const std::string &dbname, const std::string &vlogDir, VlogDB **db) {
        *db = nullptr;
        Status s;
        *db = new VlogDBImpl(options, dbname, vlogDir, s);
        return s;
    }

    VlogDB::~VlogDB() {}

    VlogDBImpl::VlogDBImpl(leveldb::VlogOptions &options, const std::string &dbname, const std::string &vlogDir,
                           leveldb::Status &s) : options_(options), vlogDir_(vlogDir), lastVlogNum_(0),
                                                 headVLogNum_(0) {
        threadPool_ = new ThreadPool(options_.numThreads);
        openedVlog_ = std::vector<FILE *>(options_.numOpenfile);
        s = DB::Open(options, dbname, &indexDB_);
        if (!s.ok()) {
            std::cerr << s.ToString() << std::endl;
            return;
        }
        if (access(vlogDir_.c_str(), F_OK) != 0 && options_.create_if_missing) {
            mkdir(vlogDir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        }
        Recover();

	    fprintf(stderr,"gc size: %lu\n",options_.gcAfterExe);
        /*
        if (options_.gcAfterExe > 0) {
            fprintf(stderr,"start gc\n");
            threadPool_->addTask(&VlogDBImpl::GarbageCollect,this,options.gcAfterExe);
        }
         */
    }

    FILE *VlogDBImpl::OpenVlog(int vlogNum) {
        std::string filename = vlogDir_ + "/" + std::to_string(vlogNum);
        if (openedVlog_[vlogNum] == nullptr) {
            openedVlog_[vlogNum] = fopen(filename.c_str(), "a+");
        }
        return openedVlog_[vlogNum];
    }

    /*
    * indexDB: <key,vlognum, offset+value size>
    * vlog: <key size, value size, key, value>
    * use '$' to seperate offset and value size, key size and value size, value size and key
    */
    Status VlogDBImpl::Put(const leveldb::WriteOptions& writeOptions, const std::string &key, const std::string &val) {
        insertCnt++;
        //TODO make it configurable
        if(insertCnt==20000000){
            if (options_.gcAfterExe > 0) {
                fprintf(stderr,"start gc\n");
                threadPool_->addTask(&VlogDBImpl::GarbageCollect,this,options_.gcAfterExe);
            }
        }
        uint64_t startMicro = NowMiros();
        long keySize = key.size();
        long valueSize = val.size();
        std::string keySizeStr = std::to_string(keySize);
        std::string valueSizeStr = std::to_string(valueSize);

        std::string vlogStr = "";
        appendStr(vlogStr, {keySizeStr, "$", valueSizeStr, "$", key, val}); // | key size | value size | key | value |
        FILE *vlog = OpenVlog(lastVlogNum_);
        fwrite(vlogStr.c_str(), vlogStr.size(), 1, vlog);
        STATS::Add(STATS::GetInstance()->vlogWriteDisk,vlogStr.size());

        long vlogOffset = ftell(vlog) - val.size();
        std::string vlogOffsetStr = std::to_string(vlogOffset);
        std::string indexStr = "";
        appendStr(indexStr, {std::to_string(lastVlogNum_), "$", vlogOffsetStr, "$", valueSizeStr});
        if (ftell(vlog) > options_.vlogSize) {
            fsync(fileno(vlog));
            lastVlogNum_++;
            indexDB_->Put(writeOptions, "lastVlogNum", std::to_string(lastVlogNum_));
        }
        STATS::TimeAndCount(STATS::GetInstance()->writeVlogStat, startMicro, NowMiros());
        Status s = indexDB_->Put(writeOptions, key, indexStr);
        // save tail
        return s;
    }

    /*
    * Get value offset and value size from indexDB
    * Get value from vlog
    */
    Status VlogDBImpl::Get(const leveldb::ReadOptions& readOptions, const std::string &key, std::string *val) {
        uint64_t startMicro = NowMiros();
        std::string valueInfo;
        Status s = indexDB_->Get(readOptions, key, &valueInfo);
        if (!s.ok()) return s;
        s = readValue(valueInfo, val);
        STATS::TimeAndCount(STATS::GetInstance()->readVlogStat, startMicro, NowMiros());
        return s;
    }

    Status VlogDBImpl::Delete(const leveldb::WriteOptions& writeOptions, const std::string &key) {
        uint64_t startMicro = NowMiros();
        Status s = indexDB_->Delete(writeOptions, key);
        STATS::TimeAndCount(STATS::GetInstance()->writeVlogStat, startMicro, NowMiros());
        return s;
    }

    // multi-threading range query, return scan number
    size_t VlogDBImpl::Scan(const leveldb::ReadOptions& readOptions, const std::string &start, size_t num,
                            std::vector<std::string> &keys, std::vector<std::string> &values) {
        uint64_t startMicro = NowMiros();
        size_t i = 0, j = 0;
        if (keys.size() < num)
            keys.resize(num);
        if (values.size() < num)
            values.resize(num);
        std::vector<std::string> valueInfos(num);

        // use index tree iter to get value info
        auto iter = indexDB_->NewIterator(readOptions);
        iter->Seek(start);
        int sep = 8;
        //for main thread waiting
        uint64_t iterStart = NowMiros();
        std::vector<std::future<Status>> retStatus;
        for (; i < num && iter->Valid(); i++) {
            keys[i] = iter->key().ToString();
            valueInfos[i] = iter->value().ToString();
            int filenum;
            size_t offset;
            size_t size;
            parseValueInfo(valueInfos[i],filenum,offset,size);
            FILE* f = OpenVlog(filenum);
//            uint64_t startAdvice = NowMiros();
//            readahead(fileno(f),offset,size);
//            STATS::Time(STATS::GetInstance()->fadviceTime, startAdvice, NowMiros());
//                std::cerr<<"add task\n";
            if ((i % (sep) == 0 && i != 0) || i == num - 1) {
                uint64_t assignStart = NowMiros();
                retStatus.emplace_back(
                        threadPool_->addTask(
                                &VlogDBImpl::ReadValuesForScan, this, std::ref(valueInfos), j, i, std::ref(values)));
                STATS::Time(STATS::GetInstance()->assignThread, assignStart, NowMiros());
                j = i;
            }
//                std::cerr<<"finish add\n";
            iter->Next();
            if (!iter->Valid()) std::cerr << "not valid\n";
        }
        delete (iter);
        STATS::Time(STATS::GetInstance()->scanVlogIter, iterStart, NowMiros());
        // wait for all threads to complete
        uint64_t wait = NowMiros();
        for (auto &fs:retStatus) {
            try {
                fs.wait();
                // if no that many keys
            } catch (const std::future_error &e) {
                break;
            }
        }
        STATS::Time(STATS::GetInstance()->waitScanThreadsFinish, wait, NowMiros());
        STATS::TimeAndCount(STATS::GetInstance()->scanVlogStat, startMicro, NowMiros());
        return i;
    }

    Status VlogDBImpl::readValue(std::string &valueInfo, std::string *val) {
        //get value offset and value size
        Status s;
        int vlogNum;
        size_t offset;
        size_t valueSize;
        parseValueInfo(valueInfo, vlogNum, offset, valueSize);

        char value[valueSize];
        //read value from vlog
        FILE *vlog = OpenVlog(vlogNum);
        size_t got = pread(fileno(vlog), value, valueSize, offset);
        if (got != valueSize) {
            s.IOError("get value error\n");
            std::cerr << "get value error" << std::endl;
        }
        val->assign(value, valueSize);
        return s;
    }

    Status VlogDBImpl::ReadValuesForScan(const std::vector<std::string> &valueInfos, int begin, int end,
                                        std::vector<std::string> &vals) {
        int vlogNum;
        size_t offset;
        size_t valueSize;
        Status s;
        for (int i = begin; i < end; i++) {
            // get file number, offset and value size
            parseValueInfo(valueInfos[i], vlogNum, offset, valueSize);

            char value[valueSize];
            FILE *f = OpenVlog(vlogNum);

            size_t got = pread(fileno(f), value, valueSize, offset);
            if (got != valueSize) {
                s.IOError("get value error\n");
                std::cerr << "get value error" << std::endl;
            }
            vals[i].assign(value, valueSize);
            if (!s.ok()) return s;
        }
        return s;
    }

    bool VlogDBImpl::GetProperty(const Slice &property, std::string *value) {
        fprintf(stderr,"getproperty\n");
        return indexDB_->GetProperty(property, value);
    };

    VlogDBImpl::~VlogDBImpl() {
        for (FILE *f:openedVlog_) {
            if (f) fclose(f);
        }
        delete indexDB_;
        delete threadPool_;
    }

    void VlogDBImpl::parseValueInfo(const std::string &valueInfo, int &vlogNum, size_t &offset, size_t &valueSize) {
        size_t offsetSep = valueInfo.find('$');
        size_t sizeSep = valueInfo.rfind('$');
        std::string vlogNumStr = valueInfo.substr(0, offsetSep);
        std::string offsetStr = valueInfo.substr(offsetSep + 1, sizeSep - offsetSep - 1);
        std::string valueSizeStr = valueInfo.substr(sizeSep + 1, valueInfo.size() - sizeSep - 1);
        //TODO delete this line
        //if (options_.numThreads == 1) fwrite((offsetStr + ",").c_str(), offsetStr.size() + 1, 1, offsetsFile);
        vlogNum = std::stoi(vlogNumStr);
        offset = std::stoul(offsetStr);
        valueSize = std::stoul(valueSizeStr);
    }

    void VlogDBImpl::Recover() {
        std::string lastVlogNumStr, headVlogNumStr;
        //restore next vlognumber and lastsequence
        indexDB_->Get(leveldb::ReadOptions(), "lastVlogNum", &lastVlogNumStr);
        indexDB_->Get(leveldb::ReadOptions(), "headVlogNum", &headVlogNumStr);
        if (lastVlogNumStr != "") lastVlogNum_ = std::stoi(lastVlogNumStr);
        if (headVlogNumStr != "") headVLogNum_ = std::stoi(headVlogNumStr);
        std::cerr << "last vlog num:" << lastVlogNum_ << std::endl;
        std::cerr << "head vlog num:" << headVLogNum_ << std::endl;
    }

    Status VlogDBImpl::GarbageCollect(size_t size) {
        size_t done = 0;
        int keySize;
        int valueSize;
        size_t offset;
        std::string valueInfo;
        Status s;
        std::cerr << "gc" << size << std::endl;
        while (headVLogNum_ != lastVlogNum_) {
            uint64_t startMicros = NowMiros();
            FILE *gcVlog = OpenVlog(headVLogNum_);
            fseek(gcVlog, 0, SEEK_SET);
            while (2 == fscanf(gcVlog, "%d$%d$", &keySize, &valueSize)) {
                offset = ftell(gcVlog) + keySize;     // value offset
                char key[keySize];
                fread(key, keySize, 1, gcVlog);
                uint64_t startGet = NowMiros();
                auto s = indexDB_->Get(leveldb::ReadOptions(), key, &valueInfo);
                STATS::Time(STATS::GetInstance()->gcReadLsm, startGet, NowMiros());
                if (!s.ok()) {
                    fseek(gcVlog, valueSize, SEEK_CUR);
                    done += (keySize + valueSize);
                    continue;
                }

                int vlogNum;
                size_t offset1;
                size_t valueSize1;
                parseValueInfo(valueInfo, vlogNum, offset1, valueSize1);
                if (vlogNum == headVLogNum_ && offset == offset1) {
                    // write back
                    char value[valueSize];
                    fread(value, valueSize, 1, gcVlog);
                    uint64_t startPut = NowMiros();
                    Put(leveldb::WriteOptions(), key, value);
                    STATS::Time(STATS::GetInstance()->gcPutBack, startPut, NowMiros());
                    STATS::Add(STATS::GetInstance()->gcWritebackBytes, valueSize);
                } else {
                    // drop
                    fseek(gcVlog, valueSize, SEEK_CUR);
                    done += (keySize + valueSize);
                }
            }
            DeleteVlog(headVLogNum_);
            headVLogNum_++;
            //save head vlog number
            indexDB_->Put(leveldb::WriteOptions(), "headVlogNum", std::to_string(headVLogNum_));
            STATS::Time(STATS::GetInstance()->gcTime, startMicros, NowMiros());
            if (done > size) break;
        }
        std::cerr << "done " << done <<"byte"<< std::endl;
        return s;
    }

    Status VlogDBImpl::DeleteVlog(int vlogNum) {
        std::string filename = vlogDir_ + "/" + std::to_string(vlogNum);
        return remove(filename.c_str()) == 0 ? Status() : Status().IOError("delete vlog error\n");
    }
}
