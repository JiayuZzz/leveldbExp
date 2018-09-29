//
// Created by wujy on 9/8/18.
//

#include "db/vlog_impl.h"
#include "unistd.h"
#include "funcs.h"

namespace leveldb {

    /* dbname is index lsm-tree, vlogname is vlog filename */
    Status VlogDB::Open(VlogOptions options, const std::string &dbname, const std::string &vlogname, VlogDB **db) {
        *db = nullptr;
        Status s;
        *db = new VlogDBImpl(options, dbname, vlogname, s);
        return s;
    }

    VlogDB::~VlogDB() {}

    VlogDBImpl::VlogDBImpl(leveldb::VlogOptions &options, const std::string &dbname, const std::string &vlogname,
                           leveldb::Status &s) : options_(options) {
        threadPool_ = new ThreadPool(options_.numThreads);
        s = DB::Open(options, dbname, &indexDB_);
        if (!s.ok()) {
            std::cerr << s.ToString() << std::endl;
            return;
        }
        vlog_ = fopen(vlogname.c_str(), "a+");
    }

    /*
    * indexDB: <key,offset+value size>
    * vlog: <key size, value size, key, value>
    * use '$' to seperate offset and value size, key size and value size, value size and key
    */
    Status VlogDBImpl::Put(const leveldb::WriteOptions writeOptions, const string &key, const string &val) {
        uint64_t startMicro = NowMiros();
        long keySize = key.size();
        long valueSize = val.size();
        string keySizeStr = std::to_string(keySize);
        string valueSizeStr = std::to_string(valueSize);

        string vlogStr = "";
        appendStr(vlogStr,{keySizeStr,"$",valueSizeStr,"$",key,val}); // | key size | value size | key | value |
        fwrite(vlogStr.c_str(), vlogStr.size(), 1, vlog_);

        long vlogOffset = ftell(vlog_) - val.size();
        string vlogOffsetStr = std::to_string(vlogOffset);
        string indexStr = "";
        appendStr(indexStr,{vlogOffsetStr,"$",valueSizeStr});
        Status s = indexDB_->Put(writeOptions, key, indexStr);
        STATS::timeAndCount(STATS::getInstance()->writeVlogStat, startMicro, NowMiros());
        return s;
    }

    /*
    * Get value offset and value size from indexDB
    * Get value from vlog
    */
    Status VlogDBImpl::Get(const leveldb::ReadOptions readOptions, const string &key, string *val) {
        uint64_t startMicro = NowMiros();
        string valueInfo;
        Status s = indexDB_->Get(readOptions, key, &valueInfo);
        if (!s.ok()) return s;
        s = readValue(valueInfo, val);
        STATS::timeAndCount(STATS::getInstance()->readVlogStat, startMicro, NowMiros());
        return s;
    }

    Status VlogDBImpl::Delete(const leveldb::WriteOptions writeOptions, const string &key) {
        uint64_t startMicro = NowMiros();
        Status s = indexDB_->Delete(writeOptions, key);
        STATS::timeAndCount(STATS::getInstance()->writeVlogStat, startMicro, NowMiros());
        return s;
    }

    // multi-threading range query, return scan number
    size_t VlogDBImpl::Scan(const leveldb::ReadOptions readOptions, const string &start, size_t num,
                            std::vector<string> &keys, std::vector<string> &values) {
        uint64_t startMicro = NowMiros();
        size_t i;
        if (keys.size() < num)
            keys.resize(num);
        if (values.size() < num)
            values.resize(num);
        std::vector<string> valueInfos(num);

        // use index tree iter to get value info
        auto iter = indexDB_->NewIterator(readOptions);
        iter->Seek(start);

        //for main thread waiting
        std::future<Status> s[num];
        for (i = 0; i < num && iter->Valid(); i++) {
            keys[i] = iter->key().ToString();
            valueInfos[i] = iter->value().ToString();
            s[i] = threadPool_->addTask(&VlogDBImpl::readValue,
                                        this,
                                        std::ref(valueInfos[i]),
                                        &values[i]);
            iter->Next();
            if (!iter->Valid()) std::cerr << "not valid\n";
        }
        // wait for all threads to complete
        uint64_t wait = NowMiros();
        for (auto &fs:s) {
            try {
                fs.wait();
            // if no that many keys
            } catch (const std::future_error &e){
                break;
            }
        }
        STATS::time(STATS::getInstance()->waitScanThreadsFinish, wait, NowMiros());
        STATS::timeAndCount(STATS::getInstance()->scanVlogStat, startMicro, NowMiros());
        return i;
    }

    Status VlogDBImpl::readValue(string &valueInfo, string *val) {
        //get value offset and value size
        size_t sepPos = valueInfo.find('$');
        string offsetStr = valueInfo.substr(0, sepPos);
        string valueSizeStr = valueInfo.substr(sepPos + 1, valueInfo.size() - sepPos - 1);
        long offset = std::stol(offsetStr);
        long valueSize = std::stol(valueSizeStr);
        char value[valueSize];
        //read value from vlog
        size_t got = pread(fileno(vlog_), value, valueSize, offset);
        val->assign(value, valueSize);
        return Status();
    }

    bool VlogDBImpl::GetProperty(const Slice &property, std::string *value) {
        return indexDB_->GetProperty(property, value);
    };

    VlogDBImpl::~VlogDBImpl() {
        fclose(vlog_);
        delete indexDB_;
        delete threadPool_;
    }
}
