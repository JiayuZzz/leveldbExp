//
// Created by wujy on 5/31/19.
//

#ifndef LEVELDB_VALUEFILE_H
#define LEVELDB_VALUEFILE_H

#include "leveldb/statistics.h"
#include "funcs.h"
#include "unistd.h"

namespace leveldb {

/* selective kv */
    struct ValueLoc {
        FILE *f;
        size_t offset;
        size_t size;

        ValueLoc() {}

        ValueLoc(FILE *file, size_t o, size_t s) : f(file), offset(o), size(s) {}
    };

    struct VfileMeta {
        int valid;
        int invalidKV;

        VfileMeta(int v) : valid(v),invalidKV(0) {}

        VfileMeta() {}
    };

    class ValueLog {
    public:
        //return offset of record
        size_t AddRecord(const std::string& key, const std::string& value){
            uint64_t startMicros = NowMicros();
            //fwrite((conbineKVPair(key,value)).c_str(),key.size()+value.size()+2,1,writingVlog_);
            write(fileno(f),(conbineKVPair(key,value)).data(),key.size()+value.size()+2);
            size_t offset = ftell(f);
            STATS::TimeAndCount(STATS::GetInstance()->writeVlogStat,startMicros,NowMicros());
            return offset-value.size()-1;
        }

        void NextFile(const std::string& file){
            if(f) fclose(f);
            f = fopen(file.c_str(),"a+");
        }

        void Sync(){
            fdatasync(fileno(f));
            STATS::Add(STATS::GetInstance()->vlogWriteDisk,ftell(f));
        }

        ~ValueLog(){fclose(f);}

    private:
        FILE *f;
    };

}

#endif //LEVELDB_VALUEFILE_H
