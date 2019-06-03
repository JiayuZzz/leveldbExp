//
// Created by wujy on 4/24/19.
//

#include <iostream>
#include <leveldb/env.h>
#include <zconf.h>
#include "leveldb/vtable_builder.h"
#include "leveldb/statistics.h"
#include "db/funcs.h"

namespace leveldb {
    VtableBuilder::VtableBuilder(const std::string& filepath):file(fopen(filepath.c_str(),"w")),finished(false),pos(0),num(0) {
        toSync.push_back(file);
    }

    VtableBuilder::VtableBuilder():file(nullptr),finished(true),pos(0),num(0){}

    size_t VtableBuilder::Add(const leveldb::Slice &key, const leveldb::Slice &value) {
        uint64_t startMicro = NowMicros();
        size_t ret = pos+key.size()+1;
        pos = ret+value.size()+1;
        // TODO: pre alloc buffer space
        buffer+=conbineKVPair(key.ToString(),value.ToString());
        num++;
        STATS::Time(STATS::GetInstance()->vtableWriteBuffer,startMicro,NowMicros());
        return ret;
    }

    // finish a vtable, return kv cnt
    int VtableBuilder::Finish() {
        uint64_t startMicros = NowMicros();
        //assert(!finished);
        size_t write = fwrite(buffer.c_str(), buffer.size(), 1, file);
        //fdatasync(fileno(file));
        STATS::Add(STATS::GetInstance()->vTableWriteDisk,ftell(file));
        //fclose(file);
        STATS::TimeAndCount(STATS::GetInstance()->writeVtableStat, startMicros, NowMicros());
        finished = true;
        return num;
    }

    void VtableBuilder::NextFile(const std::string& filepath) {
        file = fopen(filepath.c_str(),"w");
        finished = false;
        toSync.push_back(file);
        buffer.clear();
        num = 0;
        pos = 0;
    }

    // sync all vtable written, return kv cnt of last finished vtable
    int VtableBuilder::Done() {
        if(!finished) Finish();
        uint64_t startMicros = NowMicros();
        for(FILE* f:toSync) {
            fdatasync(fileno(f));
            fclose(f);
        }
        STATS::Time(STATS::GetInstance()->vtableSync, startMicros, NowMicros());
        return num;
    }
}