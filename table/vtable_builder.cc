//
// Created by wujy on 4/24/19.
//

#include <iostream>
#include <leveldb/env.h>
#include "leveldb/vtable_builder.h"
#include "leveldb/statistics.h"
#include "db/funcs.h"

namespace leveldb {
    VtableBuilder::VtableBuilder(const std::string& filepath):file(fopen(filepath.c_str(),"w")),finished(false),pos(0) {
    }

    VtableBuilder::~VtableBuilder() {}

    size_t VtableBuilder::Add(const leveldb::Slice &key, const leveldb::Slice &value) {
        size_t ret = pos+key.size()+1;
        pos = ret+value.size()+1;
        uint64_t startMicro = NowMiros();
        buffer+=conbineKVPair(key.ToString(),value.ToString());
        STATS::Time(STATS::GetInstance()->vtableWriteBuffer,startMicro,NowMiros());
        return ret;
    }

    Status VtableBuilder::Finish() {
        assert(!finished);
        size_t write = fwrite(buffer.c_str(), buffer.size(), 1, file);
        buffer.clear();
        pos = 0;
        fclose(file);
        return write==buffer.size()?Status():Status::IOError("Write Vtalbe Error");
    }

    void VtableBuilder::NextFile(const std::string& filepath) {
        file = fopen(filepath.c_str(),"w");
        finished = false;
    }

    bool VtableBuilder::Done() {
        return finished;
    }
}