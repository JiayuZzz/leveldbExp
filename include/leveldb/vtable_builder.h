//
// Created by wujy on 4/24/19.
//

#ifndef LEVELDB_VTABLE_BUILDER_H
#define LEVELDB_VTABLE_BUILDER_H

#include "leveldb/export.h"
#include "leveldb/status.h"
#include <string>

namespace leveldb {

// vtable format: key1$value1$key2$value2...
// vtable name: ('a'+level)+filenum
class LEVELDB_EXPORT VtableBuilder {
public:
    VtableBuilder(const std::string& filepath);

    VtableBuilder();

    ~VtableBuilder(){};

    size_t Add(const Slice& key, const Slice& value);

    int Finish();

    void Sync();
//    int Done();

    void NextFile(const std::string& filepath);

    bool Finished(){ return finished;}

private:
    std::string buffer;
    size_t pos;
    FILE* file;
    bool finished;
    int num;       // num kv pairs
    std::vector<FILE*> toSync;     //sync while all done
};

}

#endif //LEVELDB_VTABLE_BUILDER_H
