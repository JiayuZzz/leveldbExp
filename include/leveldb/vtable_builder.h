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
class LEVELDB_EXPORT VtableBuilder {
public:
    VtableBuilder(const std::string& filepath);

    ~VtableBuilder();

    size_t Add(const Slice& key, const Slice& value);

    Status Finish();

    bool Done();

    void NextFile(const std::string& filepath);

private:
    std::string buffer;
    size_t pos;
    FILE* file;
    bool finished;
};

}

#endif //LEVELDB_VTABLE_BUILDER_H
