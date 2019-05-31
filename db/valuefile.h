//
// Created by wujy on 5/31/19.
//

#ifndef LEVELDB_VALUEFILE_H
#define LEVELDB_VALUEFILE_H

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
        int numKV;
        int invalidKV;

        VfileMeta(int kv) : numKV(kv),invalidKV(0) {}

        VfileMeta() {}
    };

}

#endif //LEVELDB_VALUEFILE_H
