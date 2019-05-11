//
// Created by wujy on 5/9/19.
//

#ifndef LEVELDB_VALUE_ITER_H
#define LEVELDB_VALUE_ITER_H

#include "leveldb/iterator.h"
#include <string>
#include <iostream>
#include <fstream>
#include <leveldb/db.h>

namespace leveldb {
    class ValueIterator : public Iterator {
    public:
        ValueIterator(const std::string& valueFile, DB* db);


        virtual ~ValueIterator(){};

        virtual bool Valid() const ;

        virtual void SeekToFirst();

        virtual void SeekToLast();

        virtual void Seek(const Slice& target){};

        virtual void Next();

        virtual void Prev();

        virtual Slice key() const;

        virtual Slice value() const;

        virtual Status status() const {return Status();};

    private:
        std::ifstream f;
        std::string filename_;
        std::string key_;
        std::string value_;
        DB* db_;
    };
}

#endif //LEVELDB_VALUE_ITER_H
