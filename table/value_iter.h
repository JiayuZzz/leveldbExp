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
        ValueIterator(const std::string& valueFile);


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
        std::fstream f;
        std::string filename_;
        std::string key_;
        std::string value_;
        DB* db_;

        void parseValueInfo(const std::string &valueInfo, std::string &filename, size_t &offset, size_t &valueSize) {
            size_t offsetSep = valueInfo.find('$');
            size_t sizeSep = valueInfo.rfind('$');
            offset = std::stoul(valueInfo.substr(offsetSep+1, sizeSep - offsetSep - 1));
            valueSize = std::stoul(valueInfo.substr(sizeSep+1, valueInfo.size()-sizeSep-1));
            filename = valueInfo.substr(0, offsetSep);
            //std::cerr<<"######filename : "<<filename<<std::endl;
        }
    };
}

#endif //LEVELDB_VALUE_ITER_H
