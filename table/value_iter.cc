//
// Created by wujy on 5/9/19.
//

#include "value_iter.h"

namespace leveldb {
    ValueIterator::ValueIterator(const std::string &valueFile):key_(""),value_("") {
        f.open(valueFile);
    }

    bool ValueIterator::Valid() {
        return !key_.empty();
    }

    Slice ValueIterator::key() const {
        return Slice(key_);
    }

    Slice ValueIterator::value() const {
        return Slice(value_);
    }

    void ValueIterator::SeekToFirst() {
        f.seekg(0,std::ios::beg);
        Next();
    }

    void ValueIterator::Next() {
        while(getline(f, key_, '$')){
            std::string valueInfo;
            std::string filename;
            size_t offset, size;
            db_->Get(leveldb::ReadOptions(true),key_,&valueInfo);
            parseValueInfo(valueInfo,filename,offset,size);
            if(filename==filename_&&offset==f.tellg()){
                getline(f, value_, '$');
                return;
            } else {
                // TODO: jump value size
                getline(f, value_, '$');
            }
        }
        key_ = "";
    }

    void ValueIterator::SeekToLast() {}

    void ValueIterator::Prev() {}

}