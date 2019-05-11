//
// Created by wujy on 5/9/19.
//

#include "value_iter.h"
#include "db/funcs.h"

namespace leveldb {
    ValueIterator::ValueIterator(const std::string &valueFile, DB* db):key_(""),value_(""),db_(db),filename_(valueFile) {
        f.open(valueFile);
    }

    bool ValueIterator::Valid() const {
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
//        std::cerr<<"call next"<<std::endl;
        while(getline(f, key_, '~')){
            std::string valueInfo;
            std::string filename;
            size_t offset = 0, size = 0;
//            std::cerr<<"call get"<<std::endl;
            db_->Get(leveldb::ReadOptions(true),key_,&valueInfo);
//            std::cerr<<"get done\n";
            if(valueInfo.size()<100) {
                parseValueInfo(valueInfo, filename, offset, size);
//                std::cerr << "key " << key_ << " value info " << valueInfo << " filename " << filename << " offset "
//                          << offset << std::endl;
            }
//            std::cerr<<"here filename "<<filename_<<" offset "<<f.tellg()<<std::endl;
            // TODO, filename = filename_?
//            std::cerr<<"same offset?"<<(offset==f.tellg())<<std::endl;
            if(offset==f.tellg()){
//                std::cerr<<"valid value"<<std::endl;
                getline(f, value_, '~');
                return;
            } else {
                // TODO: jump value size
                getline(f, value_, '~');
            }
        }
        std::cerr<<"exit next\n";
        key_ = "";
    }

    void ValueIterator::SeekToLast() {}

    void ValueIterator::Prev() {}

}