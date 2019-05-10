#include "funcs.h"
#include "vector"
#include <fcntl.h>

namespace leveldb {
    void appendStr(std::string &source, std::initializer_list<std::string> strs) {
        for (const auto &str:strs) {
            source.append(str);
        }
    }

    std::string conbineStr(std::initializer_list<std::string> strs){
        std::string res;
        for (const auto &str:strs) {
            res.append(str);
        }
        return res;
    }

    void myreadahead(int fileno, size_t offset, size_t lenght){
        static std::vector<int> alreadRead = std::vector<int>(1000);
        if(alreadRead[fileno]==1) return;
        alreadRead[fileno] = 1;
        readahead(fileno, offset, lenght);
    }

    std::string conbineValueInfo(const std::string& filename, size_t offset, size_t size){
        std::string vinfo;
        appendStr(vinfo,{filename,"$",std::to_string(offset),"$",std::to_string(size)});
        return vinfo;
    }

    std::string conbineKVPair(const std::string& key, const std::string& value){
        std::string kv;
        appendStr(kv,{key,"$",value,"$"});
        return kv;
    }
}