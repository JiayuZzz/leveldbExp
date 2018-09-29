#include "funcs.h"

namespace leveldb {
    void appendStr(std::string &source, std::initializer_list<std::string> strs) {
        for (const auto &str:strs) {
            source.append(str);
        }
    }
}