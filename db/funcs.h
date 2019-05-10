//
// Created by wujy on 9/29/18.
//

#ifndef LEVELDB_MY_FUNCS_H
#define LEVELDB_MY_FUNCS_H

#include <string>

namespace leveldb {
    void appendStr(std::string& source, std::initializer_list<std::string> strs); //append all strings in strs to source
    std::string conbineStr(std::initializer_list<std::string> strs);
    void myreadahead(int fileno, size_t offset, size_t lenght);
    std::string conbineValueInfo(const std::string& filename, size_t offset, size_t size);
    std::string conbineKVPair(const std::string& key, const std::string& value);
}

#endif //LEVELDB_MY_FUNCS_H
