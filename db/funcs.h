//
// Created by wujy on 9/29/18.
//

#ifndef LEVELDB_MY_FUNCS_H
#define LEVELDB_MY_FUNCS_H

#include <string>

namespace leveldb {
    void appendStr(std::string& source, std::initializer_list<std::string> strs); //append all strings in strs to source
    void myreadahead(int fileno, size_t offset, size_t lenght);
}

#endif //LEVELDB_MY_FUNCS_H
