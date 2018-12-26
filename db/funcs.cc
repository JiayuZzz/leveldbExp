#include "funcs.h"
#include "vector"
#include <fcntl.h>

namespace leveldb {
    void appendStr(std::string &source, std::initializer_list<std::string> strs) {
        for (const auto &str:strs) {
            source.append(str);
        }
    }

    void myreadahead(int fileno, size_t offset, size_t lenght){
        static std::vector<int> alreadRead = std::vector<int>(1000);
        if(alreadRead[fileno]==1) return;
        alreadRead[fileno] = 1;
        readahead(fileno, offset, lenght);
    }
}