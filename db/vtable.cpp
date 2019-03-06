//
// Created by wujy on 3/4/19.
//

#include "vtable.h"

long Vtable::Put(const std::string &key, const std::string &value) {
    FILE* vtable = openVtable(current);
    long offset = ftell(vtable);
    long write = fwrite(value.c_str(),value.size(),1,vtable);
    return write==value.size()?offset:-1;
}