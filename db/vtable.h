//
// Created by wujy on 3/4/19.
//

#ifndef LEVELDB_VTABLE_H
#define LEVELDB_VTABLE_H


#include <string>
#include <unordered_map>
#include "unistd.h"
#include "sys/stat.h"
#include <iostream>

// singleton
class Vtable {
private:
    std::string vtableDir;
    int current;          // current writing vtable number
    std::unordered_map<int, FILE*> openedVtables;
    Vtable():current(0),openedVtables(std::unordered_map<int, FILE*>()){}

public:
    static Vtable* Instance(){
        static Vtable vtable;
        return &vtable;
    }

    void SetPath(const std::string& path){
        if(access(path.c_str(), F_OK)!=0){
            mkdir(path.c_str(),S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        }
        vtableDir = path;
    }

    int Current(){return current;}

    void SetCurrent(int num){ current = num; }

    long Put(const std::string& key, const std::string& value);

    FILE* openVtable(int num){
        std::string filename = vtableDir+"/"+std::to_string(num);
        if(openedVtables.find(num)==openedVtables.end()){
            openedVtables[num] = fopen(filename.c_str(),"a+");
        }
        return openedVtables[num];
    }
};


#endif //LEVELDB_VTABLE_H
