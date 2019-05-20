//
// Created by wujy on 5/18/19.
//

#ifndef LEVELDB_BLOCK_QUEUE_H
#define LEVELDB_BLOCK_QUEUE_H

#include "port/port_stdcxx.h"
#include "mutexlock.h"
#include <queue>

namespace leveldb {

    template<class T>
    class BlockQueue {
    public:
        BlockQueue():mutex_(),cv_(&mutex_),q_(){}

        T Get(){
            MutexLock ml(&mutex_);
            while(q_.empty()) cv_.Wait();
            T ret = q_.front();
            q_.pop();
            return ret;
        }

        void Put(const T& item){
            MutexLock ml(&mutex_);
            q_.push(item);
            cv_.Signal();
        }

    private:
        port::CondVar cv_;
        port::Mutex mutex_;
        std::queue<T> q_;
    };

}


#endif //LEVELDB_BLOCK_QUEUE_H
