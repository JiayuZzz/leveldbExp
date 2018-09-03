//
// Created by wujy on 18-9-2.
//

#ifndef LEVELDB_STATISTICS_H
#define LEVELDB_STATISTICS_H

#include <string>
#include <vector>
#include <iostream>

#define STATS leveldb::Stats
#define NowMiros() Env::Default()->NowMicros()

namespace leveldb{
    using statType = std::pair<uint32_t ,uint64_t >;   // <op counts, total time>

    //singleton statistics class
    class Stats {
    public:
        static Stats* getInstance(){
            static Stats expStats;
            return &expStats;
        }

        statType writeStat;
        statType readStat;
        statType writeLogStat;
        statType readVlogStat;
        statType writeVlogStat;

        static void timeAndCount(statType &stat, uint64_t start, uint64_t end) {
            stat.first++;
            stat.second += (end - start);
        }

        static void time(uint64_t &stat, uint64_t start, uint64_t end) {
            stat+=(end-start);
        }

        void printAll(){
            printf("\n");
            printVlogRW();
            printRW();
            printWriteLog();
            printf("\n");
        }

        void printWriteLog(){
            double time = writeLogStat.second;
            uint32_t cnt = writeLogStat.first;
            printf("Write log time: %.2f s, Avg: %.2f us, proportion of write:%.2f\n",time/1000000,time/cnt,time/writeStat.second);
        }

        void printRW(){
            uint32_t writeCnt = writeStat.first;
            double writeTime = writeStat.second;
            uint32_t readCnt = readStat.first;
            double readTime = readStat.second;
            printf("LSM-Tree write time: %.2f s, count: %u, write latency: %.2f us\n",writeTime/1000000,writeCnt,writeTime/writeCnt);
            printf("LSM-Tree read time: %.2f s, count: %u, read latency: %.2f us\n",readTime/1000000,readCnt,readTime/readCnt);
        }

        void printVlogRW(){
            uint32_t writeCnt = writeVlogStat.first;
            double writeTime = writeVlogStat.second;
            uint32_t readCnt = readVlogStat.first;
            double readTime = readVlogStat.second;
            printf("Total write time: %.2f s, count: %u, write latency: %.2f us\n",writeTime/1000000,writeCnt,writeTime/writeCnt);
            printf("Total read time: %.2f s, count: %u, read latency: %.2f us\n",readTime/1000000,readCnt,readTime/readCnt);
        }



    private:

        Stats(){}

    };
}

#endif //LEVELDB_STATISTICS_H
