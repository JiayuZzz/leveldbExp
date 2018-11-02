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

namespace leveldb {
    using statType = std::pair<uint32_t, uint64_t>;   // <op counts, total time>

    //singleton statistics class
    class Stats {
    public:
        static Stats *GetInstance() {
            static Stats expStats;
            return &expStats;
        }

        statType writeStat;
        statType readStat;
        statType readVlogStat;
        statType scanVlogStat;
        statType writeVlogStat;
        uint64_t scanVlogIter;
        uint64_t writeMemtable;
        uint64_t waitScanThreadsFinish;
        uint64_t compactionIterTime;             //time of iteration during compaction
        uint64_t compactionOutputTime;           //time of writing output file during compaction
        uint64_t compactionAddToBuilder;         //time of adding kv to table builder
        uint64_t assignThread;
        uint64_t gcTime;

        static void TimeAndCount(statType &stat, uint64_t start, uint64_t end) {
            stat.first++;
            stat.second += (end - start);
        }

        static void Time(uint64_t &stat, uint64_t start, uint64_t end) {
            stat += (end - start);
        }

        void printAll() {
            printf("\n");
            printVlogRW();
            printRW();
            printf("\n");
        }

        // print lsm-tree read/write stats
        void printRW() {
            uint32_t writeCnt = writeStat.first;
            double writeTime = writeStat.second;
            uint32_t readCnt = readStat.first;
            double readTime = readStat.second;
            double memTime = writeMemtable;
            double cOutputTime = compactionOutputTime;
            double cIterTime = compactionIterTime - cOutputTime;
            double builderTime = compactionAddToBuilder;
            printf("LSM-Tree write time: %.2f s, count: %u, write latency: %.2f us\n", writeTime / 1000000, writeCnt,
                   writeTime / writeCnt);
            printf("LSM-Tree read time: %.2f s, count: %u, read latency: %.2f us\n", readTime / 1000000, readCnt,
                   readTime / readCnt);
            printf("LSM-Tree write memtable time: %.2f s\n", memTime / 1000000);
            printf("LSM-Tree compactionIterTime (compaction - outputfile): %.2f s\n", cIterTime / 1000000);
            printf("LSM-Tree compaction output file time: %.2f s\n", cOutputTime / 1000000);
            printf("LSM-Tree compaction add to builder time: %.2f s\n", builderTime / 1000000);
        }

        // print vlog/expdb read/write stats
        void printVlogRW() {
            uint32_t writeCnt = writeVlogStat.first;
            double writeTime = writeVlogStat.second;
            uint32_t readCnt = readVlogStat.first;
            double readTime = readVlogStat.second;
            uint32_t scanCnt = scanVlogStat.first;
            double scanTime = scanVlogStat.second;
            double iterTime = scanVlogIter;
            double assign = assignThread;
            double gc = gcTime;
            printf("Total vlogdb write time: %.2f s, count: %u, write latency: %.2f us\n", writeTime / 1000000,
                   writeCnt, writeTime / writeCnt);
            printf("Total vlogdb read time: %.2f s, count: %u, read latency: %.2f us\n", readTime / 1000000, readCnt,
                   readTime / readCnt);
            printf("Total vlogdb scan time:%.2f s, count: %u, scan latency: %.2f us\n", scanTime / 1000000, scanCnt,
                   scanTime / scanCnt);
            printf("Vlog scan LSM-Tree iter time: %.2f s\n",iterTime / 1000000);
            printf("Assign vlog thread time: %.2f s\n",assign/1000000);
            printf("Wait vlog scan threads finish time: %.2f s\n", (double) waitScanThreadsFinish / 1000000);
        }


    private:

        Stats() {}

    };
}

#endif //LEVELDB_STATISTICS_H
