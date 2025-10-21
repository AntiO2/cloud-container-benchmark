//
// Created by antio2 on 2025/10/21.
//
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/options_util.h>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <vector>
#include <iostream>
#include <fstream>

#include "config.h"
#include "rocksdb_factory.h"

using namespace rocksdb;

class RocksDBSequentialLoadTest : public ::testing::Test {
protected:
    Config cfg;
    std::unique_ptr<DB> db;
    std::vector<std::unique_ptr<ColumnFamilyHandle>> handles;

    void SetUp() override {
        Options options;
        options.create_if_missing = true;

        if (cfg.destroy_before_start) {
            DestroyDB(cfg.db_path, options);
        }

        db = createRocksDB(cfg.db_path, handles);
    }

    void TearDown() override {
        handles.clear();
        db.reset();
    }

    static std::string encodeKey(int indexId, int key, long long timestamp) {
        std::string s(4 + 4 + 8, '\0');
        memcpy(&s[0], &indexId, 4);
        memcpy(&s[4], &key, 4);
        memcpy(&s[8], &timestamp, 8);
        return s;
    }

    static std::string encodeValue(long long rowId) {
        std::string s(8, '\0');
        memcpy(&s[0], &rowId, 8);
        return s;
    }

    void fillSequentialData() {
        std::vector<std::thread> threads;
        std::atomic<long long> counter{0};
        long long timestamp = 1000000;

        for (int t = 0; t < cfg.thread_num; ++t) {
            threads.emplace_back([this, t, &counter, timestamp]() {
                WriteOptions wopt;
                for (int key = 0; key < cfg.id_range; ++key) {
                    std::string k = encodeKey(t, key, timestamp);
                    std::string v = encodeValue(key);
                    db->Put(wopt, k, v);
                    counter.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }

        for (auto& th : threads) th.join();
        std::cout << "Total loaded: " << counter.load() << " entries" << std::endl;
    }
};

TEST_F(RocksDBSequentialLoadTest, SequentialLoad) {
    fillSequentialData();
}