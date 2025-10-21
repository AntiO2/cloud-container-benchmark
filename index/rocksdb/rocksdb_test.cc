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

#include "config.h"
#include "rocksdb_factory.h"

using namespace rocksdb;

class RocksDBPerfTest : public ::testing::Test {
protected:
    Config cfg;
    std::vector<std::unique_ptr<ColumnFamilyHandle>> handles;
    std::unique_ptr<DB> db;
    void SetUp() override {
        Options options;
        options.create_if_missing = true;

        if (cfg.destroy_before_start)
            DestroyDB(cfg.db_path, options);
        db = createRocksDB(cfg, handles);
    }

    void TearDown() override {
        handles.clear();
        db.reset();
    }

    static std::string encodeKey(int indexId, int key, long long timestamp) {
        std::string s;
        s.resize(4 + 4 + 8);
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

    long long getUniqueRowId(int indexId, int key, long long timestamp) {
        ReadOptions ropt;
        std::string val_str;

        if (cfg.ts_type == ts_type_t::udt) {
            // Convert timestamp to Slice
            std::string ts_buf;
            Slice tsSlice = EncodeU64Ts(timestamp, &ts_buf);  // helper function
            ropt.timestamp = &tsSlice;
            std::string k = encodeKey(indexId, key, 0);       // key part only
            Status s = db->Get(ropt, k, &val_str);
            if (!s.ok()) return -1;
        } else {
            std::string k = encodeKey(indexId, key, timestamp);
            Status s = db->Get(ropt, k, &val_str);
            if (!s.ok()) return -1;
        }

        long long rowId;
        memcpy(&rowId, val_str.data(), sizeof(rowId));
        return rowId;
    }

public:
    void threadWorker(int indexId, std::atomic<long long>& counter) {
        std::mt19937_64 rng(std::random_device{}());

        for (int i = 0; i < cfg.ops_per_thread; ++i) {
            int key = i % cfg.id_range;
            long long ts = std::chrono::system_clock::now().time_since_epoch().count();
            long long oldRow = getUniqueRowId(indexId, key, ts);
            long long newRow = (oldRow == -1) ? rng() : oldRow + 1;

            WriteOptions wopt;
            if (cfg.ts_type == ts_type_t::udt) {
                std::string ts_buf;
                Slice tsSlice = EncodeU64Ts(ts, &ts_buf);
                std::string k = encodeKey(indexId, key, 0); // key without timestamp
                std::string v = encodeValue(newRow);
                db->Put(wopt, k, tsSlice, v);
            } else {
                std::string k = encodeKey(indexId, key, ts);
                std::string v = encodeValue(newRow);
                db->Put(wopt, k, v);
            }

            counter.fetch_add(1, std::memory_order_relaxed);
        }
    }
};

TEST_F(RocksDBPerfTest, MultiThreadPerf)
{
    std::cerr << "Start update test" << std::endl;
    std::atomic<long long> totalOps = 0;
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < cfg.thread_num; ++i) {
        threads.emplace_back(&RocksDBPerfTest::threadWorker, this, i, std::ref(totalOps));
    }

    for (auto& t : threads) t.join();

    auto end = std::chrono::high_resolution_clock::now();
    double sec = std::chrono::duration<double>(end - start).count();

    std::cout << "Total ops: " << totalOps
              << ", time: " << sec << "s, throughput: "
              << (long long)(totalOps / sec) << " ops/s" << std::endl;

    ASSERT_GT(totalOps, 0);
}
