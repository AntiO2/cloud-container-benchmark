//
// Created by antio2 on 2025/10/21.
//

#ifndef ROCKSDB_BENCH_CONFIG_H
#define ROCKSDB_BENCH_CONFIG_H

#pragma once
#include <string>

enum class ts_typt_t {
    retina,
    udt
};

struct Config1000 {
    std::string db_path = "/tmp/test_rocksdb_perf";
    int thread_num = 16;
    int id_range = 10000000;
    int ops_per_thread = 1000000;
    bool destroy_before_start = true;
    ts_typt_t ts_type = ts_typt_t::retina;
};

struct Config1000_udt {
    std::string db_path = "/tmp/test_rocksdb_udt_1000";
    int thread_num = 16;
    int id_range = 10000000;
    int ops_per_thread = 1000000;
    bool destroy_before_start = true;
    ts_typt_t ts_type = ts_typt_t::udt;
};

struct Config10 {
    std::string db_path = "/tmp/test_rocksdb_perf_10";
    int thread_num = 16;
    int id_range = 100000;
    int ops_per_thread = 1000000;
    bool destroy_before_start = true;
    ts_typt_t ts_type = ts_typt_t::retina;
};

using Config = Config1000_udt;

#endif //ROCKSDB_BENCH_CONFIG_H
