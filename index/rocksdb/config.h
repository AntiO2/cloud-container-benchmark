//
// Created by antio2 on 2025/10/21.
//

#ifndef ROCKSDB_BENCH_CONFIG_H
#define ROCKSDB_BENCH_CONFIG_H

#pragma once
#include <string>

enum class ts_type_t {
    retina,
    udt
};

struct Config1000 {
    std::string db_path = "/tmp/test_rocksdb_perf";
    int thread_num = 16;
    int id_range = 10000000;
    int ops_per_thread = 1000000;
    bool destroy_before_start = true;
    ts_type_t ts_type = ts_type_t::retina;
};

struct Config1000_udt {
    std::string db_path = "/tmp/test_rocksdb_udt_1000";
    int thread_num = 16;
    int id_range = 10000000;
    int ops_per_thread = 1000000;
    bool destroy_before_start = true;
    ts_type_t ts_type = ts_type_t::udt;
};

struct Config1_retina {
    std::string db_path = "/tmp/test_rocksdb_perf_retina_0.1";
    int thread_num = 16;
    int id_range = 1000;
    int ops_per_thread = 1000000;
    bool destroy_before_start = true;
    ts_type_t ts_type = ts_type_t::retina;
};

struct Config1_udt {
    std::string db_path = "/tmp/test_rocksdb_perf_udt_0.1";
    int thread_num = 16;
    int id_range = 1000;
    int ops_per_thread = 1000000;
    bool destroy_before_start = true;
    ts_type_t ts_type = ts_type_t::udt;
};

using Config = Config1_udt;

#endif //ROCKSDB_BENCH_CONFIG_H
