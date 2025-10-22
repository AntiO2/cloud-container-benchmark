//
// Created by antio2 on 2025/10/21.
//



#pragma once
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <memory>
#include <vector>
#include <iostream>
#include "rocksdb_factory.h"
#include "config.h"
#include <rocksdb/comparator.h>
using namespace rocksdb;

std::unique_ptr<DB> initRocksDB(
        const Config& cfg,
        std::vector<std::unique_ptr<ColumnFamilyHandle>>& handles)
{
    Options options;
    options.create_if_missing = true;

    if (cfg.destroy_before_start_)
        DestroyDB(cfg.db_path_, options);
    return createRocksDB(cfg, handles);
}

std::unique_ptr<DB> createRocksDB(
        const Config& cfg,
        std::vector<std::unique_ptr<ColumnFamilyHandle>>& handles)
{
    std::vector<std::string> existingColumnFamilies;
    Status s = DB::ListColumnFamilies(DBOptions(), cfg.db_path_, &existingColumnFamilies);

    if (!s.ok()) {
        existingColumnFamilies = {kDefaultColumnFamilyName};
    }

    bool hasDefault = false;
    for (const auto& n : existingColumnFamilies)
        if (n == kDefaultColumnFamilyName) hasDefault = true;
    if (!hasDefault)
        existingColumnFamilies.push_back(kDefaultColumnFamilyName);

    // 根据 ts_type 设置 column family comparator
    std::vector<ColumnFamilyDescriptor> descriptors;
    for (const auto& name : existingColumnFamilies) {
        ColumnFamilyOptions cfOptions;
        if (cfg.ts_type_ == ts_type_t::udt) {
            cfOptions.comparator = rocksdb::BytewiseComparatorWithU64Ts();
        } else {
        }
        descriptors.emplace_back(name, cfOptions);
    }

    DBOptions dbOptions;
    dbOptions.create_if_missing = true;
    dbOptions.create_missing_column_families = true;

    DB* db = nullptr;
    std::vector<ColumnFamilyHandle*> rawHandles;
    s = DB::Open(dbOptions, cfg.db_path_, descriptors, &rawHandles, &db);
    if (!s.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " + s.ToString());
    }

    handles.clear();
    for (auto* h : rawHandles)
        handles.emplace_back(h);

    std::cout << "Opened RocksDB: " << cfg.db_path_
              << ", CF=" << handles.size()
              << ", ts_type=" << (cfg.ts_type_ == ts_type_t::udt ? "UDT" : "Retina") << std::endl;

    return std::unique_ptr<DB>(db);
}