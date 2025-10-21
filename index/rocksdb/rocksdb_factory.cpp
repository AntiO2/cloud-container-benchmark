//
// Created by antio2 on 2025/10/21.
//

#include "rocksdb_factory.h"
#pragma once
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <memory>
#include <vector>
#include <iostream>

using namespace rocksdb;

std::unique_ptr<DB> createRocksDB(
        const std::string& dbPath,
        std::vector<std::unique_ptr<ColumnFamilyHandle>>& handles)
{
    std::vector<std::string> existingColumnFamilies;
    Status s = DB::ListColumnFamilies(DBOptions(), dbPath, &existingColumnFamilies);

    if (!s.ok()) {
        existingColumnFamilies = {kDefaultColumnFamilyName};
    }

    bool hasDefault = false;
    for (const auto& n : existingColumnFamilies)
        if (n == kDefaultColumnFamilyName) hasDefault = true;
    if (!hasDefault)
        existingColumnFamilies.push_back(kDefaultColumnFamilyName);

    std::vector<ColumnFamilyDescriptor> descriptors;
    for (const auto& name : existingColumnFamilies) {
        descriptors.emplace_back(name, ColumnFamilyOptions());
    }

    DBOptions dbOptions;
    dbOptions.create_if_missing = true;
    dbOptions.create_missing_column_families = true;

    DB* db = nullptr;
    std::vector<ColumnFamilyHandle*> rawHandles;
    s = DB::Open(dbOptions, dbPath, descriptors, &rawHandles, &db);
    if (!s.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " + s.ToString());
    }

    handles.clear();
    for (auto* h : rawHandles)
        handles.emplace_back(h);

    std::cout << "Opened RocksDB: " << dbPath << ", CF=" << handles.size() << std::endl;
    return std::unique_ptr<DB>(db);
}
