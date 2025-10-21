//
// Created by antio2 on 2025/10/21.
//

#ifndef ROCKSDB_BENCH_ROCKSDB_FACTORY_H
#define ROCKSDB_BENCH_ROCKSDB_FACTORY_H


#include <memory>
#include <vector>
#include <rocksdb/db.h>

std::unique_ptr<rocksdb::DB> createRocksDB(
        const std::string& dbPath,
        std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>>& handles);

#endif //ROCKSDB_BENCH_ROCKSDB_FACTORY_H
