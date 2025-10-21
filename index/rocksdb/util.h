//
// Created by antio2 on 2025/10/21.
//

#ifndef ROCKSDB_BENCH_UTIL_H
#define ROCKSDB_BENCH_UTIL_H
#include <rocksdb/slice.h>
#include <string>

inline rocksdb::Slice longToSlice(long long value, std::string& buf) {
    buf.resize(sizeof(long long));           // make sure buffer has 8 bytes
    memcpy(&buf[0], &value, sizeof(long long)); // serialize value into buffer
    return rocksdb::Slice(buf);
}

#endif //ROCKSDB_BENCH_UTIL_H
