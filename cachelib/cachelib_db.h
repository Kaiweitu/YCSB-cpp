//
//  leveldb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_CachelibDB_DB_H_
#define YCSB_C_CachelibDB_DB_H_

#include <iostream>
#include <string>
#include <mutex>
#include <atomic>
#include <unordered_map>
#include <random>
#include "core/db.h"
#include "utils/properties.h"
#include <cachelib/allocator/Util.h>
#include <cachelib/allocator/CacheAllocator.h>
#include <cachelib/allocator/nvmcache/NvmCache.h>

namespace ycsbc
{
  using Cache = facebook::cachelib::LruAllocator; // or Lru2QAllocator, or TinyLFUAllocator
  using CacheConfig = typename Cache::Config;
  using CacheKey = typename Cache::Key;
  using CacheItemHandle = typename Cache::ItemHandle;
  using NvmCacheConfig = typename facebook::cachelib::NvmCache<Cache>::Config;

  class CachelibDB : public DB
  {
  public:
    CachelibDB() {}
    ~CachelibDB() {}

    void Init();

    void Cleanup();

    Status Read(const std::string &table, const std::string &key,
                const std::vector<std::string> *fields, std::vector<Field> &result)
    {
      return (this->*(method_read_))(table, key, fields, result);
    }

    Status Scan(const std::string &table, const std::string &key, int len,
                const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result)
    {
      return (this->*(method_scan_))(table, key, len, fields, result);
    }

    Status Update(const std::string &table, const std::string &key, std::vector<Field> &values)
    {
      return (this->*(method_update_))(table, key, values);
    }

    Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values)
    {
      return (this->*(method_insert_))(table, key, values);
    }

    Status Delete(const std::string &table, const std::string &key)
    {
      return (this->*(method_delete_))(table, key);
    }

    void RegisterThreadID(int thread_id) override
    {
      // auto cnt = thread_cnt.fetch_add(1);
      // assert(thread_id < pool_num_.size());

      // std::cout << "Address of x: " << std::hex << reinterpret_cast<std::uintptr_t>(&defaultPool_) << std::to_string((int)defaultPool_) << std::endl;
      defaultPool_ = poolID_[thread_id];
      // std::cout << thread_id << " " << poolID_.size() << std::endl;

      // thread_pool_mapping.insert({std::this_thread::get_id(), thread_id});
    }

  private:
    void SerializeRow(const std::vector<Field> &values, std::string *data);
    void DeserializeRowFilter(std::vector<Field> *values, const std::string &data,
                              const std::vector<std::string> &fields);
    void DeserializeRow(std::vector<Field> *values, const std::string &data);

    // void GetOptions(const utils::Properties &props, leveldb::Options *opt);
    Status find(const std::string &, const std::string &,
                const std::vector<std::string> *, std::vector<Field> &);

    Status remove(const std::string &table, const std::string &key);

    Status insertOrReplace(const std::string &table, const std::string &key,
                           std::vector<Field> &values);

    Status scan(const std::string &table, const std::string &key, int len,
                const std::vector<std::string> *fields,
                std::vector<std::vector<Field>> &result);

    Status (CachelibDB::*method_read_)(const std::string &, const std::string &,
                                       const std::vector<std::string> *, std::vector<Field> &);
    Status (CachelibDB::*method_scan_)(const std::string &, const std::string &, int,
                                       const std::vector<std::string> *,
                                       std::vector<std::vector<Field>> &);
    Status (CachelibDB::*method_update_)(const std::string &, const std::string &,
                                         std::vector<Field> &);
    Status (CachelibDB::*method_insert_)(const std::string &, const std::string &,
                                         std::vector<Field> &);
    Status (CachelibDB::*method_delete_)(const std::string &, const std::string &);

    int fieldcount_;
    std::string field_prefix_;
    std::vector<facebook::cachelib::PoolId> poolID_;
    int pool_num_{8};
    std::hash<std::string> hash_fn;
    std::unique_ptr<Cache> cache_;

    std::string prekey{"user"};
    std::unordered_map<std::thread::id, int> thread_pool_mapping;
    // std::atomic<int> thread_cnt{0};
    const int total_length{12};
    // thread_local facebook::cachelib::PoolId pool_;

    // static leveldb::DB *db_;
    // std::atomic<uint64_t> access_counter(0);
    thread_local static facebook::cachelib::PoolId defaultPool_;

    // Create a random number engine
    std::default_random_engine engine;

    // Use the random_device to seed the engine for more randomness
    std::random_device rd;

    static int ref_cnt_;
    std::mutex mu_;
    static int init_cnt_;
  };

  DB *NewCachelibDB();

} // ycsbc

#endif // YCSB_C_LEVELDB_DB_H_
