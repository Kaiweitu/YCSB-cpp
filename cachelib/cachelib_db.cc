//
//  leveldb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "cachelib_db.h"
#include "core/db.h"
#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/utils.h"
#include <random>

#include <folly/init/Init.h>
#include <cassert>

namespace
{
  const std::string PROP_NAME = "cachelib.dbname";
  const std::string PROP_NAME_DEFAULT = "";

  const std::string PROP_CACHE_SIZE_MB = "cachelib.cache_size";
  const std::string PROP_CACHE_SIZE_MB_DEFAULT = "1024";

  const std::string PROP_MODE = "cachelib.mode";
  const std::string PROP_MODE_DEFAULT = "striping";

  const std::string PROP_PERF_DEVICE = "cachelib.perf_device";
  const std::string PROP_PERF_DEVICE_DEFAULT = "optane0";

  const std::string PROP_PERF_CAPACITY_GB = "cachelib.perf_cap_gb";
  const std::string PROP_PERF_CAPACITY_GB_DEFAULT = "100";

  const std::string PROP_CAP_DEVICE = "cachelib.cap_device";
  const std::string PROP_CAP_DEVICE_DEFAULT = "nvme0";

  const std::string PROP_CAP_CAPACITY_GB = "cachelib.cap_cap_gb";
  const std::string PROP_CAP_CAPACITY_GB_DEFAULT = "150";

  const std::string PROP_CACHING_WRITE_MODE = "cachelib.caching_write_mode";
  const std::string PROP_CACHING_WRITE_MODE_DEFAULT = "wb";

  const std::string PROP_BIGHASH_SIZE_PCT = "cachelib.bighash_size_pct";
  const std::string PROP_BIGHASH_SIZE_PCT_DEFAULT = "50";

  const std::string PROP_READER_THREAD = "cachelib.reader_thread";
  const std::string PROP_READER_THREAD_DEFAULT = "128";

  const std::string PROP_WRITER_THREAD = "cachelib.writer_thread";
  const std::string PROP_WRITER_THREAD_DEFAULT = "64";
  
  const std::string PROP_STEP_SIZE = "cachelib.step_size";
  const std::string PROP_STEP_SIZE_DEFAULT = "209715200";


  const std::string PROP_MIGRATE_RATE = "cachelib.migrate_rate";
  const std::string PROP_MIGRATE_RATE_DEFAULT = "200";
} // anonymous

namespace ycsbc
{

  // CachelibDB *CachelibDB::db_ = nullptr;

  int CachelibDB::ref_cnt_ = 0;
  int CachelibDB::init_cnt_ = 0;
  // std::mutex CachelibDB::mu_;
  thread_local facebook::cachelib::PoolId CachelibDB::defaultPool_ = 0;

  void CachelibDB::Init()
  {
    
    CacheConfig config;
    NvmCacheConfig nvmConfig;
    int init_id = init_cnt_;

    init_cnt_++;

    engine.seed(rd());

    constexpr size_t GB = 1024ULL * 1024ULL * 1024ULL;
    constexpr size_t MB = 1024ULL * 1024ULL;
    constexpr size_t KB = 1024ULL;
    const utils::Properties &props = *props_;

    // Cache Configuration
    int cacheSize = std::stoi(props.GetProperty(PROP_CACHE_SIZE_MB, PROP_CACHE_SIZE_MB_DEFAULT));
    config.setCacheSize(cacheSize * (MB));
    config.enablePoolRebalancing(nullptr, std::chrono::seconds(0));
    config.setAccessConfig(typename Cache::AccessConfig{
        static_cast<uint32_t>(25),
        static_cast<uint32_t>(20)});
    config.configureChainedItems(typename Cache::AccessConfig{
        static_cast<uint32_t>(25),
        static_cast<uint32_t>(20)});

    // Nvm Cache Configuration
    nvmConfig.navyConfig.setBlockCacheDataChecksum(false);
    nvmConfig.enableFastNegativeLookups = true;

    const std::string &mode = props.GetProperty(PROP_MODE, PROP_MODE_DEFAULT);

    std::vector<uint64_t> nvmCacheSize;
    nvmCacheSize.push_back(std::stoi(props.GetProperty(PROP_CAP_CAPACITY_GB, PROP_CAP_CAPACITY_GB_DEFAULT)) * GB);
    nvmCacheSize.push_back(std::stoi(props.GetProperty(PROP_PERF_CAPACITY_GB, PROP_PERF_CAPACITY_GB_DEFAULT)) * GB);

    std::vector<std::string> nvmCachePaths;
    nvmCachePaths.push_back(props.GetProperty(PROP_CAP_DEVICE, PROP_CAP_DEVICE_DEFAULT) + "p" + std::to_string(init_id + 1));
    nvmCachePaths.push_back(props.GetProperty(PROP_PERF_DEVICE, PROP_PERF_DEVICE_DEFAULT) + "p" + std::to_string(init_id + 1));
    std::cout << nvmCachePaths[0] << " " << nvmCachePaths[1] << std::endl;

    if (mode == "striping")
    {
      nvmConfig.navyConfig.setHierarchy(nvmCachePaths, nvmCacheSize, "raid");
    }
    else if (mode == "tiering" || mode == "most")
    {
      nvmConfig.navyConfig.setHierarchy(nvmCachePaths, nvmCacheSize, "tiering");
      nvmConfig.navyConfig.setMigrateRate(std::stoi(props.GetProperty(PROP_MIGRATE_RATE, PROP_MIGRATE_RATE_DEFAULT)) * MB);
      nvmConfig.navyConfig.setHotThreshold(8);
      nvmConfig.navyConfig.setCoolingThreshold(18);
      nvmConfig.navyConfig.setHotBlockRegionSize(
          100000 * MB);
      nvmConfig.navyConfig.setOffloadRatioMax(
          (uint8_t)100);
      nvmConfig.navyConfig.setOffloadRatioMin(
          (uint8_t)0);
      nvmConfig.navyConfig.setTieringPageSize(
          (uint64_t)2048 * KB);

      if (mode == "most") {
        nvmConfig.navyConfig.setIsHBRTuningOn(true);
        nvmConfig.navyConfig.setHotBlockRegionSize(
          std::stol(props.GetProperty(PROP_STEP_SIZE, PROP_STEP_SIZE_DEFAULT)) * MB);
      }
      else
        nvmConfig.navyConfig.setIsHBRTuningOn(false);
    }
    else if (mode == "caching")
    {
      nvmConfig.navyConfig.setWriteMode(props.GetProperty(PROP_CACHING_WRITE_MODE, PROP_CACHING_WRITE_MODE_DEFAULT));
      nvmConfig.navyConfig.setHierarchy(
          nvmCachePaths, nvmCacheSize, "caching");
      nvmConfig.navyConfig.setTieringPageSize(
          (uint64_t)2048 * KB);
    }

    nvmConfig.navyConfig.setBlockCacheNumInMemBuffers(128);
    nvmConfig.navyConfig.setNavyReqOrderingShards(21);
    nvmConfig.navyConfig.setBlockSize(4096);
    nvmConfig.navyConfig.setBlockCacheRegionSize(2 * MB);

    int navyBigHashSizePct = std::stoi(props.GetProperty(PROP_BIGHASH_SIZE_PCT, PROP_BIGHASH_SIZE_PCT_DEFAULT));
    if (navyBigHashSizePct > 0)
    {
      if (mode == "caching")
      {
        nvmConfig.navyConfig.setBigHash((int)navyBigHashSizePct,
                                        4096,
                                        8,
                                        2048);
      }
      else
      {
        nvmConfig.navyConfig.setBigHash(navyBigHashSizePct,
                                        4096,
                                        8,
                                        2048);
      };
    };

    nvmConfig.navyConfig.setReaderAndWriterThreads(
        std::stoi(props.GetProperty(PROP_READER_THREAD, PROP_READER_THREAD_DEFAULT)),
        std::stoi(props.GetProperty(PROP_WRITER_THREAD, PROP_WRITER_THREAD_DEFAULT)));

    nvmConfig.navyConfig.setBlockCacheCleanRegions(1);
    nvmConfig.navyConfig.setMaxConcurrentInserts(1000000);
    nvmConfig.navyConfig.setMaxParcelMemoryMB(1024);
    nvmConfig.truncateItemToOriginalAllocSizeInNvm = false;
    nvmConfig.navyConfig.setDeviceMaxWriteSize(1024 * 1024);

    config.enableNvmCache(nvmConfig);
    config.setNvmAdmissionMinTTL(0);
    config.cacheName = "ycsb-cachelib";

    cache_ = std::make_unique<Cache>(config);

    for (int i = 0; i < pool_num_; i++)
    {
      auto mmConfig = Cache::MMConfig(60,
                                      0.1,
                                      false,
                                      true,
                                      false,
                                      0);

      poolID_.push_back(
          cache_->addPool("default" + std::to_string(i), cache_->getCacheMemoryStats().cacheSize / pool_num_, {}, mmConfig, nullptr, nullptr, true));
    }
    // auto mmConfig = Cache::MMConfig(60,
    //                                 0,
    //                                 false,
    //                                 true,
    //                                 false,
    //                                 0);

    // defaultPool_ =
    //     cache_->addPool("default", cache_->getCacheMemoryStats().cacheSize, {}, mmConfig, nullptr, nullptr, true);

    // defaultPool_ = cache_->addPool("default", cache_->getCacheMemoryStats().cacheSize);

    method_read_ = &CachelibDB::find;
    method_scan_ = &CachelibDB::scan;
    method_update_ = &CachelibDB::insertOrReplace;
    method_insert_ = &CachelibDB::insertOrReplace;
    method_delete_ = &CachelibDB::remove;

    return;
  }

  void CachelibDB::Cleanup()
  {
  }

  void CachelibDB::SerializeRow(const std::vector<Field> &values, std::string *data)
  {
    for (const Field &field : values)
    {
      uint32_t len = field.name.size();
      data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
      data->append(field.name.data(), field.name.size());
      len = field.value.size();
      data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
      data->append(field.value.data(), field.value.size());
    }
  }

  void CachelibDB::DeserializeRowFilter(std::vector<Field> *values, const std::string &data,
                                        const std::vector<std::string> &fields)
  {
    const char *p = data.data();
    const char *lim = p + data.size();

    std::vector<std::string>::const_iterator filter_iter = fields.begin();
    while (p != lim && filter_iter != fields.end())
    {
      assert(p < lim);
      uint32_t len = *reinterpret_cast<const uint32_t *>(p);
      p += sizeof(uint32_t);
      std::string field(p, static_cast<const size_t>(len));
      p += len;
      len = *reinterpret_cast<const uint32_t *>(p);
      p += sizeof(uint32_t);
      std::string value(p, static_cast<const size_t>(len));
      p += len;
      if (*filter_iter == field)
      {
        values->push_back({field, value});
        filter_iter++;
      }
    }
    assert(values->size() == fields.size());
  }

  void CachelibDB::DeserializeRow(std::vector<Field> *values, const std::string &data)
  {
    const char *p = data.data();
    const char *lim = p + data.size();
    while (p != lim)
    {
      assert(p < lim);
      uint32_t len = *reinterpret_cast<const uint32_t *>(p);
      p += sizeof(uint32_t);
      std::string field(p, static_cast<const size_t>(len));
      p += len;
      len = *reinterpret_cast<const uint32_t *>(p);
      p += sizeof(uint32_t);
      std::string value(p, static_cast<const size_t>(len));
      p += len;
      values->push_back({field, value});
    }
    assert(values->size() == fieldcount_);
  }

  DB::Status CachelibDB::find(const std::string &table, const std::string &key,
                              const std::vector<std::string> *fields, std::vector<Field> &result)
  {

    auto it = cache_->find((CacheKey)key, facebook::cachelib::AccessMode::kRead);
    it.wait();
    if (!it)
      return kNotFound;

    std::string data(reinterpret_cast<const char *>(it->getMemory()), it->getSize());
    if (fields != nullptr)
      DeserializeRowFilter(&result, data, *fields);
    else
      DeserializeRow(&result, data);

    return kOK;
  }

  DB::Status CachelibDB::insertOrReplace(const std::string &table, const std::string &key,
                                         std::vector<Field> &values)
  {
    std::string data;
    SerializeRow(values, &data);

    std::uniform_int_distribution<int> dist(0, 1);
    // defaultPool_
    // auto id = thread_pool_mapping[std::this_thread::get_id()];
    // assert(id < poolID_.size());
    // auto handle = cache_->allocate(poolID_[dist(engine)], key, data.length(), 0);
    auto handle = cache_->allocate(hash_fn(key) % pool_num_, key, data.length(), 0);
    if (!handle)
      throw utils::Exception(std::string("Cachelib Put failed "));

    std::memcpy(handle->getWritableMemory(), data.data(), data.size());
    cache_->insertOrReplace(handle);

    return kOK;
  }

  DB::Status CachelibDB::remove(const std::string &table, const std::string &key)
  {
    // leveldb::WriteOptions wopt;
    auto rv = cache_->remove(key);

    if (rv == Cache::RemoveRes::kNotFoundInRam)
      throw utils::Exception(std::string("Cachelib Delete failed"));

    return kOK;
  }

  uint64_t extractNumber(const std::string &key)
  {
    // // Find the position where the numeric part starts
    // std::size_t pos = key.find_first_of("0123456789");

    // if (pos == std::string::npos)
    // {
    //   // No digits found in the string
    //   throw std::invalid_argument("No numeric part found in the key string");
    // }

    // Extract the numeric part of the string
    std::string numberStr = key.substr(4);

    // Convert the numeric part to uint64_t
    try
    {
      uint64_t number = std::stoull(numberStr);
      return number;
    }
    catch (const std::invalid_argument &e)
    {
      // Handle case where the conversion fails
      throw std::invalid_argument("Invalid number format in key string");
    }
    catch (const std::out_of_range &e)
    {
      // Handle case where the number is out of range for uint64_t
      throw std::out_of_range("Number in key string is out of uint64_t range");
    }
  }

  DB::Status CachelibDB::scan(const std::string &table, const std::string &key, int len,
                              const std::vector<std::string> *fields,
                              std::vector<std::vector<Field>> &result)
  {
    uint64_t key_offset = 0;
    uint64_t key_num_base = extractNumber(key);
    int found = 0;
    int offset_max = 100;

    // Precompute the length of the fixed portion of the key to avoid repeated calculations
    const int prekey_length = prekey.size();
    char cur_key[prekey_length + total_length + 1];      // +1 for null-terminator
    std::memcpy(cur_key, prekey.c_str(), prekey_length); // Copy the fixed prefix
    cur_key[prekey_length + total_length] = '\0';        // Null-terminate

    while (true)
    {
      // Directly convert number to string and place it into cur_key
      uint64_t current_key_num = key_num_base + key_offset;
      int digits_written = snprintf(cur_key + prekey_length, total_length + 1, "%012lu", current_key_num);

      // Perform the cache lookup
      auto it = cache_->find((CacheKey)cur_key, facebook::cachelib::AccessMode::kRead);
      it.wait();
      if (it)
      {
        std::string data(reinterpret_cast<const char *>(it->getMemory()), it->getSize());
        result.emplace_back(); // Use emplace_back for efficiency
        std::vector<Field> &values = result.back();

        // Deserialize directly to the vector
        if (fields != nullptr)
        {
          DeserializeRowFilter(&values, data, *fields);
        }
        else
        {
          DeserializeRow(&values, data);
        }
        assert(values.size() == static_cast<size_t>(fieldcount_));
        found++;
      }

      // Check termination conditions
      if (found == len || key_offset == offset_max)
        break;
      key_offset++;
    }

    return kOK;
  }
  // void CachelibDB::RegisterThreadID(int thread_id) 
  // {
  //   // auto cnt = thread_cnt.fetch_add(1);
  //   assert(thread_id < pool_num_.size());
  //   defaultPool_ = poolID_[thread_id];

  //   std::cout << "Address of x: " << std::hex << reinterpret_cast<std::uintptr_t>(&defaultPool_) << std::to_string((int) defaultPool_) << std::endl;

  //   // thread_pool_mapping.insert({thread_id, poolID_[cnt % poolID_.size()]});
  // };

  DB *NewCachelibDB()
  {
    return new CachelibDB;
  }

  const bool registered = DBFactory::RegisterDB("cachelib", NewCachelibDB);

} // ycsbc