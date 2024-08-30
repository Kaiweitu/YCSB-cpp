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

#include "cachelib/allocator/Util.h"
#include <cachelib/allocator/CacheAllocator.h>
#include <cachelib/allocator/nvmcache/NvmCache.h>
#include <folly/init/Init.h>

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
} // anonymous

namespace ycsbc
{

  using Cache = facebook::cachelib::LruAllocator; // or Lru2QAllocator, or TinyLFUAllocator
  using CacheConfig = typename Cache::Config;
  using CacheKey = typename Cache::Key;
  using CacheItemHandle = typename Cache::ItemHandle;
  using NvmCacheConfig = typename facebook::cachelib::NvmCache<Cache>::Config;

  // CachelibDB *CachelibDB::db_ = nullptr;
  std::unique_ptr<Cache> cache_;
  facebook::cachelib::PoolId defaultPool_;
  int CachelibDB::ref_cnt_ = 0;
  std::mutex CachelibDB::mu_;

  void CachelibDB::Init()
  {
    CacheConfig config;
    NvmCacheConfig nvmConfig;

    constexpr size_t GB = 1024ULL * 1024ULL * 1024ULL;
    constexpr size_t MB = 1024ULL * 1024ULL;
    constexpr size_t KB = 1024ULL;
    const utils::Properties &props = *props_;

    // Cache Configuration
    int cacheSize = std::stoi(props.GetProperty(PROP_CACHE_SIZE_MB, PROP_CACHE_SIZE_MB_DEFAULT));
    config.setCacheSize(cacheSize * (MB));
    config.enablePoolRebalancing(nullptr, std::chrono::seconds(0));
    config.setAccessConfig(typename Cache::AccessConfig{
        static_cast<uint32_t>(22),
        static_cast<uint32_t>(20)});
    config.configureChainedItems(typename Cache::AccessConfig{
        static_cast<uint32_t>(22),
        static_cast<uint32_t>(20)});

    // Nvm Cache Configuration
    nvmConfig.navyConfig.setBlockCacheDataChecksum(false);
    nvmConfig.enableFastNegativeLookups = true;

    const std::string &mode = props.GetProperty(PROP_MODE, PROP_MODE_DEFAULT);

    std::vector<uint64_t> nvmCacheSize;
    nvmCacheSize.push_back(std::stoi(props.GetProperty(PROP_CAP_CAPACITY_GB, PROP_CAP_CAPACITY_GB_DEFAULT)) * GB);
    nvmCacheSize.push_back(std::stoi(props.GetProperty(PROP_PERF_CAPACITY_GB, PROP_PERF_CAPACITY_GB_DEFAULT)) * GB);

    std::vector<std::string> nvmCachePaths;
    nvmCachePaths.push_back(props.GetProperty(PROP_CAP_DEVICE, PROP_CAP_DEVICE_DEFAULT));
    nvmCachePaths.push_back(props.GetProperty(PROP_PERF_DEVICE, PROP_PERF_DEVICE_DEFAULT));

    if (mode == "striping")
    {
      nvmConfig.navyConfig.setHierarchy(nvmCachePaths, nvmCacheSize, "raid");
    }
    else if (mode == "tiering" || mode == "most")
    {
      nvmConfig.navyConfig.setHierarchy(nvmCachePaths, nvmCacheSize, "tiering");
      nvmConfig.navyConfig.setMigrateRate(100 * MB);
      nvmConfig.navyConfig.setHotThreshold(4);
      nvmConfig.navyConfig.setCoolingThreshold(200);
      nvmConfig.navyConfig.setHotBlockRegionSize(
          100000 * MB);
      nvmConfig.navyConfig.setOffloadRatioMax(
          (uint8_t)100);
      nvmConfig.navyConfig.setOffloadRatioMin(
          (uint8_t)0);
      nvmConfig.navyConfig.setTieringPageSize(
          (uint64_t)2048 * KB);

      if (mode == "most")
        nvmConfig.navyConfig.setIsHBRTuningOn(true);
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
    defaultPool_ =
        cache_->addPool("default", cache_->getCacheMemoryStats().cacheSize);



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
    auto handle = cache_->allocate(defaultPool_, key, data.length(), 0);
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

  DB::Status CachelibDB::scan(const std::string &table, const std::string &key, int len,
                              const std::vector<std::string> *fields,
                              std::vector<std::vector<Field>> &result) {
   throw utils::Exception(std::string("Cachelib Scan not implemented"));

   return kOK; 
  }

  DB *NewCachelibDB()
  {
    return new CachelibDB;
  }

  const bool registered = DBFactory::RegisterDB("cachelib", NewCachelibDB);

} // ycsbc