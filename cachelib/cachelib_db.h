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

#include "core/db.h"
#include "utils/properties.h"

namespace ycsbc
{

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

    // static leveldb::DB *db_;
    static int ref_cnt_;
    static std::mutex mu_;
  };

  DB *NewCachelibDB();

} // ycsbc

#endif // YCSB_C_LEVELDB_DB_H_
