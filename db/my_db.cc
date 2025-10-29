#include "db/my_db.hh"
#include "core/properties.h"  // YCSB 属性系统
#include <cassert>
#include <iostream>
#include <memory>
using namespace std;
namespace ycsbc {

thread_local std::unique_ptr<MyDB::ThreadLocal> MyDB::tls_ = nullptr;

void MyDB::Init() {
  // 全局初始化（只做一次）
  std::call_once(global_init_flag_, [&]() {
    // TODO: 这里放你的 DB 的全局初始化，例如打开引擎、加载元数据等
    // e.g., engine_open(db_path_.c_str(), create_if_missing_);
    inited_.store(true, std::memory_order_release);
  });
}

void MyDB::Close(){
  return;
}


int MyDB::Read(const string & /*table*/, const string &key,
               const vector<string> * /*fields*/,
               vector<KVPair> &result) {
  assert(tls_ && "InitThread not called?");
  // TODO: 调你的 DB 的 Get(key)
  // string value;
  // bool ok = engine_get(tls_->handle.get(), key, &value);
  bool ok = true; // mock
  string value = "mock_value"; // mock

  if (!ok) return kNotFound;
  result.clear();
  result.emplace_back("field0", value); // YCSB 需要 KVPair 列表
  return kOK;
}

int MyDB::Insert(const string & /*table*/, const string &key, vector<KVPair> &values) {
  assert(tls_ && "InitThread not called?");
  // YCSB 的 values 是一个字段列表，通常我们把它序列化成一个 value
  // 你也可以只取第一个字段（取决于你的引擎 KV 模型）
  // 例：拼接 "field=value;" 或直接 values[0].second
  string value;
  if (!values.empty()) value = values[0].second;
  // bool ok = engine_put(tls_->handle.get(), key, value);
  bool ok = true; // mock
  return ok ? kOK : kError;
}

int MyDB::Update(const string & /*table*/, const string &key, vector<KVPair> &values) {
  // 对 KV 存储，Update 等同 Put
  return Insert("", key, values);
}

int MyDB::Delete(const string & /*table*/, const string &key) {
  assert(tls_ && "InitThread not called?");
  // bool ok = engine_delete(tls_->handle.get(), key);
  bool ok = true; // mock
  return ok ? kOK : kError;
}

int MyDB::Scan(const string & /*table*/, const string &start_key, int record_count,
               const vector<string> * /*fields*/,
               vector<vector<KVPair>> &result) {
  assert(tls_ && "InitThread not called?");
  // TODO: 实现从 start_key 开始的正向迭代，最多取 record_count 条
  // 伪代码：
  // auto it = engine_seek(tls_->handle.get(), start_key);
  // for (int i = 0; i < record_count && it.valid(); ++i, it.next()) { ... }
  result.clear();
  for (int i = 0; i < record_count; ++i) {
    vector<KVPair> row;
    row.emplace_back("field0", "mock_scan_value");
    result.emplace_back(std::move(row));
  }
  return kOK;
}


} // namespace ycsbc
