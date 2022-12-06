#ifndef SRC_REDIS_EHASHES_H_
#define SRC_REDIS_EHASHES_H_

#include <string>
#include <vector>

#include "src/redis.h"

namespace blackwidow {

class RedisEhashes : public Redis {
public:
    RedisEhashes() = default;
    ~RedisEhashes();

    // Common Commands
    Status Open(BlackwidowOptions bw_options, const std::string& db_path) override;
    Status ResetOption(const std::string& key, const std::string& value);
    Status ResetDBOption(const std::string& key, const std::string& value);
    Status CompactRange(const rocksdb::Slice* begin, const rocksdb::Slice* end) override;
    Status GetProperty(const std::string& property, uint64_t* out) override;
    Status ScanKeyNum(uint64_t* num) override;
    Status ScanKeys(const std::string& pattern, std::vector<std::string>* keys) override;
    void GetColumnFamilyHandles(std::vector<rocksdb::ColumnFamilyHandle*>& handles) override;

    // Ehashs Commands
    Status Ehset(const Slice& key, const Slice& field, const Slice& value);
    Status Ehsetnx(const Slice& key, const Slice& field, const Slice& value, int32_t* ret, int32_t ttl);
    Status Ehsetxx(const Slice& key, const Slice& field, const Slice& value, int32_t* ret, int32_t ttl);
    Status Ehsetex(const Slice& key, const Slice& field, const Slice& value, int32_t ttl);
    int32_t Ehexpire(const Slice& key, const Slice& field, int32_t ttl);
    int32_t Ehexpireat(const Slice& key, const Slice& field, int32_t timestamp);
    int64_t Ehttl(const Slice& key, const Slice& field);
    int32_t Ehpersist(const Slice& key, const Slice& field);
    Status Ehget(const Slice& key, const Slice& field, std::string* value);
    Status Ehexists(const Slice& key, const Slice& field);
    Status Ehdel(const Slice& key, const std::vector<std::string>& fields, int32_t* ret);
    Status Ehlen(const Slice& key, int32_t* ret);
    Status EhlenForce(const Slice& key, int32_t* ret);
    Status Ehstrlen(const Slice& key, const Slice& field, int32_t* len);
    Status Ehincrby(const Slice& key, const Slice& field, int64_t value, int64_t* ret, int32_t ttl);
    Status Ehincrbynxex(const Slice& key, const Slice& field, int64_t value, int64_t* ret, int32_t ttl);
    Status Ehincrbyxxex(const Slice& key, const Slice& field, int64_t value, int64_t* ret, int32_t ttl);
    Status Ehincrbyfloat(const Slice& key, const Slice& field, const Slice& by, std::string* new_value, int32_t ttl);
    Status Ehincrbyfloatnxex(const Slice& key, const Slice& field, const Slice& by, std::string* new_value, int32_t ttl);
    Status Ehincrbyfloatxxex(const Slice& key, const Slice& field, const Slice& by, std::string* new_value, int32_t ttl);
    Status Ehmset(const Slice& key, const std::vector<FieldValue>& fvs);
    Status Ehmsetex(const Slice& key, const std::vector<FieldValueTTL>& fvts);
    Status Ehmget(const Slice& key, const std::vector<std::string>& fields, std::vector<ValueStatus>* vss);
    Status Ehkeys(const Slice& key, std::vector<std::string>* fields);
    Status Ehvals(const Slice& key, std::vector<std::string>* values);
    Status Ehgetall(const Slice& key, std::vector<FieldValueTTL>* fvts);
    Status Ehscan(const Slice& key, int64_t cursor,
                  const std::string& pattern, int64_t count,
                  std::vector<FieldValueTTL>* fvts, int64_t* next_cursor);
    Status Ehscanx(const Slice& key, const std::string start_field,
                   const std::string& pattern, int64_t count,
                   std::vector<FieldValueTTL>* fvts,
                   std::string* next_field);
    Status PKEHScanRange(const Slice& key,
                        const Slice& field_start,
                        const std::string& field_end,
                        const Slice& pattern, int32_t limit,
                        std::vector<FieldValue>* field_values,
                        std::string* next_field);
    Status PKEHRScanRange(const Slice& key,
                         const Slice& field_start, const std::string& field_end,
                         const Slice& pattern, int32_t limit,
                         std::vector<FieldValue>* field_values,
                         std::string* next_field);
    Status PKScanRange(const Slice& key_start, const Slice& key_end,
                       const Slice& pattern, int32_t limit,
                       std::vector<std::string>* keys, std::string* next_key);
    Status PKRScanRange(const Slice& key_start, const Slice& key_end,
                        const Slice& pattern, int32_t limit,
                        std::vector<std::string>* keys, std::string* next_key);

    // Keys Commands
    Status Expire(const Slice& key, int32_t ttl) override;
    Status Del(const Slice& key) override;
    bool Scan(const std::string& start_key,
              const std::string& pattern,
              std::vector<std::string>* keys,
              int64_t* count, std::string* next_key) override;
    Status Expireat(const Slice& key, int32_t timestamp) override;
    Status Persist(const Slice& key) override;
    Status TTL(const Slice& key, int64_t* timestamp) override;

    // Iterate all data
    void ScanDatabase();

private:
    std::vector<rocksdb::ColumnFamilyHandle*> handles_;
};

} //  namespace blackwidow

#endif
