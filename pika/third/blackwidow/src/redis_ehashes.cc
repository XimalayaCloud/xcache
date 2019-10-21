#include <memory>
#include <unordered_set>

#include "src/redis_ehashes.h"

#include "blackwidow/util.h"
#include "src/base_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "src/strings_value_format.h"
#include "src/ehashes_filter.h"


namespace blackwidow {

using EhashesValue = StringsValue;
using ParsedEhashesValue = ParsedStringsValue;

RedisEhashes::~RedisEhashes() {
    std::vector<rocksdb::ColumnFamilyHandle*> tmp_handles = handles_;
    handles_.clear();
    for (auto handle : tmp_handles) {
        delete handle;
    }
}

Status RedisEhashes::Open(const BlackwidowOptions& bw_options,
                          const std::string& db_path) {
    rocksdb::Options ops(bw_options.options);
    Status s = rocksdb::DB::Open(ops, db_path, &db_);
    if (s.ok()) {
        // create column family
        rocksdb::ColumnFamilyHandle* cf;
        s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "data_cf", &cf);
        if (!s.ok()) {
            return s;
        }
        // close DB
        delete cf;
        delete db_;
    }

    // Open
    rocksdb::DBOptions db_ops(bw_options.options);
    rocksdb::ColumnFamilyOptions meta_cf_ops(bw_options.options);
    rocksdb::ColumnFamilyOptions data_cf_ops(bw_options.options);
    meta_cf_ops.compaction_filter_factory = std::make_shared<EhashesMetaFilterFactory>();
    data_cf_ops.compaction_filter_factory = std::make_shared<EhashesDataFilterFactory>(&db_, &handles_);

    // use the bloom filter policy to reduce disk reads
    rocksdb::BlockBasedTableOptions table_ops(bw_options.table_options);
    table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
    rocksdb::BlockBasedTableOptions meta_cf_table_ops(table_ops);
    rocksdb::BlockBasedTableOptions data_cf_table_ops(table_ops);
    if (!bw_options.share_block_cache && bw_options.block_cache_size > 0) {
        meta_cf_table_ops.block_cache = rocksdb::NewLRUCache(bw_options.block_cache_size);
        data_cf_table_ops.block_cache = rocksdb::NewLRUCache(bw_options.block_cache_size);
    }
    meta_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(meta_cf_table_ops));
    data_cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(data_cf_table_ops));

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    // Meta CF
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, meta_cf_ops));
    // Data CF
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("data_cf", data_cf_ops));

    db_ops.rate_limiter = bw_options.rate_limiter;
    default_write_options_.disableWAL = bw_options.disable_wal;

    return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisEhashes::ResetOption(const std::string& key, const std::string& value) {
    Status s = GetDB()->SetOptions(handles_[0], {{key,value}});
    if (!s.ok()) {
        return s;
    }
    return GetDB()->SetOptions(handles_[1], {{key,value}});
}

Status RedisEhashes::CompactRange(const rocksdb::Slice* begin,
                                  const rocksdb::Slice* end) {
    Status s = db_->CompactRange(default_compact_range_options_, handles_[0], begin, end);
    if (!s.ok()) {
        return s;
    }
    return db_->CompactRange(default_compact_range_options_, handles_[1], begin, end);
}

Status RedisEhashes::GetProperty(const std::string& property, uint64_t* out) {
    std::string value;
    db_->GetProperty(handles_[0], property, &value);
    *out = std::strtoull(value.c_str(), NULL, 10);
    db_->GetProperty(handles_[1], property, &value);
    *out += std::strtoull(value.c_str(), NULL, 10);
    return Status::OK();
}

Status RedisEhashes::ScanKeyNum(uint64_t* num) {
    uint64_t count = 0;
    rocksdb::ReadOptions iterator_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    iterator_options.snapshot = snapshot;
    iterator_options.fill_cache = false;

    rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(iter->value());
        if (parsed_hashes_meta_value.count() != 0
            && !parsed_hashes_meta_value.IsStale()) {
            count++;
        }
    }
    *num = count;
    delete iter;
    return Status::OK();
}

Status RedisEhashes::ScanKeys(const std::string& pattern,
                              std::vector<std::string>* keys) {
    std::string key;
    rocksdb::ReadOptions iterator_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    iterator_options.snapshot = snapshot;
    iterator_options.fill_cache = false;

    rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(iter->value());
        if (parsed_hashes_meta_value.count() != 0
            && !parsed_hashes_meta_value.IsStale()) {
            key = iter->key().ToString();
            if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
                keys->push_back(key);
            }
        }
    }
    delete iter;
    return Status::OK();
}

Status RedisEhashes::Ehset(const Slice& key, const Slice& field,
                           const Slice& value, int32_t* ret) {
    rocksdb::WriteBatch batch;
    ScopeRecordLock l(lock_mgr_, key);

    int32_t version = 0;
    std::string meta_value;
    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            version = parsed_hashes_meta_value.InitialMetaValue();
            parsed_hashes_meta_value.ModifyCount(1);
            batch.Put(handles_[0], key, meta_value);
            HashesDataKey data_key(key, version, field);
            EhashesValue ehashes_value(value);
            batch.Put(handles_[1], data_key.Encode(), ehashes_value.Encode());
            *ret = 1;
        } else {
            version = parsed_hashes_meta_value.version();
            std::string data_value;
            HashesDataKey hashes_data_key(key, version, field);
            s = db_->Get(default_read_options_, handles_[1], hashes_data_key.Encode(), &data_value);
            if (s.ok()) {
                ParsedEhashesValue parsed_ehashes_value(&data_value);
                *ret = parsed_ehashes_value.IsStale() ? 1 : 0;
                EhashesValue ehashes_value(value);
                batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
            } else if (s.IsNotFound()) {
                parsed_hashes_meta_value.ModifyCount(1);
                batch.Put(handles_[0], key, meta_value);
                EhashesValue ehashes_value(value);
                batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
                *ret = 1;
            } else {
                return s;
            }
        }
    } else if (s.IsNotFound()) {
        char str[4];
        EncodeFixed32(str, 1);
        HashesMetaValue meta_value(std::string(str, sizeof(int32_t)));
        version = meta_value.UpdateVersion();
        batch.Put(handles_[0], key, meta_value.Encode());
        HashesDataKey data_key(key, version, field);
        EhashesValue ehashes_value(value);
        batch.Put(handles_[1], data_key.Encode(), ehashes_value.Encode());
        *ret = 1;
    } else {
        return s;
    }

    return db_->Write(default_write_options_, &batch);
}

Status RedisEhashes::Ehsetnx(const Slice& key, const Slice& field,
                             const Slice& value, int32_t* ret) {
    rocksdb::WriteBatch batch;
    ScopeRecordLock l(lock_mgr_, key);

    int32_t version = 0;
    std::string meta_value;
    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            version = parsed_hashes_meta_value.UpdateVersion();
            parsed_hashes_meta_value.set_count(1);
            parsed_hashes_meta_value.set_timestamp(0);
            batch.Put(handles_[0], key, meta_value);
            HashesDataKey hashes_data_key(key, version, field);
            EhashesValue ehashes_value(value);
            batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
            *ret = 1;
        } else {
            version = parsed_hashes_meta_value.version();
            HashesDataKey hashes_data_key(key, version, field);
            std::string data_value;
            s = db_->Get(default_read_options_, handles_[1], hashes_data_key.Encode(), &data_value);
            if (s.ok()) {
                ParsedEhashesValue parsed_ehashes_value(&data_value);
                if (parsed_ehashes_value.IsStale()) {
                    *ret = 1;
                    EhashesValue ehashes_value(value);
                    batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
                } else {
                    *ret = 0;
                }
            } else if (s.IsNotFound()) {
                parsed_hashes_meta_value.ModifyCount(1);
                batch.Put(handles_[0], key, meta_value);
                EhashesValue ehashes_value(value);
                batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
                *ret = 1;
            } else {
                return s;
            }
        }
    } else if (s.IsNotFound()) {
        char str[4];
        EncodeFixed32(str, 1);
        HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
        version = hashes_meta_value.UpdateVersion();
        batch.Put(handles_[0], key, hashes_meta_value.Encode());
        HashesDataKey hashes_data_key(key, version, field);
        EhashesValue ehashes_value(value);
        batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
        *ret = 1;
    } else {
        return s;
    }
    return db_->Write(default_write_options_, &batch);
}

Status RedisEhashes::Ehsetex(const Slice& key, const Slice& field,
                             const Slice& value, int32_t ttl) {
    if (ttl <= 0) {
        return Status::InvalidArgument("invalid expire time");
    }

    rocksdb::WriteBatch batch;
    ScopeRecordLock l(lock_mgr_, key);

    int32_t version = 0;
    std::string meta_value;
    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            version = parsed_hashes_meta_value.InitialMetaValue();
            parsed_hashes_meta_value.ModifyCount(1);
            batch.Put(handles_[0], key, meta_value);
            HashesDataKey data_key(key, version, field);
            EhashesValue ehashes_value(value);
            ehashes_value.SetRelativeTimestamp(ttl);
            batch.Put(handles_[1], data_key.Encode(), ehashes_value.Encode());
        } else {
            version = parsed_hashes_meta_value.version();
            std::string data_value;
            HashesDataKey hashes_data_key(key, version, field);
            s = db_->Get(default_read_options_, handles_[1], hashes_data_key.Encode(), &data_value);
            if (s.ok()) {
                EhashesValue ehashes_value(value);
                ehashes_value.SetRelativeTimestamp(ttl);
                batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
            } else if (s.IsNotFound()) {
                parsed_hashes_meta_value.ModifyCount(1);
                batch.Put(handles_[0], key, meta_value);
                EhashesValue ehashes_value(value);
                ehashes_value.SetRelativeTimestamp(ttl);
                batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
            } else {
                return s;
            }
        }
    } else if (s.IsNotFound()) {
        char str[4];
        EncodeFixed32(str, 1);
        HashesMetaValue meta_value(std::string(str, sizeof(int32_t)));
        version = meta_value.UpdateVersion();
        batch.Put(handles_[0], key, meta_value.Encode());
        HashesDataKey data_key(key, version, field);
        EhashesValue ehashes_value(value);
        ehashes_value.SetRelativeTimestamp(ttl);
        batch.Put(handles_[1], data_key.Encode(), ehashes_value.Encode());
    } else {
        return s;
    }

    return db_->Write(default_write_options_, &batch);
}

int32_t RedisEhashes::Ehexpire(const Slice& key, const Slice& field, int32_t ttl) {
    if (ttl <= 0) {
        return 0;
    }

    ScopeRecordLock l(lock_mgr_, key);

    std::string meta_value;
    int32_t version = 0;
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0
            || parsed_hashes_meta_value.IsStale()) {
            return 0;
        } else {
            version = parsed_hashes_meta_value.version();
            HashesDataKey data_key(key, version, field);
            std::string data_value;
            s = db_->Get(read_options, handles_[1], data_key.Encode(), &data_value);
            if (s.ok()) {
                ParsedEhashesValue parsed_ehashes_value(&data_value);
                if (parsed_ehashes_value.IsStale()) {
                    return 0;
                } else {
                    parsed_ehashes_value.SetRelativeTimestamp(ttl);
                    db_->Put(default_write_options_, handles_[1], data_key.Encode(), data_value);
                    return 1; 
                }
            } else if (s.IsNotFound()) {
                return 0;
            } else {
                return -1;
            }
        }
    }

    return 0;
}

int32_t RedisEhashes::Ehexpireat(const Slice& key, const Slice& field, int32_t timestamp) {
    if (timestamp <= 0) {
        return 0;
    }

    ScopeRecordLock l(lock_mgr_, key);

    std::string meta_value;
    int32_t version = 0;
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0
            || parsed_hashes_meta_value.IsStale()) {
            return 0;
        } else {
            version = parsed_hashes_meta_value.version();
            HashesDataKey data_key(key, version, field);
            std::string data_value;
            s = db_->Get(read_options, handles_[1], data_key.Encode(), &data_value);
            if (s.ok()) {
                ParsedEhashesValue parsed_ehashes_value(&data_value);
                if (parsed_ehashes_value.IsStale()) {
                    return 0;
                } else {
                    parsed_ehashes_value.set_timestamp(timestamp);
                    db_->Put(default_write_options_, handles_[1], data_key.Encode(), data_value);
                    return 1;
                }
            } else if (s.IsNotFound()) {
                return 0;
            } else {
                return -1;
            }
        }
    }

    return 0;
}

int64_t RedisEhashes::Ehttl(const Slice& key, const Slice& field) {
    std::string meta_value;
    int32_t version = 0;
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            return -2;
        } else {
            version = parsed_hashes_meta_value.version();
            HashesDataKey data_key(key, version, field);
            std::string data_value;
            s = db_->Get(read_options, handles_[1], data_key.Encode(), &data_value);
            if (s.ok()) {
                ParsedEhashesValue parsed_ehashes_value(&data_value);
                if (parsed_ehashes_value.IsStale()) {
                    return -2;
                } else {
                    int64_t timestamp = parsed_ehashes_value.timestamp();
                    if (timestamp == 0) {
                        return -1;
                    } else {
                        int64_t curtime;
                        rocksdb::Env::Default()->GetCurrentTime(&curtime);
                        return (timestamp - curtime >= 0) ? timestamp - curtime : -2;
                    }
                }
            } else if (s.IsNotFound()) {
                return -2;
            } else {
                return -3;
            }
        }
    }

    return -3;
}

int32_t RedisEhashes::Ehpersist(const Slice& key, const Slice& field) {
    ScopeRecordLock l(lock_mgr_, key);

    std::string meta_value;
    int32_t version = 0;
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            return 0;
        } else {
            version = parsed_hashes_meta_value.version();
            HashesDataKey data_key(key, version, field);
            std::string data_value;
            s = db_->Get(read_options, handles_[1], data_key.Encode(), &data_value);
            if (s.ok()) {
                ParsedEhashesValue parsed_ehashes_value(&data_value);
                if (parsed_ehashes_value.IsStale()) {
                    return 0;
                } else {
                    int32_t timestamp = parsed_ehashes_value.timestamp();
                    if (timestamp == 0) {
                        return 0;
                    } else {
                        parsed_ehashes_value.set_timestamp(0);
                        db_->Put(default_write_options_, handles_[1], data_key.Encode(), data_value);
                        return 1;
                    }
                }
            } else if (s.IsNotFound()) {
                return 0;
            } else {
                return -1;
            }
        }
    }

    return 0;
}

Status RedisEhashes::Ehget(const Slice& key, const Slice& field, std::string* value) {
    value->clear();
    std::string meta_value;
    int32_t version = 0;
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0) {
            return Status::NotFound();
        } else if (parsed_hashes_meta_value.IsStale()) {
            return Status::NotFound("Stale");
        } else {
            version = parsed_hashes_meta_value.version();
            HashesDataKey data_key(key, version, field);
            s = db_->Get(read_options, handles_[1], data_key.Encode(), value);
            if (s.ok()) {
                ParsedEhashesValue parsed_ehashes_value(value);
                if (parsed_ehashes_value.IsStale()) {
                    return Status::NotFound("Stale");
                } else {
                    parsed_ehashes_value.StripSuffix();
                }
            }
        }
    }
    return s;
}

Status RedisEhashes::Ehexists(const Slice& key, const Slice& field) {
    std::string value;
    return Ehget(key, field, &value);
}

Status RedisEhashes::Ehdel(const Slice& key, const std::vector<std::string>& fields, int32_t* ret) {
    std::vector<std::string> filtered_fields;
    std::unordered_set<std::string> field_set;
    for (auto iter = fields.begin(); iter != fields.end(); ++iter) {
        std::string field = *iter;
        if (field_set.find(field) == field_set.end()) {
            field_set.insert(field);
            filtered_fields.push_back(*iter);
        }
    }

    rocksdb::WriteBatch batch;
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;

    std::string meta_value;
    int32_t del_cnt = 0;
    int32_t version = 0;
    ScopeRecordLock l(lock_mgr_, key);
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            *ret = 0;
            return Status::OK();
        } else {
            std::string data_value;
            version = parsed_hashes_meta_value.version();
            int32_t hlen = parsed_hashes_meta_value.count();
            for (const auto& field : filtered_fields) {
                HashesDataKey hashes_data_key(key, version, field);
                s = db_->Get(read_options, handles_[1], hashes_data_key.Encode(), &data_value);
                if (s.ok()) {
                    del_cnt++;
                    batch.Delete(handles_[1], hashes_data_key.Encode());
                } else if (s.IsNotFound()) {
                    continue;
                } else {
                    return s;
                }
            }
            *ret = del_cnt;
            hlen -= del_cnt;
            parsed_hashes_meta_value.set_count(hlen);
            batch.Put(handles_[0], key, meta_value);
        }
    } else if (s.IsNotFound()) {
        *ret = 0;
        return Status::OK();
    } else {
        return s;
    }
    return db_->Write(default_write_options_, &batch);
}

Status RedisEhashes::Ehlen(const Slice& key, int32_t* ret) {
    *ret = 0;
    std::string meta_value;
    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0) {
            *ret = 0;
            return Status::NotFound();
        } else if (parsed_hashes_meta_value.IsStale()) {
            *ret = 0;
            return Status::NotFound("Stale");
        } else {
            *ret = parsed_hashes_meta_value.count();
        }
    } else if (s.IsNotFound()) {
        *ret = 0;
    }
    return s;
}

Status RedisEhashes::EhlenForce(const Slice& key, int32_t* ret) {
    rocksdb::WriteBatch batch;
    ScopeRecordLock l(lock_mgr_, key);

    *ret = 0;
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;

    std::string meta_value;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0) {
            *ret = 0;
            return Status::NotFound();
        } else if (parsed_hashes_meta_value.IsStale()) {
            *ret = 0;
            return Status::NotFound("Stale");
        } else {
            int32_t count = 0;
            int32_t version = parsed_hashes_meta_value.version();
            HashesDataKey hashes_data_key(key, version, "");
            Slice prefix = hashes_data_key.Encode();
            auto iter = db_->NewIterator(read_options, handles_[1]);
            for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
                ParsedEhashesValue parsed_ehashes_value(iter->value());
                if (parsed_ehashes_value.IsStale()) {
                    batch.Delete(handles_[1], iter->key());
                } else {
                    count++;
                }
            }
            delete iter;
            *ret = count;

            parsed_hashes_meta_value.set_count(count);
            batch.Put(handles_[0], key, meta_value);
            db_->Write(default_write_options_, &batch);
        }
    } else if (s.IsNotFound()) {
        *ret = 0;
    }
    return s;
}


Status RedisEhashes::Ehstrlen(const Slice& key, const Slice& field, int32_t* len) {
    std::string value;
    Status s = Ehget(key, field, &value);
    if (s.ok()) {
        *len = value.size();
    } else {
        *len = 0;
    }
    return s;
}

Status RedisEhashes::Ehincrby(const Slice& key, const Slice& field, int64_t value, int64_t* ret) {
    *ret = 0;
    rocksdb::WriteBatch batch;
    ScopeRecordLock l(lock_mgr_, key);

    int32_t version = 0;
    std::string old_value;
    std::string meta_value;

    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            version = parsed_hashes_meta_value.UpdateVersion();
            parsed_hashes_meta_value.set_count(1);
            parsed_hashes_meta_value.set_timestamp(0);
            batch.Put(handles_[0], key, meta_value);
            HashesDataKey hashes_data_key(key, version, field);
            char buf[32];
            Int64ToStr(buf, 32, value);
            Slice data_value(buf);
            EhashesValue ehashes_value(data_value);
            batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
            *ret = value;
        } else {
            version = parsed_hashes_meta_value.version();
            HashesDataKey hashes_data_key(key, version, field);
            s = db_->Get(default_read_options_, handles_[1], hashes_data_key.Encode(), &old_value);
            if (s.ok()) {
                ParsedEhashesValue parsed_ehashes_value(&old_value);
                if (parsed_ehashes_value.IsStale()) {
                    char buf[32];
                    Int64ToStr(buf, 32, value);
                    Slice data_value(buf);
                    EhashesValue ehashes_value(data_value);
                    batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
                    *ret = value;
                } else {
                    int64_t timestamp = parsed_ehashes_value.timestamp();
                    parsed_ehashes_value.StripSuffix();
                    int64_t ival = 0;
                    if (!StrToInt64(old_value.data(), old_value.size(), &ival)) {
                        return Status::Corruption("hash value is not an integer");
                    }
                    if ((value >= 0 && LLONG_MAX - value < ival) || 
                        (value < 0 && LLONG_MIN - value > ival)) {
                        return Status::InvalidArgument("Overflow");
                    }
                    *ret = ival + value;
                    char buf[32];
                    Int64ToStr(buf, 32, *ret);
                    Slice data_value(buf);
                    EhashesValue ehashes_value(data_value);
                    ehashes_value.set_timestamp(timestamp);
                    batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
                }
            } else if (s.IsNotFound()) {
                char buf[32];
                Int64ToStr(buf, 32, value);
                parsed_hashes_meta_value.ModifyCount(1);
                batch.Put(handles_[0], key, meta_value);
                Slice data_value(buf);
                EhashesValue ehashes_value(data_value);
                batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
                *ret = value;
            } else {
                return s;
            }
        }
    } else if (s.IsNotFound()) {
        char str[4];
        EncodeFixed32(str, 1);
        HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
        version = hashes_meta_value.UpdateVersion();
        batch.Put(handles_[0], key, hashes_meta_value.Encode());
        HashesDataKey hashes_data_key(key, version, field);

        char buf[32];
        Int64ToStr(buf, 32, value);
        Slice data_value(buf);
        EhashesValue ehashes_value(data_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
        *ret = value;
    } else {
        return s;
    }
    return db_->Write(default_write_options_, &batch);
}

Status RedisEhashes::Ehincrbyfloat(const Slice& key, const Slice& field,
                                   const Slice& by, std::string* new_value) {
    new_value->clear();
    rocksdb::WriteBatch batch;
    ScopeRecordLock l(lock_mgr_, key);

    int32_t version = 0;
    std::string meta_value;
    std::string old_value_str;
    long double long_double_by;

    if (StrToLongDouble(by.data(), by.size(), &long_double_by) == -1) {
        return Status::Corruption("value is not a vaild float");
    }

    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            version = parsed_hashes_meta_value.UpdateVersion();
            parsed_hashes_meta_value.set_count(1);
            parsed_hashes_meta_value.set_timestamp(0);
            batch.Put(handles_[0], key, meta_value);
            HashesDataKey hashes_data_key(key, version, field);

            LongDoubleToStr(long_double_by, new_value);
            EhashesValue ehashes_value(*new_value);
            batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
        } else {
            version = parsed_hashes_meta_value.version();
            HashesDataKey hashes_data_key(key, version, field);
            s = db_->Get(default_read_options_, handles_[1], hashes_data_key.Encode(), &old_value_str);
            if (s.ok()) {
                ParsedEhashesValue parsed_ehashes_value(&old_value_str);
                if (parsed_ehashes_value.IsStale()) {
                    LongDoubleToStr(long_double_by, new_value);
                    EhashesValue ehashes_value(*new_value);
                    batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
                } else {
                    int64_t timestamp = parsed_ehashes_value.timestamp();
                    parsed_ehashes_value.StripSuffix();
                    long double total;
                    long double old_value;
                    if (StrToLongDouble(old_value_str.data(), old_value_str.size(), &old_value) == -1) {
                        return Status::Corruption("value is not a vaild float");
                    }

                    total = old_value + long_double_by;
                    if (LongDoubleToStr(total, new_value) == -1) {
                        return Status::InvalidArgument("Overflow");
                    }
                    EhashesValue ehashes_value(*new_value);
                    ehashes_value.set_timestamp(timestamp);
                    batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
                }
            } else if (s.IsNotFound()) {
                LongDoubleToStr(long_double_by, new_value);
                parsed_hashes_meta_value.ModifyCount(1);
                batch.Put(handles_[0], key, meta_value);
                EhashesValue ehashes_value(*new_value);
                batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
            } else {
                return s;
            }
        }
    } else if (s.IsNotFound()) {
        char str[4];
        EncodeFixed32(str, 1);
        HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
        version = hashes_meta_value.UpdateVersion();
        batch.Put(handles_[0], key, hashes_meta_value.Encode());

        HashesDataKey hashes_data_key(key, version, field);
        LongDoubleToStr(long_double_by, new_value);
        EhashesValue ehashes_value(*new_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
    } else {
        return s;
    }
    return db_->Write(default_write_options_, &batch);
}

Status RedisEhashes::Ehmset(const Slice& key, const std::vector<FieldValue>& fvs) {
    std::unordered_set<std::string> fields;
    std::vector<FieldValue> filtered_fvs;
    for (auto iter = fvs.rbegin(); iter != fvs.rend(); ++iter) {
        std::string field = iter->field;
        if (fields.find(field) == fields.end()) {
            fields.insert(field);
            filtered_fvs.push_back(*iter);
        }
    }

    rocksdb::WriteBatch batch;
    ScopeRecordLock l(lock_mgr_, key);

    int32_t version = 0;
    std::string meta_value;
    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            version = parsed_hashes_meta_value.UpdateVersion();
            parsed_hashes_meta_value.set_count(filtered_fvs.size());
            parsed_hashes_meta_value.set_timestamp(0);
            batch.Put(handles_[0], key, meta_value);
            for (const auto& fv : filtered_fvs) {
                HashesDataKey hashes_data_key(key, version, fv.field);
                EhashesValue ehashes_value(fv.value);
                batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
            }
        } else {
            int32_t count = 0;
            std::string data_value;
            version = parsed_hashes_meta_value.version();
            for (const auto& fv : filtered_fvs) {
                HashesDataKey hashes_data_key(key, version, fv.field);
                s = db_->Get(default_read_options_, handles_[1], hashes_data_key.Encode(), &data_value);
                if (s.ok()) {
                    EhashesValue ehashes_value(fv.value);
                    batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
                } else if (s.IsNotFound()) {
                    count++;
                    EhashesValue ehashes_value(fv.value);
                    batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
                } else {
                    return s;
                }
            }
            parsed_hashes_meta_value.ModifyCount(count);
            batch.Put(handles_[0], key, meta_value);
        }
    } else if (s.IsNotFound()) {
        char str[4];
        EncodeFixed32(str, filtered_fvs.size());
        HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
        version = hashes_meta_value.UpdateVersion();
        batch.Put(handles_[0], key, hashes_meta_value.Encode());
        for (const auto& fv : filtered_fvs) {
            HashesDataKey hashes_data_key(key, version, fv.field);
            EhashesValue ehashes_value(fv.value);
            batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
        }
    }
    return db_->Write(default_write_options_, &batch);
}

Status RedisEhashes::Ehmsetex(const Slice& key, const std::vector<FieldValueTTL>& fvts) {
    std::unordered_set<std::string> fields;
    std::vector<FieldValueTTL> filtered_fvs;
    for (auto iter = fvts.rbegin(); iter != fvts.rend(); ++iter) {
        std::string field = iter->field;
        if (fields.find(field) == fields.end()) {
            fields.insert(field);
            filtered_fvs.push_back(*iter);
        }
    }

    rocksdb::WriteBatch batch;
    ScopeRecordLock l(lock_mgr_, key);

    int32_t version = 0;
    std::string meta_value;
    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            version = parsed_hashes_meta_value.UpdateVersion();
            parsed_hashes_meta_value.set_count(filtered_fvs.size());
            parsed_hashes_meta_value.set_timestamp(0);
            batch.Put(handles_[0], key, meta_value);
            for (const auto& fv : filtered_fvs) {
                HashesDataKey hashes_data_key(key, version, fv.field);
                EhashesValue ehashes_value(fv.value);
                if (fv.ttl > 0) {
                    ehashes_value.SetRelativeTimestamp(fv.ttl);
                }
                batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
            }
        } else {
            int32_t count = 0;
            std::string data_value;
            version = parsed_hashes_meta_value.version();
            for (const auto& fv : filtered_fvs) {
                HashesDataKey hashes_data_key(key, version, fv.field);
                s = db_->Get(default_read_options_, handles_[1], hashes_data_key.Encode(), &data_value);
                if (s.ok()) {
                    EhashesValue ehashes_value(fv.value);
                    if (fv.ttl > 0) {
                        ehashes_value.SetRelativeTimestamp(fv.ttl);
                    }
                    batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
                } else if (s.IsNotFound()) {
                    count++;
                    EhashesValue ehashes_value(fv.value);
                    if (fv.ttl > 0) {
                        ehashes_value.SetRelativeTimestamp(fv.ttl);
                    }
                    batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
                } else {
                    return s;
                }
            }
            parsed_hashes_meta_value.ModifyCount(count);
            batch.Put(handles_[0], key, meta_value);
        }
    } else if (s.IsNotFound()) {
        char str[4];
        EncodeFixed32(str, filtered_fvs.size());
        HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
        version = hashes_meta_value.UpdateVersion();
        batch.Put(handles_[0], key, hashes_meta_value.Encode());
        for (const auto& fv : filtered_fvs) {
            HashesDataKey hashes_data_key(key, version, fv.field);
            EhashesValue ehashes_value(fv.value);
            if (fv.ttl > 0) {
                ehashes_value.SetRelativeTimestamp(fv.ttl);
            }
            batch.Put(handles_[1], hashes_data_key.Encode(), ehashes_value.Encode());
        }
    }
    return db_->Write(default_write_options_, &batch);
}

Status RedisEhashes::Ehmget(const Slice& key,
                            const std::vector<std::string>& fields,
                            std::vector<ValueStatus>* vss) {
    vss->clear();

    int32_t version = 0;
    std::string value;
    std::string meta_value;
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);

    if (s.ok()) {
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            s = Status::NotFound();
        }
    } 

    if (s.ok()) {
        version = parsed_hashes_meta_value.version();
        for (const auto& field : fields) {
            HashesDataKey hashes_data_key(key, version, field);
            s = db_->Get(read_options, handles_[1], hashes_data_key.Encode(), &value);
            if (s.ok()) {
                ParsedEhashesValue parsed_ehashes_value(&value);
                if (parsed_ehashes_value.IsStale()) {
                    vss->push_back({std::string(), Status::NotFound()});
                } else {
                    parsed_ehashes_value.StripSuffix();
                    vss->push_back({value, Status::OK()});
                }
            } else if (s.IsNotFound()) {
                vss->push_back({std::string(), Status::NotFound()});
            } else {
                vss->clear();
                return s;
            }
        }
    } else if (s.IsNotFound()) {
        for (size_t idx = 0; idx < fields.size(); ++idx) {
            vss->push_back({std::string(), Status::NotFound()});
        }
    }

    return s;
}

Status RedisEhashes::Ehkeys(const Slice& key, std::vector<std::string>* fields) {
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;

    std::string meta_value;
    int32_t version = 0;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0) {
            return Status::NotFound();
        } else if (parsed_hashes_meta_value.IsStale()) {
            return Status::NotFound("Stale");
        } else {
            std::string value;
            version = parsed_hashes_meta_value.version();
            HashesDataKey hashes_data_key(key, version, "");
            Slice prefix = hashes_data_key.Encode();
            auto iter = db_->NewIterator(read_options, handles_[1]);
            for (iter->Seek(prefix);
                 iter->Valid() && iter->key().starts_with(prefix);
                 iter->Next()) {

                ParsedEhashesValue parsed_ehashes_value(iter->value());
                if (!parsed_ehashes_value.IsStale()) {
                    ParsedHashesDataKey parsed_hashes_data_key(iter->key());
                    fields->push_back(parsed_hashes_data_key.field().ToString());
                }
            }
            delete iter;
        }
    }
    return s;
}

Status RedisEhashes::Ehvals(const Slice& key, std::vector<std::string>* values) {
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;

    std::string meta_value;
    int32_t version = 0;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0) {
            return Status::NotFound();
        } else if (parsed_hashes_meta_value.IsStale()) {
            return Status::NotFound("Stale");
        } else {
            std::string value;
            version = parsed_hashes_meta_value.version();
            HashesDataKey hashes_data_key(key, version, "");
            Slice prefix = hashes_data_key.Encode();
            auto iter = db_->NewIterator(read_options, handles_[1]);
            for (iter->Seek(prefix); 
                 iter->Valid() && iter->key().starts_with(prefix);
                 iter->Next()) {

                ParsedEhashesValue parsed_ehashes_value(iter->value());
                if (!parsed_ehashes_value.IsStale()) {
                    values->push_back(parsed_ehashes_value.value().ToString());
                }
            }
            delete iter;
        }
    }
    return s;
}

Status RedisEhashes::Ehgetall(const Slice& key, std::vector<FieldValueTTL>* fvts) {
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;

    std::string meta_value;
    int32_t version = 0;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0) {
            return Status::NotFound();
        } else if (parsed_hashes_meta_value.IsStale()) {
            return Status::NotFound("Stale");
        } else {
            std::string value;
            version = parsed_hashes_meta_value.version();
            HashesDataKey hashes_data_key(key, version, "");
            Slice prefix = hashes_data_key.Encode();
            auto iter = db_->NewIterator(read_options, handles_[1]);
            for (iter->Seek(prefix); 
                 iter->Valid() && iter->key().starts_with(prefix);
                 iter->Next()) {

                ParsedEhashesValue parsed_ehashes_value(iter->value());
                if (!parsed_ehashes_value.IsStale()) {
                    int64_t ttl;
                    int64_t timestamp = parsed_ehashes_value.timestamp();
                    if (timestamp == 0) {
                        ttl = -1;
                    } else {
                        int64_t curtime;
                        rocksdb::Env::Default()->GetCurrentTime(&curtime);
                        ttl = (timestamp - curtime >= 0) ? timestamp - curtime : -2;
                    }

                    ParsedHashesDataKey parsed_hashes_data_key(iter->key());
                    fvts->push_back({parsed_hashes_data_key.field().ToString(),
                        parsed_ehashes_value.value().ToString(), static_cast<int32_t>(ttl)});
                }
            }
            delete iter;
        }
    }
    return s;
}

Status RedisEhashes::Ehscan(const Slice& key, int64_t cursor,
                            const std::string& pattern, int64_t count,
                            std::vector<FieldValueTTL>* fvts, int64_t* next_cursor) {
    *next_cursor = 0;
    fvts->clear();
    if (cursor < 0) {
        *next_cursor = 0;
        return Status::OK();
    }

    int64_t rest = count;
    int64_t step_length = count;
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;

    std::string meta_value;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            *next_cursor = 0;
            return Status::NotFound();
        } else {
            std::string sub_field;
            std::string start_point;
            int32_t version = parsed_hashes_meta_value.version();
            s = GetScanStartPoint(key, pattern, cursor, &start_point);
            if (s.IsNotFound()) {
                cursor = 0;
                if (isTailWildcard(pattern)) {
                    start_point = pattern.substr(0, pattern.size() - 1);
                }
            }
            if (isTailWildcard(pattern)) {
                sub_field = pattern.substr(0, pattern.size() - 1);
            }

            HashesDataKey hashes_data_prefix(key, version, sub_field);
            HashesDataKey hashes_start_data_key(key, version, start_point);
            std::string prefix = hashes_data_prefix.Encode().ToString();
            rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
            for (iter->Seek(hashes_start_data_key.Encode()); 
                 iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
                 iter->Next()) {

                ParsedHashesDataKey parsed_hashes_data_key(iter->key());
                std::string field = parsed_hashes_data_key.field().ToString();
                if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0)) {
                    ParsedEhashesValue parsed_ehashes_value(iter->value());
                    if (!parsed_ehashes_value.IsStale()) {
                        int64_t ttl;
                        int64_t timestamp = parsed_ehashes_value.timestamp();
                        if (timestamp == 0) {
                            ttl = -1;
                        } else {
                            int64_t curtime;
                            rocksdb::Env::Default()->GetCurrentTime(&curtime);
                            ttl = (timestamp - curtime >= 0) ? timestamp - curtime : -2;
                        }

                        fvts->push_back({field, parsed_ehashes_value.value().ToString(), static_cast<int32_t>(ttl)});
                        rest--;
                    }
                }
                
            }

            if (iter->Valid() && (iter->key().compare(prefix) <= 0 || iter->key().starts_with(prefix))) {
                *next_cursor = cursor + step_length;
                ParsedHashesDataKey parsed_hashes_data_key(iter->key());
                std::string next_field = parsed_hashes_data_key.field().ToString();
                StoreScanNextPoint(key, pattern, *next_cursor, next_field);
            } else {
                *next_cursor = 0;
            }
            delete iter;
        }
    } else {
        *next_cursor = 0;
        return s;
    }
    return Status::OK();
}

Status RedisEhashes::Ehscanx(const Slice& key, const std::string start_field,
                             const std::string& pattern, int64_t count,
                             std::vector<FieldValueTTL>* fvts,
                             std::string* next_field) {
    next_field->clear();
    fvts->clear();

    int64_t rest = count;
    std::string meta_value;
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;
    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 || parsed_hashes_meta_value.IsStale()) {
            *next_field = "";
            return Status::NotFound();
        } else {
            int32_t version = parsed_hashes_meta_value.version();
            HashesDataKey hashes_data_prefix(key, version, Slice());
            HashesDataKey hashes_start_data_key(key, version, start_field);
            std::string prefix = hashes_data_prefix.Encode().ToString();
            rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
            for (iter->Seek(hashes_start_data_key.Encode()); 
                 iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
                 iter->Next()) {

                ParsedHashesDataKey parsed_hashes_data_key(iter->key());
                std::string field = parsed_hashes_data_key.field().ToString();
                if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0)) {
                    ParsedEhashesValue parsed_ehashes_value(iter->value());
                    if (!parsed_ehashes_value.IsStale()) {
                        int64_t ttl;
                        int64_t timestamp = parsed_ehashes_value.timestamp();
                        if (timestamp == 0) {
                            ttl = -1;
                        } else {
                            int64_t curtime;
                            rocksdb::Env::Default()->GetCurrentTime(&curtime);
                            ttl = (timestamp - curtime >= 0) ? timestamp - curtime : -2;
                        }

                        fvts->push_back({field, parsed_ehashes_value.value().ToString(), static_cast<int32_t>(ttl)});
                        rest--;
                    }
                }
            }

            if (iter->Valid() && iter->key().starts_with(prefix)) {
                ParsedHashesDataKey parsed_hashes_data_key(iter->key());
                *next_field = parsed_hashes_data_key.field().ToString();
            } else {
                *next_field = "";
            }
            delete iter;
        }
    } else {
        *next_field = "";
        return s;
    }
    return Status::OK();
}

Status RedisEhashes::PKEHScanRange(const Slice& key,
                                  const Slice& field_start,
                                  const std::string& field_end,
                                  const Slice& pattern, int32_t limit,
                                  std::vector<FieldValue>* field_values,
                                  std::string* next_field) {
    next_field->clear();
    field_values->clear();

    int64_t remain = limit;
    std::string meta_value;
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;

    bool start_no_limit = !field_start.compare("");
    bool end_no_limit = !field_end.compare("");

    if (!start_no_limit 
        && !end_no_limit
        && (field_start.compare(field_end) > 0)) {
        return Status::InvalidArgument("error in given range");
    }

    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0
            || parsed_hashes_meta_value.IsStale()) {
            return Status::NotFound();
        } else {
            int32_t version = parsed_hashes_meta_value.version();
            HashesDataKey hashes_data_prefix(key, version, Slice());
            HashesDataKey hashes_start_data_key(key, version, field_start);
            std::string prefix = hashes_data_prefix.Encode().ToString();
            rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
            for (iter->Seek(start_no_limit ? prefix : hashes_start_data_key.Encode());
                iter->Valid() && remain > 0 && iter->key().starts_with(prefix);
                iter->Next()) {

                ParsedHashesDataKey parsed_hashes_data_key(iter->key());
                std::string field = parsed_hashes_data_key.field().ToString();
                if (!end_no_limit && field.compare(field_end) > 0) {
                    break;
                }
                if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0)) {
                    ParsedEhashesValue parsed_ehashes_value(iter->value());
                    if (!parsed_ehashes_value.IsStale()) {
                        field_values->push_back({field, parsed_ehashes_value.value().ToString()});
                        remain--;
                    }
                }
            }

            if (iter->Valid() && iter->key().starts_with(prefix)) {
                ParsedHashesDataKey parsed_hashes_data_key(iter->key());
                if (end_no_limit || parsed_hashes_data_key.field().compare(field_end) <= 0) {
                    *next_field = parsed_hashes_data_key.field().ToString();
                }
            }
            delete iter;
        }
    } else {
        return s;
    }
    return Status::OK();
}

Status RedisEhashes::PKEHRScanRange(const Slice& key,
                                   const Slice& field_start, const std::string& field_end,
                                   const Slice& pattern, int32_t limit,
                                   std::vector<FieldValue>* field_values,
                                   std::string* next_field) {
    next_field->clear();
    field_values->clear();

    int64_t remain = limit;
    std::string meta_value;
    rocksdb::ReadOptions read_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    read_options.snapshot = snapshot;

    bool start_no_limit = !field_start.compare("");
    bool end_no_limit = !field_end.compare("");

    if (!start_no_limit 
        && !end_no_limit
        && (field_start.compare(field_end) < 0)) {
        return Status::InvalidArgument("error in given range");
    }

    Status s = db_->Get(read_options, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0 
            || parsed_hashes_meta_value.IsStale()) {
            return Status::NotFound();
        } else {
            int32_t version = parsed_hashes_meta_value.version();
            int32_t start_key_version = start_no_limit ? version + 1 : version;
            std::string start_key_field = start_no_limit ? "" : field_start.ToString();
            HashesDataKey hashes_data_prefix(key, version, Slice());
            HashesDataKey hashes_start_data_key(key, start_key_version, start_key_field);
            std::string prefix = hashes_data_prefix.Encode().ToString();
            rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
            for (iter->SeekForPrev(hashes_start_data_key.Encode().ToString()); 
                iter->Valid() && remain > 0 && iter->key().starts_with(prefix);
                iter->Prev()) {
                ParsedHashesDataKey parsed_hashes_data_key(iter->key());
                std::string field = parsed_hashes_data_key.field().ToString();
                if (!end_no_limit && field.compare(field_end) < 0) {
                    break;
                }
                if (StringMatch(pattern.data(), pattern.size(), field.data(), field.size(), 0)) {
                    ParsedEhashesValue parsed_ehashes_value(iter->value());
                    if (!parsed_ehashes_value.IsStale()) {
                        field_values->push_back({field, parsed_ehashes_value.value().ToString()});
                        remain--;
                    }
                }
            }

            if (iter->Valid() && iter->key().starts_with(prefix)) {
                ParsedHashesDataKey parsed_hashes_data_key(iter->key());
                if (end_no_limit || parsed_hashes_data_key.field().compare(field_end) >= 0) {
                    *next_field = parsed_hashes_data_key.field().ToString();
                }
            }
            delete iter;
        }
    } else {
        return s;
    }
    return Status::OK();
}

Status RedisEhashes::PKScanRange(const Slice& key_start, const Slice& key_end,
                                 const Slice& pattern, int32_t limit,
                                 std::vector<std::string>* keys, std::string* next_key) {
    std::string key;
    int32_t remain = limit;
    rocksdb::ReadOptions iterator_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    iterator_options.snapshot = snapshot;
    iterator_options.fill_cache = false;

    bool start_no_limit = !key_start.compare("");
    bool end_no_limit = !key_end.compare("");

    if (!start_no_limit 
        && !end_no_limit
        && (key_start.compare(key_end) > 0)) {
        return Status::InvalidArgument("error in given range");
    }

    rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);
    if (start_no_limit) {
        it->SeekToFirst();
    } else {
        it->Seek(key_start);
    }

    while (it->Valid() && remain > 0 && (end_no_limit || it->key().compare(key_end) <= 0)) {
        ParsedHashesMetaValue parsed_hashes_meta_value(it->value());
        if (parsed_hashes_meta_value.count() == 0 
            || parsed_hashes_meta_value.IsStale()) {
            it->Next();
        } else {
            key = it->key().ToString();
            if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
                keys->push_back(key);
            }
            remain--;
            it->Next();
        }
    }

    if (it->Valid() && (end_no_limit || it->key().compare(key_end) <= 0)) {
        *next_key = it->key().ToString();
    } else {
        *next_key = "";
    }
    delete it;
    return Status::OK();
}

Status RedisEhashes::PKRScanRange(const Slice& key_start, const Slice& key_end,
                                  const Slice& pattern, int32_t limit,
                                  std::vector<std::string>* keys, std::string* next_key) {
    std::string key;
    int32_t remain = limit;
    rocksdb::ReadOptions iterator_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    iterator_options.snapshot = snapshot;
    iterator_options.fill_cache = false;

    bool start_no_limit = !key_start.compare("");
    bool end_no_limit = !key_end.compare("");

    if (!start_no_limit 
        && !end_no_limit
        && (key_start.compare(key_end) < 0)) {
        return Status::InvalidArgument("error in given range");
    }

    rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);
    if (start_no_limit) {
        it->SeekToLast();
    } else {
        it->SeekForPrev(key_start);
    }

    while (it->Valid() && remain > 0 && (end_no_limit || it->key().compare(key_end) >= 0)) {
        ParsedHashesMetaValue parsed_hashes_meta_value(it->value());
        if (parsed_hashes_meta_value.count() == 0 
            || parsed_hashes_meta_value.IsStale()) {
            it->Prev();
        } else {
            key = it->key().ToString();
            if (StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
                keys->push_back(key);
            }
            remain--;
            it->Prev();
        }
    }

    if (it->Valid() && (end_no_limit || it->key().compare(key_end) >= 0)) {
        *next_key = it->key().ToString();
    } else {
        *next_key = "";
    }
    delete it;
    return Status::OK();
}

Status RedisEhashes::Expire(const Slice& key, int32_t ttl) {
    std::string meta_value;
    ScopeRecordLock l(lock_mgr_, key);
    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0) {
            return Status::NotFound();
        } else if (parsed_hashes_meta_value.IsStale()) {
            return Status::NotFound("Stale");
        }
        if (ttl > 0) {
            parsed_hashes_meta_value.SetRelativeTimestamp(ttl);
            s = db_->Put(default_write_options_, handles_[0], key, meta_value);
        } else {
            parsed_hashes_meta_value.set_count(0);
            parsed_hashes_meta_value.UpdateVersion();
            parsed_hashes_meta_value.set_timestamp(0);
            s = db_->Put(default_write_options_, handles_[0], key, meta_value);
        }
    }
    return s;
}

Status RedisEhashes::Del(const Slice& key) {
    std::string meta_value;
    ScopeRecordLock l(lock_mgr_, key);
    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0) {
            return Status::NotFound();
        } else if (parsed_hashes_meta_value.IsStale()) {
            return Status::NotFound("Stale");
        } else {
            parsed_hashes_meta_value.InitialMetaValue();
            s = db_->Put(default_write_options_, handles_[0], key, meta_value);
        }
    }
    return s;
}

bool RedisEhashes::Scan(const std::string& start_key,
                        const std::string& pattern,
                        std::vector<std::string>* keys,
                        int64_t* count, std::string* next_key) {
    std::string meta_key;
    bool is_finish = true;
    rocksdb::ReadOptions iterator_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    iterator_options.snapshot = snapshot;
    iterator_options.fill_cache = false;

    rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);

    it->Seek(start_key);
    while (it->Valid() && (*count) > 0) {
        ParsedHashesMetaValue parsed_meta_value(it->value());
        if (parsed_meta_value.count() == 0 || parsed_meta_value.IsStale()) {
            it->Next();
            continue;
        } else {
            meta_key = it->key().ToString();
            if (StringMatch(pattern.data(), pattern.size(), meta_key.data(), meta_key.size(), 0)) {
                keys->push_back(meta_key);
            }
            (*count)--;
            it->Next();
        }
    }

    std::string prefix = isTailWildcard(pattern) ?
    pattern.substr(0, pattern.size() - 1) : "";
    if (it->Valid() && (it->key().compare(prefix) <= 0 || it->key().starts_with(prefix))) {
        *next_key = it->key().ToString();
        is_finish = false;
    } else {
        *next_key = "";
    }
    delete it;
    return is_finish;
}

Status RedisEhashes::Expireat(const Slice& key, int32_t timestamp) {
    std::string meta_value;
    ScopeRecordLock l(lock_mgr_, key);
    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0) {
            return Status::NotFound();
        } else if (parsed_hashes_meta_value.IsStale()) {
            return Status::NotFound("Stale");
        } else {
            if (timestamp > 0) {
                parsed_hashes_meta_value.set_timestamp(timestamp);
            } else {
                parsed_hashes_meta_value.InitialMetaValue();
            }
            s = db_->Put(default_write_options_, handles_[0], key, meta_value);
        }
    }
    return s;
}

Status RedisEhashes::Persist(const Slice& key) {
    std::string meta_value;
    ScopeRecordLock l(lock_mgr_, key);
    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0) {
            return Status::NotFound();
        } else if (parsed_hashes_meta_value.IsStale()) {
            return Status::NotFound("Stale");
        } else {
            int32_t timestamp = parsed_hashes_meta_value.timestamp();
            if (timestamp == 0) {
            return Status::NotFound("Not have an associated timeout");
        }  else {
            parsed_hashes_meta_value.set_timestamp(0);
            s = db_->Put(default_write_options_, handles_[0], key, meta_value);
        }
        }
    }
    return s;
}

Status RedisEhashes::TTL(const Slice& key, int64_t* timestamp) {
    std::string meta_value;
    Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
    if (s.ok()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
        if (parsed_hashes_meta_value.count() == 0) {
            *timestamp = -2;
            return Status::NotFound();
        } else if (parsed_hashes_meta_value.IsStale()) {
            *timestamp = -2;
            return Status::NotFound("Stale");
        } else {
            *timestamp = parsed_hashes_meta_value.timestamp();
            if (*timestamp == 0) {
                *timestamp = -1;
            } else {
                int64_t curtime;
                rocksdb::Env::Default()->GetCurrentTime(&curtime);
                *timestamp = *timestamp - curtime >= 0 ? *timestamp - curtime : -2;
            }
        }
    } else if (s.IsNotFound()) {
        *timestamp = -2;
    }
    return s;
}

void RedisEhashes::ScanDatabase() {
    rocksdb::ReadOptions iterator_options;
    const rocksdb::Snapshot* snapshot;
    ScopeSnapshot ss(db_, &snapshot);
    iterator_options.snapshot = snapshot;
    iterator_options.fill_cache = false;
    int32_t current_time = time(NULL);

    printf("\n***************Ehashes Meta Data***************\n");
    auto meta_iter = db_->NewIterator(iterator_options, handles_[0]);
    for (meta_iter->SeekToFirst(); meta_iter->Valid(); meta_iter->Next()) {
        ParsedHashesMetaValue parsed_hashes_meta_value(meta_iter->value());
        int32_t survival_time = 0;
        if (parsed_hashes_meta_value.timestamp() != 0) {
            survival_time = parsed_hashes_meta_value.timestamp() - current_time > 0 ?
            parsed_hashes_meta_value.timestamp() - current_time : -1;
        }

        printf("[key : %-30s] [count : %-10d] [timestamp : %-10d] [version : %d] [survival_time : %d]\n",
               meta_iter->key().ToString().c_str(),
               parsed_hashes_meta_value.count(),
               parsed_hashes_meta_value.timestamp(),
               parsed_hashes_meta_value.version(),
               survival_time);
    }
    delete meta_iter;

    printf("\n***************Ehashes Field Data***************\n");
    auto field_iter = db_->NewIterator(iterator_options, handles_[1]);
    for (field_iter->SeekToFirst(); field_iter->Valid(); field_iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(field_iter->key());
        printf("[key : %-30s] [field : %-20s] [value : %-20s] [version : %d]\n",
               parsed_hashes_data_key.key().ToString().c_str(),
               parsed_hashes_data_key.field().ToString().c_str(),
               field_iter->value().ToString().c_str(),
               parsed_hashes_data_key.version());
    }
    delete field_iter;
}

} //  namespace blackwidow
