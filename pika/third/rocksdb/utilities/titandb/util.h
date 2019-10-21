#pragma once

#include "rocksdb/cache.h"
#include "util/compression.h"
#include "util/coding.h"

namespace rocksdb {
namespace titandb {

// ttl length
const size_t kStringsValueSuffixLength = sizeof(int32_t);

#define TRY(expr)          \
  do {                     \
    auto s = (expr);       \
    if (!s.ok()) return s; \
  } while (0)

#define EXPECT(expr)      \
  do {                    \
    if (!(expr)) abort(); \
  } while (0)

// A slice pointed to an owned buffer.
class OwnedSlice : public Slice {
 public:
  void reset(std::unique_ptr<char[]> _data, size_t _size) {
    data_ = _data.get();
    size_ = _size;
    buffer_ = std::move(_data);
  }

  void reset(std::unique_ptr<char[]> buffer, const Slice& s) {
    data_ = s.data();
    size_ = s.size();
    buffer_ = std::move(buffer);
  }

  char* release() {
    data_ = nullptr;
    size_ = 0;
    return buffer_.release();
  }

  static void CleanupFunc(void* buffer, void*) {
    delete[] reinterpret_cast<char*>(buffer);
  }

 private:
  std::unique_ptr<char[]> buffer_;
};

// A slice pointed to a fixed size buffer.
template <size_t T>
class FixedSlice : public Slice {
 public:
  FixedSlice() : Slice(buffer_, T) {}

  char* get() { return buffer_; }

 private:
  char buffer_[T];
};

// Compresses the input data according to the compression context.
// Returns a slice with the output data and sets "*type" to the output
// compression type.
//
// If compression is actually performed, fills "*output" with the
// compressed data. However, if the compression ratio is not good, it
// returns the input slice directly and sets "*type" to
// kNoCompression.
Slice Compress(const CompressionContext& ctx, const Slice& input,
               std::string* output, CompressionType* type);

// Uncompresses the input data according to the uncompression type.
// If successful, fills "*buffer" with the uncompressed data and
// points "*output" to it.
Status Uncompress(const UncompressionContext& ctx, const Slice& input,
                  OwnedSlice* output);

void UnrefCacheHandle(void* cache, void* handle);

template <class T>
void DeleteCacheValue(const Slice&, void* value) {
  delete reinterpret_cast<T*>(value);
}

int32_t GetTimeStampFromValue(const Slice& value);

bool IsBlobKeyStale(int32_t timestamp);


}  // namespace titandb
}  // namespace rocksdb
