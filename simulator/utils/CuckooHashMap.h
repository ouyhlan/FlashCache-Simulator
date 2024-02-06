#pragma once

// Concurrent Cuckoo Hash for SSD FTL Layer
// based on https://github.com/efficient/libcuckoo

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "candidate.hpp"
#include "utils/CuckooHashBucket.h"
#include "utils/CuckooHashConfig.h"

constexpr size_t constPow(size_t a, size_t b) {
  return (b == 0) ? 1 : a * constPow(a, b - 1);
}

// Fixed Size since no need for expanison
// HashMap store ({key, partial}, {BloomFilter, storage_offset})
// partial is an 1 byte chained timestamp
// key is allow to be repeated and its fingerprint is a BloomFilter
class CuckooHashMap {
  using PowerType = uint8_t;  // type to be power e.g. PowerType q; -> 2^q
  using SizeType = uint32_t;  // max size lower than 2^32
  using KeyType = uint64_t;
  using ValueType = uint64_t;
  using Page = std::vector<candidate_t>;
  using Partial = uint8_t;
  using Bucket = CuckooHashBucket<ValueType, Partial>;
  using Timestamp = CuckooHashTimestamp<Partial>;
  using Lock = SpinLock;
  using LocksArray = std::vector<Lock>;

 public:
  // q for table size 2^q
  CuckooHashMap(PowerType q);

  bool insert(const SizeType &key, const ValueType &val, Page &&page,
              Page &replaced_page);
  bool find(const KeyType &key, const candidate_t &item, ValueType &val);
  // bool erase(const KeyType &key, const ValueType &val);

  SizeType size() const;
  double loadfactor() const;

 private:
  const uint64_t indexMask_;
  const uint64_t BucketSizes_;
  std::vector<Bucket> buckets_;
  std::vector<Timestamp> timestamps_;
  mutable LocksArray locks_;

  struct Set {
    SizeType id;
    Partial partial;
  };

  struct CuckooRecord {
    SizeType bucketId;
    SizeType slot;
    Set set;
    bool toDelete{false};
  };

  using CuckooRecordsArray = std::array<CuckooRecord, kMaxBFSPathLen>;

  // Hash function -- MurmurHash2
  // partial should be nonzero
  SizeType altIndex(const SizeType index, Partial partial) const {
    // ensure tag is nonzero for the multiply. 0xc6a4a7935bd1e995 is the
    // hash constant from 64-bit MurmurHash2
    const uint64_t nonzeroPartial = static_cast<SizeType>(partial);
    return static_cast<SizeType>(
        (index ^ (nonzeroPartial * 0xc6a4a7935bd1e995)) & indexMask_);
  }

  // expects the locks to be held outside the funciton
  Set getSetFromBucket(const Bucket &b, const SizeType bucketId,
                       const SizeType slot) {
    assert(b.isOccupied(slot));
    Partial partial = b.partial(slot);
    return {b.isInplace(slot) ? bucketId : altIndex(bucketId, partial),
            partial};
  }

  static SizeType lockIndex(const SizeType bucketId) {
    return bucketId & (kMaxNumLocks - 1);
  }

  SizeType setIndex(const KeyType key) {
    return static_cast<SizeType>(key & indexMask_);
  }

  struct LockDeleter {
    void operator()(SpinLock *l) const { l->unlock(); }
  };
  using LockManager = std::unique_ptr<SpinLock, LockDeleter>;

  // Give two buckets interface
  class TwoBuckets {
   public:
    TwoBuckets() {}

    TwoBuckets(LocksArray &locks, SizeType i1, SizeType i2)
        : i1(i1),
          i2(i2),
          firstManager_(&locks[lockIndex(i1)]),
          secondManager_((lockIndex(i1) != lockIndex(i2)) ? &locks[i2]
                                                          : nullptr) {}

    void unlock() {
      firstManager_.reset();
      secondManager_.reset();
    }

    SizeType i1, i2;

   private:
    LockManager firstManager_, secondManager_;
  };

  // Lock related function
  LockManager lockOne(SizeType i) const;
  TwoBuckets lockTwo(SizeType i1, SizeType i2) const;
  TwoBuckets lockTwoForOneSet(const Set &set) const;
  std::pair<TwoBuckets, LockManager> lockThree(SizeType i1, SizeType i2,
                                               SizeType i3) const;

  // Insert related data structure
  enum CuckooStatus { ok, failure, failureKeyNotFound, failureBucketFull };

  struct TablePosition {
    SizeType index;
    SizeType slot;
    CuckooStatus status;
  };

  // cuckoo BFS search

  static constexpr SizeType s = constPow(1, 2);

  struct SlotInfo {
    SizeType bucketId;
    uint16_t pathcode;
    uint8_t depth;
    bool isOldest;

    SlotInfo() {}
    SlotInfo(const SizeType b, const uint64_t p, const uint8_t d,
             const bool t = false)
        : bucketId(b), pathcode(p), depth(d), isOldest(t) {
      assert(d < kMaxBFSPathLen);
    }
  };

  class BQueue {
   public:
    BQueue() noexcept : first_(0), last_(0) {}

    void enqueue(SlotInfo x) {
      assert(!isFull());
      slots_[last_++] = x;
    }

    SlotInfo dequeue() {
      assert(!isEmpty());
      assert(first_ < last_);
      SlotInfo &x = slots_[first_++];

      return x;
    }

    bool isEmpty() const { return first_ == last_; }

    bool isFull() const { return last_ == maxCuckooCount; }

    static constexpr SizeType maxCuckooCount =
        2 * ((kDefaultSlotPerBucket == 1)
                 ? kMaxBFSPathLen
                 : (constPow(kDefaultSlotPerBucket, kMaxBFSPathLen) - 1) /
                       (kDefaultSlotPerBucket - 1));

   private:
    SlotInfo slots_[maxCuckooCount];
    SizeType first_;
    SizeType last_;
  };

  // utils function
  TablePosition cuckooInsert(TwoBuckets &b, const Set &set,
                             Page &replaced_page);
  TablePosition cuckooFind(const candidate_t &item, const Set &set,
                           const SizeType i1, const SizeType i2);

  // runCuckoo related function
  CuckooStatus runCuckoo(TwoBuckets &b, SizeType &insertBucket,
                         SizeType &insertSlot, Page &replaced_page);
  int cuckooPathSearch(CuckooRecordsArray &cuckooPath, const SizeType i1,
                       const SizeType i2);
  bool cuckooPathMove(CuckooRecordsArray &cuckooPath, SizeType depth,
                      TwoBuckets &b, Page &replaced_page);
  SlotInfo slotSearch(const SizeType i1, const SizeType i2);

  // inplace means the key store in the buckets_[key]
  bool tryFindInsertBucket(const Bucket &b, int &slot, const Set &set,
                           bool inplace) const;
  bool tryReadFromBucket(const candidate_t &item, Bucket &b, int &slot,
                         const Set &set, bool inplace) const;

  // Deletion functions
  // bool cuckooErase(const Set &hk);
  void deleteHashKeyFromBucket(const Set &set, const SizeType bucketId,
                               const SizeType slot);
  void addHashKeyToBucket(const Set &set, const ValueType &value, Page &&page,
                          const SizeType bucketId, const SizeType slot);
};
