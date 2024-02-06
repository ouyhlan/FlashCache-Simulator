#include "utils/CuckooHashMap.h"

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <type_traits>
#include <unordered_set>
#include <utility>

#include "candidate.hpp"
#include "constants.hpp"
#include "log_simple.hpp"
#include "utils/CuckooHashBucket.h"
#include "utils/CuckooHashConfig.h"

#define LOW_MASK(n) ((1ull << (n)) - 1ull)

CuckooHashMap::CuckooHashMap(PowerType q)
    : indexMask_(LOW_MASK(q)), BucketSizes_(1ull << (uint64_t)q) {
  buckets_.resize(BucketSizes_);
  timestamps_.resize(BucketSizes_);
  locks_.resize(std::min(uint64_t(kMaxNumLocks), BucketSizes_));

  // Random Initialize the timestamp of each bucket
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<Partial> dis(
      0, std::numeric_limits<Partial>::max());
  for (uint64_t i = 0; i < BucketSizes_; i++) {
    timestamps_[i].initialize(dis(gen));
  }

  std::cout << "Bucket Size: " << BucketSizes_ << std::endl;
}

CuckooHashMap::SizeType CuckooHashMap::size() const {
  CounterType s = 0;
  for (auto &c : locks_) {
    s += c.elemCounter();
  }

  assert(s >= 0);
  return static_cast<SizeType>(s);
}

double CuckooHashMap::loadfactor() const {
  return static_cast<double>(
      size() / static_cast<double>(BucketSizes_ * kDefaultSlotPerBucket));
}

bool CuckooHashMap::insert(const SizeType &setId, const ValueType &val,
                           Page &&page, Page &replaced_page) {
  // assert(size() <= (BucketSizes_ * kDefaultSlotPerBucket));
  Set set;

  // Obtain new timestamp for the insertion
  {
    auto lockManager = lockOne(setId);
    const Partial p = timestamps_[setId].getNewTimestamp();
    set = {setId, p};
  }

  auto b = lockTwoForOneSet(set);
  TablePosition pos = cuckooInsert(b, set, replaced_page);
  if (pos.status == CuckooStatus::ok) {
    addHashKeyToBucket(set, val, std::move(page), pos.index, pos.slot);
  }

  return pos.status == CuckooStatus::ok;
}

bool CuckooHashMap::find(const KeyType &key, const candidate_t &item,
                         ValueType &val) {
  Set set;
  Partial p;

  SizeType setId = setIndex(key);
  {
    auto lockManager = lockOne(setId);
    if (!timestamps_[setId].tryReadNewestTimestamp(p)) {
      // failed to get newest timestamp => curr set doesnot exist any key
      // std::cout << "Set #" << setId << " : tryReadNewestTimestamp failed"
      //           << std::endl;
      return false;
    }
  }

  set = {setId, p};
  bool done = false;
  while (!done) {
    TablePosition pos;
    {
      auto b = lockTwoForOneSet(set);
      // std::cout << "cuckooFind({" << set.id << ", "
      //           << static_cast<uint64_t>(set.partial) << "}, " << b.i1 << ","
      //           << b.i2 << ")" << std::endl;
      pos = cuckooFind(item, set, b.i1, b.i2);
      if (pos.status == CuckooStatus::ok) {
        val = buckets_[pos.index].value(pos.slot);
        done = true;
        break;
      }
    }
    // If failed, try next
    assert(pos.status == CuckooStatus::failureKeyNotFound);
    {
      auto lockManager = lockOne(setId);
      if (timestamps_[setId].tryReadNextTimestamp(p)) {
        set = {setId, p};
      } else {
        // std::cout << "Set #" << setId << " : tryReadNextTimestamp failed"
        //           << std::endl;
        break;
      }
    }
  }

  return done;
}

// bool CuckooHashMap::erase(const SizeType &key, const ValueType &val) {
//   // lazy deletion, delete `curr` timestamp will also delete all the
//   timestamp
//   // before curr
// }

// expects the locks to be taken and released outside the funciton
CuckooHashMap::TablePosition CuckooHashMap::cuckooFind(const candidate_t &item,
                                                       const Set &set,
                                                       const SizeType i1,
                                                       const SizeType i2) {
  int slot1, slot2;
  Bucket &b1 = buckets_[i1];
  if (tryReadFromBucket(item, b1, slot1, set, (set.id == i1))) {
    return TablePosition{i1, static_cast<SizeType>(slot1), CuckooStatus::ok};
  }
  Bucket &b2 = buckets_[i2];
  if (tryReadFromBucket(item, b2, slot2, set, (set.id == i2))) {
    return TablePosition{i2, static_cast<SizeType>(slot2), CuckooStatus::ok};
  }
  return TablePosition{0, 0, CuckooStatus::failureKeyNotFound};
}

CuckooHashMap::TablePosition CuckooHashMap::cuckooInsert(TwoBuckets &b,
                                                         const Set &set,
                                                         Page &replaced_page) {
  int slot1, slot2;
  Bucket &b1 = buckets_[b.i1];
  if (tryFindInsertBucket(b1, slot1, set, true)) {
    return TablePosition{b.i1, static_cast<SizeType>(slot1)};
  }
  Bucket &b2 = buckets_[b.i2];
  if (tryFindInsertBucket(b2, slot2, set, false)) {
    return TablePosition{b.i2, static_cast<SizeType>(slot2)};
  }

  // We are unlucky, so let's perform cuckoo hashing.
  SizeType insertBucket = 0;
  SizeType insertSlot = 0;

  // std::cout << "runCuckoo begin" << std::endl;
  CuckooStatus st = runCuckoo(b, insertBucket, insertSlot, replaced_page);
  if (st == CuckooStatus::ok) {
    assert(!locks_[lockIndex(b.i1)].tryLock());
    assert(!locks_[lockIndex(b.i2)].tryLock());
    assert(!buckets_[insertBucket].isOccupied(insertSlot));
    assert(insertBucket == set.id ||
           insertBucket == altIndex(set.id, set.partial));

    // Since we unlocked the buckets during run_cuckoo, another insert could
    // have inserted the same key into either b.i1 or b.i2, so we check for that
    // before doing the insert.
    // TODO: but the parital is keeped by the lock, so there might not have same
    // key

    // TablePosition pos = cuckooFind(set, b.i1, b.i2);
    // assert(pos.status != CuckooStatus::ok);

    return TablePosition{insertBucket, insertSlot, CuckooStatus::ok};
  }
  assert(st == CuckooStatus::failure);
  return TablePosition{0, 0, CuckooStatus::failure};
}

// bool CuckooHashMap::cuckooErase(const Set &set) {
//   const auto b = lockTwoForOneSet(set);
//   const TablePosition pos = cuckooFind(set, b.i1, b.i2);
//   if (pos.status == CuckooStatus::ok) {
//     deleteHashKeyFromBucket(set, pos.index, pos.slot);
//     return true;
//   } else {
//     return false;
//   }
// }

CuckooHashMap::CuckooStatus CuckooHashMap::runCuckoo(TwoBuckets &b,
                                                     SizeType &insertBucket,
                                                     SizeType &insertSlot,
                                                     Page &replaced_page) {
  b.unlock();
  CuckooRecordsArray cuckooPath;
  bool done = false;
  while (!done) {
    // std::cout << "cuckooPathSearch(" << b.i1 << ", " << b.i2 << ") begin"
    //           << std::endl;
    const int depth = cuckooPathSearch(cuckooPath, b.i1, b.i2);
    // if (depth < 0) {  // Search failed
    //   break;
    // }

    // std::cout << "cuckooPath[depth] = {" << cuckooPath[depth].bucketId << ","
    //           << cuckooPath[depth].toDelete << "} depth = " << depth
    //           << std::endl;
    // std::cout << "cuckooPathMove begin" << std::endl;
    if (cuckooPathMove(cuckooPath, depth, b, replaced_page)) {
      insertBucket = cuckooPath[0].bucketId;
      insertSlot = cuckooPath[0].slot;
      assert(insertBucket == b.i1 || insertBucket == b.i2);
      assert(!locks_[lockIndex(b.i1)].tryLock());
      assert(!locks_[lockIndex(b.i2)].tryLock());
      // assert(!buckets_[insertBucket].isOccupied(insertSlot));
      done = true;
      break;
    }
  }
  return done ? CuckooStatus::ok : CuckooStatus::failure;
}

int CuckooHashMap::cuckooPathSearch(CuckooRecordsArray &cuckooPath,
                                    const SizeType i1, const SizeType i2) {
  SlotInfo x = slotSearch(i1, i2);
  // if (x.isOldest == true) {
  //   return -1;
  // }
  // std::cout << "slotSearch end" << std::endl;

  // Fill in the cuckoo path slots from the end to the beginning.
  for (int i = x.depth; i >= 0; i--) {
    cuckooPath[i].slot = x.pathcode % kDefaultSlotPerBucket;
    x.pathcode /= kDefaultSlotPerBucket;
  }

  CuckooRecord &first = cuckooPath[0];
  if (x.pathcode == 0) {
    first.bucketId = i1;
  } else {
    assert(x.pathcode == 1);
    first.bucketId = i2;
  }
  {
    const auto lockManager = lockOne(first.bucketId);
    const Bucket &b = buckets_[first.bucketId];

    // TODO:check if the code is valid
    for (int k = 0; k < kDefaultSlotPerBucket; ++k) {
      SizeType tempSlot = (first.slot + k) % kDefaultSlotPerBucket;
      if (!b.isOccupied(tempSlot)) {
        first.slot = tempSlot;
        return 0;  // find a free slot along the path
      }
    }

    first.set = getSetFromBucket(b, first.bucketId, first.slot);
    first.toDelete = (x.depth == 0) ? x.isOldest : false;
  }
  for (int i = 1; i <= x.depth; ++i) {
    CuckooRecord &curr = cuckooPath[i];
    const CuckooRecord &prev = cuckooPath[i - 1];
    assert(prev.bucketId == prev.set.id ||
           prev.bucketId == altIndex(prev.set.id, prev.set.partial));
    curr.bucketId = altIndex(prev.bucketId, prev.set.partial);

    const auto lockManager = lockOne(curr.bucketId);
    const Bucket &b = buckets_[curr.bucketId];

    // TODO:check if the code is valid
    for (int k = 0; k < kDefaultSlotPerBucket; ++k) {
      SizeType tempSlot = (curr.slot + k) % kDefaultSlotPerBucket;
      if (!b.isOccupied(tempSlot)) {
        curr.slot = tempSlot;
        return i;
      }
    }
    curr.set = getSetFromBucket(b, curr.bucketId, curr.slot);
    curr.toDelete = (x.depth == i) ? x.isOldest : false;
  }
  return x.depth;
}

// At the end of the function TwoBuckets lock must hold
bool CuckooHashMap::cuckooPathMove(CuckooRecordsArray &cuckooPath,
                                   SizeType depth, TwoBuckets &b,
                                   Page &replaced_page) {
  if (depth == 0) {
    const SizeType currId = cuckooPath[0].bucketId;
    assert(currId == b.i1 || currId == b.i2);

    b = lockTwo(b.i1, b.i2);

    // TODO:check if the code is valid
    // check if there is a free slot for us to avoid the deletion
    for (int k = 0; k < kDefaultSlotPerBucket; ++k) {
      SizeType tempSlot = (cuckooPath[0].slot + k) % kDefaultSlotPerBucket;
      if (!buckets_[currId].isOccupied(tempSlot)) {
        cuckooPath[0].slot = tempSlot;
        return true;
      }
    }

    assert(buckets_[currId].isOccupied(cuckooPath[0].slot));

    // Delete the selected oldest entry
    CuckooRecord &first = cuckooPath[0];
    const SizeType fs = first.slot;
    Bucket &fb = buckets_[first.bucketId];
    if (!cuckooPath[0].toDelete ||
        !fb.isIdentical(fs, first.set.partial,
                        (first.bucketId == first.set.id))) {
      // Current Entry has been changed, search again
      b.unlock();
      return false;
    }

    assert(cuckooPath[0].toDelete);
    replaced_page = buckets_[first.bucketId].page(first.slot);
    deleteHashKeyFromBucket(first.set, first.bucketId, first.slot);
    return true;
  }

  while (depth > 0) {
    CuckooRecord &from = cuckooPath[depth - 1];
    CuckooRecord &to = cuckooPath[depth];
    const SizeType fs = from.slot;
    SizeType ts = to.slot;
    TwoBuckets twoB;
    LockManager extraManager;
    if (depth == 1) {
      std::tie(twoB, extraManager) = lockThree(b.i1, b.i2, to.bucketId);
    } else {
      twoB = lockTwo(from.bucketId, to.bucketId);
    }

    Bucket &fb = buckets_[from.bucketId];
    Bucket &tb = buckets_[to.bucketId];

    // TODO:check if the code is valid
    // Delete the selected oldest entry
    if (cuckooPath[depth].toDelete) {
      bool done = false;
      for (int k = 0; k < kDefaultSlotPerBucket; ++k) {
        SizeType tempSlot = (ts + k) % kDefaultSlotPerBucket;
        if (!tb.isOccupied(tempSlot)) {
          ts = tempSlot;
          done = true;
          break;
        }
      }

      if (!done &&
          tb.isIdentical(ts, to.set.partial, (to.bucketId == to.set.id))) {
        replaced_page = tb.page(ts);
        deleteHashKeyFromBucket(to.set, to.bucketId, ts);
      }
    }

    if (tb.isOccupied(ts) || !fb.isOccupied(fs) ||
        !fb.isIdentical(fs, from.set.partial, (from.bucketId == from.set.id))) {
      return false;
    }

    tb.setEntry(ts, fb.value(fs), std::move(fb.page(fs)), fb.partial(fs),
                fb.isInplace(fs), fb.isOccupied(fs));
    fb.clearEntry(fs);
    if (depth == 1) {
      b = std::move(twoB);
    }
    depth--;
  }
  return true;
}

// BFS search for empty slot
CuckooHashMap::SlotInfo CuckooHashMap::slotSearch(const SizeType i1,
                                                  const SizeType i2) {
  // std::cout << "slotSearch(" << i1 << ", " << i2 << ") begin" << std::endl;
  SlotInfo oldestEntry(0, 0, 0);
  ValueType oldestLifetime = std::numeric_limits<ValueType>::max();

  BQueue q;
  std::unordered_set<SizeType> iter_set;  // avoid search loop
  iter_set.reserve(BQueue::maxCuckooCount);

  q.enqueue(SlotInfo(i1, 0, 0));
  q.enqueue(SlotInfo(i2, 1, 0));
  iter_set.emplace(i1);
  iter_set.emplace(i2);
  while (!q.isEmpty()) {
    SlotInfo x = q.dequeue();
    // std::cout << "x {" << x.bucketId << ", " << (uint64_t)x.depth << ", "
    //           << x.isOldest << ", " << x.pathcode << "} dequeue" <<
    //           std::endl;

    auto lockManager = lockOne(x.bucketId);
    Bucket &b = buckets_[x.bucketId];

    SizeType startSlot = x.pathcode % kDefaultSlotPerBucket;
    for (SizeType i = 0; i < kDefaultSlotPerBucket; ++i) {
      uint16_t slot = (startSlot + i) % kDefaultSlotPerBucket;
      if (!b.isOccupied(slot)) {
        x.pathcode = x.pathcode * kDefaultSlotPerBucket + slot;
        // std::cout << "x {" << x.bucketId << ", " << (uint64_t)x.depth << ", "
        //           << x.isOldest << ", " << x.pathcode << "} return"
        //           << std::endl;
        return x;
      }

      assert(b.isOccupied(slot));
      ValueType currLifetime = b.value(slot);
      if (currLifetime < oldestLifetime) {
        oldestEntry = x;
        oldestEntry.pathcode = x.pathcode * kDefaultSlotPerBucket + slot;
        oldestLifetime = currLifetime;
      }

      const Partial partial = b.partial(slot);
      const SizeType yBucketId = altIndex(x.bucketId, partial);
      if ((x.depth < kMaxBFSPathLen - 1) && (iter_set.count(yBucketId) == 0)) {
        assert(!q.isFull());
        SlotInfo y(yBucketId, x.pathcode * kDefaultSlotPerBucket + slot,
                   x.depth + 1);
        iter_set.emplace(yBucketId);
        q.enqueue(y);
      }
    }
  }

  // Exceed the max search depth, can't find any free slot
  // return the oldest entry
  oldestEntry.isOldest = true;
  // std::cout << "oldestEntry {" << oldestEntry.bucketId << ", "
  //           << (uint64_t)oldestEntry.depth << ", " << oldestEntry.isOldest
  //           << ", " << oldestEntry.pathcode << "} return" << std::endl;
  return oldestEntry;
}

// Lock functions
CuckooHashMap::LockManager CuckooHashMap::lockOne(SizeType i) const {
  Lock &lock = locks_[lockIndex(i)];
  lock.lock();
  return LockManager(&lock);
}

CuckooHashMap::TwoBuckets CuckooHashMap::lockTwo(SizeType i1,
                                                 SizeType i2) const {
  SizeType l1 = lockIndex(i1);
  SizeType l2 = lockIndex(i2);

  if (l2 < l1) {
    std::swap(l1, l2);
  }

  locks_[l1].lock();
  if (l2 != l1) {
    locks_[l2].lock();
  }

  return TwoBuckets(locks_, i1, i2);
}

// Bucket1 = key, Bucket2 = key ^ Hash(partial)
CuckooHashMap::TwoBuckets CuckooHashMap::lockTwoForOneSet(
    const Set &set) const {
  const SizeType i1 = set.id;
  const SizeType i2 = altIndex(set.id, set.partial);

  return lockTwo(i1, i2);
}

std::pair<CuckooHashMap::TwoBuckets, CuckooHashMap::LockManager>
CuckooHashMap::lockThree(SizeType i1, SizeType i2, SizeType i3) const {
  std::array<SizeType, 3> l{lockIndex(i1), lockIndex(i2), lockIndex(i3)};
  // Lock in order
  if (l[2] < l[1]) {
    std::swap(l[2], l[1]);
  }
  if (l[2] < l[0]) {
    std::swap(l[2], l[0]);
  }
  if (l[1] < l[0]) {
    std::swap(l[1], l[0]);
  }

  locks_[l[0]].lock();
  if (l[1] != l[0]) {
    locks_[l[1]].lock();
  }
  if (l[2] != l[1]) {
    locks_[l[2]].lock();
  }
  return std::make_pair(TwoBuckets(locks_, i1, i2),
                        LockManager((lockIndex(i3) == lockIndex(i1) ||
                                     lockIndex(i3) == lockIndex(i2))
                                        ? nullptr
                                        : &locks_[lockIndex(i3)]));
}

// Bucket Related function
bool CuckooHashMap::tryFindInsertBucket(const Bucket &b, int &slot,
                                        const Set &set, bool inplace) const {
  slot = -1;
  for (int i = 0; i < kDefaultSlotPerBucket; ++i) {
    assert(!b.isIdentical(i, set.partial, inplace));
    if (!b.isOccupied(i)) {
      slot = i;
      break;
    }
  }

  if (slot == -1) {
    return false;
  } else {
    return true;
  }
}

// try_read_from_bucket will search the bucket for the given key and return
// the index of the slot if found, or -1 if not found.
bool CuckooHashMap::tryReadFromBucket(const candidate_t &item, Bucket &b,
                                      int &slot, const Set &set,
                                      bool inplace) const {
  slot = -1;
  for (int i = 0; i < kDefaultSlotPerBucket && slot == -1; ++i) {
    if (b.isIdentical(i, set.partial, inplace)) {
      Page &curr_page = b.page(i);
      for (auto &page_item : curr_page) {
        if (page_item.id == item.id) {
          slot = i;
          page_item.hit_count++;
          break;
        }
      }
    } else {
      // std::cout << "b.isIdentical(" << set.id << ", "
      //           << static_cast<uint64_t>(set.partial) << "}, " << inplace
      //           << ", " << slot << ", false)&& key == " << key << std::endl;
      continue;
    }
  }

  if (slot == -1) {
    return false;
  } else {
    return true;
  }
}

void CuckooHashMap::deleteHashKeyFromBucket(const Set &set,
                                            const SizeType bucketId,
                                            const SizeType slot) {
  assert(bucketId == set.id || bucketId == altIndex(set.id, set.partial));

  // std::cout << "deleteHashKeyFromBucket(" << set.id << ", "
  //           << static_cast<uint64_t>(set.partial) << "}, " << bucketId << ",
  //           "
  //           << slot << ")" << std::endl;

  timestamps_[set.id].removeTimestamp(set.partial);
  buckets_[bucketId].clearEntry(slot);
  locks_[lockIndex(bucketId)].decrElemCounter();
}

void CuckooHashMap::addHashKeyToBucket(const Set &set, const ValueType &value,
                                       Page &&page, const SizeType bucketId,
                                       const SizeType slot) {
  assert(bucketId == set.id || bucketId == altIndex(set.id, set.partial));

  // std::cout << "addHashKeyToBucket({" << set.id << ", "
  //           << static_cast<uint64_t>(set.partial) << "}, " << value << ", "
  //           << bucketId << ", " << slot << ")" << std::endl;

  // refresh hit count
  for (auto &item : page) {
    item.hit_count = 0;
  }

  buckets_[bucketId].setEntry(slot, value, std::move(page), set.partial,
                              (bucketId == set.id), true);
  locks_[lockIndex(bucketId)].incrElemCounter();
}