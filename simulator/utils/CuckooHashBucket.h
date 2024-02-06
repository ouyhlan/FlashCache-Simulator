#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <type_traits>
#include <vector>

#include "candidate.hpp"
#include "utils/CuckooHashConfig.h"

template <class ValueType, class Partial>
class CuckooHashBucket {
  using SizeType = uint32_t;  // slot size
  using BucketSize = uint8_t;
  using Page = std::vector<candidate_t>;

 public:
  CuckooHashBucket() noexcept
      : occupied_() {}  // all occupied_ default set to false

  void setEntry(SizeType idx, const ValueType &value, Page &&page,
                Partial partial, bool inplace, bool occupied) {
    pages_[idx] = std::move(page);
    values_[idx] = value;
    partials_[idx] = partial;
    inplace_[idx] = inplace;
    occupied_[idx] = occupied;
  }

  void clearEntry(SizeType idx) {
    assert(occupied_[idx]);
    occupied_[idx] = false;
  }

  Page &page(SizeType idx) { return pages_[idx]; }
  ValueType value(SizeType idx) const { return values_[idx]; }

  bool isOccupied(SizeType idx) const { return occupied_[idx]; }
  // void setOccupied(SizeType idx) { occupied_[idx] = true; }

  bool isInplace(SizeType idx) const { return inplace_[idx]; }

  Partial partial(SizeType idx) const { return partials_[idx]; }
  Partial &partial(SizeType idx) { return partials_[idx]; }

  bool isIdentical(SizeType idx, Partial partial, bool inplace) const {
    // theoretically when insertion, b.isInplace() and (b.partial(i) ==
    // hk.partial) will not happen together, since partial is strictly
    // increasing
    return (occupied_[idx]) && (inplace_[idx] == inplace) &&
           (partials_[idx] == partial);
  }

 private:
  std::array<Page, kDefaultSlotPerBucket> pages_;
  std::array<ValueType, kDefaultSlotPerBucket> values_;
  std::array<Partial, kDefaultSlotPerBucket> partials_;

  // inplace means the the key is current bucket id
  std::array<bool, kDefaultSlotPerBucket> inplace_;
  std::array<bool, kDefaultSlotPerBucket> occupied_;
};

// Timestamp system
template <class Partial>
class CuckooHashTimestamp {
 public:
  CuckooHashTimestamp() noexcept : nextTimestamp_(0), oldestTimestamp_(0) {}

  // Timestamp related function
  // Implement a chained queue-like timestamp and this queue will not be full,
  // since we have reserved enough space
  // Valid Timestamp (nextTimestamp_, oldestTimestamp_] or
  // {[0, nextTimestamp_) and [oldestTimestamp_, Partial::max()]}
  void initialize(uint8_t t) {
    nextTimestamp_ = t;
    oldestTimestamp_ = t;
  }

  Partial getNewTimestamp() {
    Partial res = nextTimestamp_;
    ++nextTimestamp_;
    return res;
  }

  bool tryReadNewestTimestamp(Partial &p) const {
    // check empty
    if (nextTimestamp_ == oldestTimestamp_) {
      return false;
    }
    p = nextTimestamp_ - 1;
    return true;
  }

  // input prev timestamp and change it into next timestamp
  bool tryReadNextTimestamp(Partial &p) const {
    Partial prevTimestamp = p;

    // check empty or reach the end of the timestamp
    if (nextTimestamp_ == oldestTimestamp_ ||
        prevTimestamp == oldestTimestamp_ || prevTimestamp == nextTimestamp_) {
      return false;
    }

    // check valid
    // since next != a
    // (next - a) < (next - old) ==> valid
    if (static_cast<Partial>(nextTimestamp_ - prevTimestamp) <
        static_cast<Partial>(nextTimestamp_ - oldestTimestamp_)) {
      p = prevTimestamp - 1;
      return true;
    } else {
      return false;
    }
  }

  void removeTimestamp(const Partial curr) {
    if (curr == oldestTimestamp_) {
      ++oldestTimestamp_;
    }
  }

  // void incrOldestTimestamp() { ++oldestTimestamp_; }
  Partial oldestTimestamp() const { return oldestTimestamp_; }

 private:
  Partial nextTimestamp_;
  Partial oldestTimestamp_;
};

// Lock Implementation
// Copied from libcuckoo
using CounterType = int64_t;
class __attribute__((aligned(64))) SpinLock {
 public:
  SpinLock() : elemCounter_(0) { lock_.clear(); }

  SpinLock(const SpinLock &other) noexcept : elemCounter_(other.elemCounter()) {
    lock_.clear();
  }

  SpinLock &operator=(const SpinLock &other) noexcept {
    elemCounter_ = other.elemCounter();
    return *this;
  }

  void lock() noexcept {
    while (lock_.test_and_set(std::memory_order_acq_rel))
      ;
  }

  void unlock() noexcept { lock_.clear(std::memory_order_release); }

  bool tryLock() noexcept {
    return !lock_.test_and_set(std::memory_order_acq_rel);
  }

  void decrElemCounter() noexcept { --elemCounter_; }
  void incrElemCounter() noexcept { ++elemCounter_; }
  CounterType elemCounter() const noexcept { return elemCounter_; }

 private:
  std::atomic_flag lock_;
  CounterType elemCounter_;
};