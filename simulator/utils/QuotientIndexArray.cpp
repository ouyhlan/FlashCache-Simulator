#include "utils/QuotientIndexArray.h"

#include <cassert>
#include <cstdint>
#include <vector>

#include "candidate.hpp"
#include "utils/CuckooHashConfig.h"
#include "utils/QuotientIndex.h"

#define LOW_MASK(n) ((1ull << (n)) - 1ull)

static uint8_t bitCalc(const uint64_t capacity) {
  uint8_t bit_to_use = 0;
  for (bit_to_use = 0; (1ull << bit_to_use) < capacity; ++bit_to_use)
    ;
  return bit_to_use;
}

QuotientIndexArray::QuotientIndexArray(PowerType q)
    : maxNumEntries_(1ull << q),
      indexMask_(LOW_MASK(q)),
      offsetBits_(bitCalc(kQFSubIndexSize)),
      offsetMask_(LOW_MASK(offsetBits_)),
      arr_(maxNumEntries_ / kQFSubIndexSize, QuotientIndex(offsetBits_)),
      numEntries_(0) {}

std::vector<candidate_t> QuotientIndexArray::insert(candidate_t item) {
  assert(numEntries_ <= maxNumEntries_);
  std::vector<candidate_t> evicted;

  SizeType fq = getQuotientIndex(item.id);
  SizeType idx = fq >> offsetBits_;
  SizeType offset = fq & (SizeType)offsetMask_;

  // check whether to evicted
  // currently using the full state
  if (arr_[idx].ratioCapacityUsed() == 1.0) {
    evicted = arr_[idx].removeAll();
    numEntries_ -= evicted.size();
  }

  arr_[idx].insert(offset, item);
  numEntries_++;
  return evicted;
}

bool QuotientIndexArray::find(candidate_t item) {
  SizeType fq = getQuotientIndex(item.id);
  SizeType idx = fq >> offsetBits_;
  SizeType offset = fq & (SizeType)offsetMask_;

  return arr_[idx].find(offset, item);
}

bool QuotientIndexArray::readmit(candidate_t item) {
  SizeType fq = getQuotientIndex(item.id);
  SizeType idx = fq >> offsetBits_;
  SizeType offset = fq & (SizeType)offsetMask_;

  // check full state
  if (arr_[idx].ratioCapacityUsed() == 1.0) {
    // don't want to clean again
    return false;
  }

  arr_[idx].insert(offset, item);
  return true;
}