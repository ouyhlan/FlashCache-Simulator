#pragma once

#include <cstdint>
#include <vector>

#include "candidate.hpp"
#include "utils/QuotientIndex.h"

// max item 2^32
class QuotientIndexArray {
  using PowerType = uint8_t;  // type to be power e.g. PowerType q; -> 2^q
  using SizeType = uint32_t;
  using TagType = uint64_t;
  using SubIndex = QuotientIndex;

 public:
  QuotientIndexArray(PowerType q);

  std::vector<candidate_t> insert(candidate_t item);
  bool find(candidate_t item);
  bool readmit(candidate_t item);

  double ratioCapacityUsed() { return (double)numEntries_ / maxNumEntries_; }

 private:
  const uint64_t maxNumEntries_;
  const uint64_t indexMask_;
  const PowerType offsetBits_;
  const uint64_t offsetMask_;
  std::vector<SubIndex> arr_;
  SizeType numEntries_;

  SizeType getQuotientIndex(uint64_t key) {
    return static_cast<SizeType>(key & indexMask_);
  }
};