#pragma once

#include <cstdint>
#include <vector>

#include "utils/QuotientFilterEntry.h"

// max item 2^32
class QuotientFilter {
  using PowerType = uint8_t;  // type to be power e.g. PowerType q; -> 2^q
  using SizeType = uint32_t;
  using TagType = uint32_t;
  using Entry = QuotientFilterEntry;

 public:
  // q for table size 2^q, r for number of tag bits
  QuotientFilter(PowerType q, PowerType r);

  bool insert(uint64_t hk);
  bool remove(uint64_t hk);
  std::vector<Entry> getIndex() { return index_; }  // for test

 private:
  const PowerType qbits_;
  const PowerType rbits_;
  const uint64_t indexMask_;
  const uint64_t remainderMask_;
  const uint64_t maxNumEntries_;
  SizeType numEntries_;
  std::vector<Entry> index_;

  // math function
  SizeType getQuotientFromHash(uint64_t hk);
  TagType getRemainderFromHash(uint64_t hk);

  // index calculate
  SizeType decr(SizeType idx);
  SizeType incr(SizeType idx);

  // utils function
  SizeType findRunStartIndex(SizeType fq);
  void deleteEntry(SizeType idx, SizeType fq);
};