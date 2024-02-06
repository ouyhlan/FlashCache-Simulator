#pragma once

#include <cstdint>
#include <vector>

#include "candidate.hpp"
#include "stats/stats.hpp"
#include "utils/QuotientIndexEntry.h"

// max item 2^32
class QuotientIndex {
  using PowerType = uint8_t;  // type to be power e.g. PowerType q; -> 2^q
  using SizeType = uint32_t;
  using TagType = uint64_t;
  using Entry = QuotientIndexEntry;

  // Cluster [begin, end]
  struct Cluster {
    SizeType begin;
    SizeType end;
    SizeType len;
  };

 public:
  // q for table size 2^q, r for number of tag bits
  QuotientIndex(PowerType q);

  bool insert(SizeType offset, candidate_t item);
  bool find(SizeType offset, candidate_t item);

  // return all the element and remove them from index
  std::vector<candidate_t> removeAll();

  // void remove(std::vector<candidate_t> evicted);

  // bool remove(uint64_t key);
  // std::vector<candidate_t> getCluster(uint64_t key);

  double ratioCapacityUsed() { return (double)numEntries_ / maxNumEntries_; }

  std::vector<Entry> getIndex() { return index_; }  // for test
  const Entry operator[](SizeType idx) const { return index_[idx]; }

 private:
  const uint64_t indexMask_;
  const uint64_t maxNumEntries_;
  SizeType numEntries_;

  std::vector<Entry> index_;

  // index calculate
  SizeType decr(SizeType idx);
  SizeType incr(SizeType idx);

  // utils function
  SizeType findRunStartIndex(SizeType fq);
  void deleteEntry(SizeType idx, SizeType fq);
};