#pragma once

#include <cstdint>
#include <vector>

#include "candidate.hpp"
#include "utils/SegmentIndex.h"

class SegmentIndexArray {
  using PowerType = uint8_t;
  using SizeType = uint32_t;
  using TagType = uint64_t;
  using SubIndex = SegmentIndex;

 public:
  SegmentIndexArray(PowerType q);

  std::vector<candidate_t> insert(candidate_t item);
  bool find(candidate_t item);
  bool readmit(candidate_t item);

  void ghostInsert(candidate_t item);

  double ratioCapacityUsed() { return (double)_num_entries / _max_num_entries; }

 private:
  const uint64_t _max_num_entries;
  const uint64_t _index_mask;
  const PowerType _offset_bits;
  const uint64_t _offset_mask;
  std::vector<SubIndex> _arr;
  SizeType _num_entries;

  SizeType getSegmentIndex(uint64_t key) {
    return static_cast<SizeType>(key & _index_mask);
  }
};