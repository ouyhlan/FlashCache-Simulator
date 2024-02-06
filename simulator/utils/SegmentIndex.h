#pragma once

#include <cstdint>
#include <queue>
#include <vector>

#include "candidate.hpp"
#include "rand.hpp"
#include "stats/stats.hpp"

class SegmentIndex {
  using PowerType = uint8_t;
  using SizeType = uint32_t;
  using TagType = uint64_t;

 public:
  SegmentIndex(PowerType q);

  bool insert(candidate_t item);
  bool find(candidate_t item);

  // return all the element and remove them from index
  std::vector<candidate_t> removeAll();

  // ghost related funciton
  void ghostInsert(candidate_t item);

  bool isFull() { return _num_entries == _max_num_entries; }

  bool canRemoveAll() { return _hit_one_entries == _max_num_entries; }

 private:
  const uint64_t _index_mask;
  const uint64_t _max_num_entries;
  SizeType _num_entries;
  SizeType _hit_one_entries;
  SizeType _ghost_size;
  SizeType _head;
  std::vector<candidate_t> _index;
  std::queue<int64_t> _ghost;
  std::unordered_map<int64_t, candidate_t> _ghost_tags;
  misc::Rand _rand_gen;

  // index calculate
  SizeType decr(SizeType idx);
  SizeType incr(SizeType idx);
};