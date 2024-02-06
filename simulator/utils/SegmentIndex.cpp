#include "utils/SegmentIndex.h"

#include <cstdint>
#include <limits>
#include <vector>

#include "candidate.hpp"

#define LOW_MASK(n) ((1ull << (n)) - 1ull)

SegmentIndex::SegmentIndex(PowerType q)
    : _index_mask(LOW_MASK(q)),
      _max_num_entries(1ull << q),
      _num_entries(0),
      _hit_one_entries(0),
      _ghost_size(0),
      _head(0) {
  _index.resize(_max_num_entries);
}

bool SegmentIndex::insert(candidate_t item) {
  if (_num_entries >= _max_num_entries &&
      _hit_one_entries >= _max_num_entries) {
    return false;
  }
  // if (_num_entries >= _max_num_entries) {
  //   return false;
  // }

  constexpr uint64_t admit_threshold =
      0.01l * std::numeric_limits<uint64_t>::max();

  // check ghost list
  if (_ghost_tags.count(item.id) > 0) {
    item.hit_count = 1;
    _ghost_size -= 1;
    _ghost_tags.erase(item.id);
  } else if (_ghost.size() == 0 || _rand_gen.next() <= admit_threshold) {
    item.hit_count = 1;
  } else {
    item.hit_count = 0;
  }

  if (_num_entries == _max_num_entries) {
    SizeType beg = _head;
    do {
      candidate_t &curr_index_item = _index[_head];
      _head = incr(_head);
      // find hit count = 0
      if (curr_index_item.hit_count == 0) {
        ghostInsert(curr_index_item);
        curr_index_item = item;
        if (item.hit_count > 0) {
          _hit_one_entries++;
        }

        return true;
      }
    } while (_head != beg);

    std::cout << "Bug Detected" << std::endl;
    // fail to insert
    return false;
  } else {
    _index[_num_entries++] = item;
    if (item.hit_count > 0) {
      _hit_one_entries++;
    }
  }

  return true;
}

bool SegmentIndex::find(candidate_t item) {
  for (SizeType i = 0; i < _num_entries; i++) {
    if (_index[i].id == item.id) {
      _index[i].hit_count++;
      if (_index[i].hit_count == 1) {
        _hit_one_entries++;
      }
      return true;
    }
  }

  return false;
}

std::vector<candidate_t> SegmentIndex::removeAll() {
  std::vector<candidate_t> evicted;

  for (auto &item : _index) {
    evicted.push_back(item);
  }

  _num_entries = 0;
  _hit_one_entries = 0;
  return evicted;
}

void SegmentIndex::ghostInsert(candidate_t item) {
  assert(item.hit_count == 0);
  while (_ghost_size + 1 > _max_num_entries * 8) {
    int64_t evict_id = _ghost.front();
    if (_ghost_tags.count(evict_id) > 0) {
      _ghost_size -= 1;
      _ghost_tags.erase(evict_id);
    }
    _ghost.pop();
  }

  _ghost.push(item.id);
  _ghost_tags[item.id] = item;
  _ghost_size += 1;
}

SegmentIndex::SizeType SegmentIndex::decr(SizeType idx) {
  return ((uint64_t)idx - 1) & _index_mask;
}

SegmentIndex::SizeType SegmentIndex::incr(SizeType idx) {
  return ((uint64_t)idx + 1) & _index_mask;
}
