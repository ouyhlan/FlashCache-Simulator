#include "utils/SegmentIndexArray.h"

#include "utils/CuckooHashConfig.h"

#define LOW_MASK(n) ((1ull << (n)) - 1ull)

static uint8_t bitCalc(const uint64_t capacity) {
  uint8_t bit_to_use = 0;
  for (bit_to_use = 0; (1ull << bit_to_use) < capacity; ++bit_to_use)
    ;
  return bit_to_use;
}

SegmentIndexArray::SegmentIndexArray(PowerType q)
    : _max_num_entries(1ull << q),
      _index_mask(LOW_MASK(q)),
      _offset_bits(bitCalc(kQFSubIndexSize)),
      _offset_mask(LOW_MASK(_offset_bits)),
      _arr(_max_num_entries / kQFSubIndexSize, SubIndex(_offset_bits)),
      _num_entries(0) {}

std::vector<candidate_t> SegmentIndexArray::insert(candidate_t item) {
  assert(_num_entries <= _max_num_entries);
  std::vector<candidate_t> evicted;
  SizeType group_id = (item.id & _index_mask) >> _offset_bits;

  // FIXME:Insertion Policy Mode
  // check whether to evicted
  // currently using the full state
  if (_arr[group_id].isFull() && _arr[group_id].canRemoveAll()) {
    evicted = _arr[group_id].removeAll();
    _num_entries -= evicted.size();
  }

  // TODO: no insertion policy mode
  // if (_arr[group_id].isFull()) {
  //   evicted = _arr[group_id].removeAll();
  //   _num_entries -= evicted.size();
  // }

  if (!_arr[group_id].isFull()) {
    _num_entries++;
  }
  _arr[group_id].insert(item);
  return evicted;
}

bool SegmentIndexArray::find(candidate_t item) {
  SizeType group_id = (item.id & _index_mask) >> _offset_bits;

  return _arr[group_id].find(item);
}

bool SegmentIndexArray::readmit(candidate_t item) {
  SizeType group_id = (item.id & _index_mask) >> _offset_bits;

  if (_arr[group_id].isFull()) {
    return false;
  }

  _arr[group_id].insert(item);
  return true;
}

void SegmentIndexArray::ghostInsert(candidate_t item) {
  item.hit_count = 0;

  SizeType group_id = (item.id & _index_mask) >> _offset_bits;
  _arr[group_id].ghostInsert(item);
}