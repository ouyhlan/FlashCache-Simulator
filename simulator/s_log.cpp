#include "s_log.hpp"

#include <algorithm>
#include <cstdint>
#include <vector>

#include "candidate.hpp"

static uint8_t bitCalc(const uint64_t capacity) {
  uint8_t bit_to_use;
  for (bit_to_use = 0; (1ull << bit_to_use) < capacity; ++bit_to_use)
    ;
  return bit_to_use;
}

namespace flashCache {

SLog::SLog(uint64_t log_capacity, stats::LocalStatsCollector &log_stats)
    : _log_stats(log_stats),
      _index(bitCalc(log_capacity)),
      _total_capacity(log_capacity),
      _total_size(0) {}

std::vector<candidate_t> SLog::insert(std::vector<candidate_t> items) {
  std::vector<candidate_t> evicted;
  evicted.clear();

  for (auto item : items) {
    auto local_evict = _index.insert(item);
    _log_stats["bytes_written"] += item.obj_size;
    _log_stats["stores_requested_bytes"] += item.obj_size;
    _total_size += item.obj_size;

    if (local_evict.size() > 0) {
      for (auto obj : local_evict) {
        _total_size -= obj.obj_size;
      }
      evicted.insert(evicted.end(), local_evict.begin(), local_evict.end());
    }
  }

  _log_stats["current_size"] = _total_size;
  assert(_total_capacity >= _total_size);
  return evicted;
}

bool SLog::find(candidate_t item) {
  if (_index.find(item)) {
    _log_stats["hits"]++;
    return true;
  } else {
    _log_stats["misses"]++;
    return false;
  }
}

void SLog::readmit(std::vector<candidate_t> items) {
  for (auto &item : items) {
    if (_index.readmit(item)) {
      _total_size += item.obj_size;
    }
  }
  _log_stats["current_size"] = _total_size;
  assert(_total_capacity >= _total_size);
}

void SLog::insertFromSets(candidate_t item) {
  if (item.hit_count > 0) {
    if (_index.readmit(item)) {
      _total_size += item.obj_size;
    }
  } else {
    _index.ghostInsert(item);
  }

  _log_stats["current_size"] = _total_size;
  assert(_total_capacity >= _total_size);
}
}  // namespace flashCache