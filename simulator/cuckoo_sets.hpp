#pragma once

#include <cstdint>
#include <unordered_set>
#include <vector>

#include "caches/zone_cache.hpp"
#include "candidate.hpp"
#include "sets_abstract.hpp"
#include "stats/stats.hpp"
#include "utils/CuckooHashMap.h"
namespace flashCache {

class CuckooSets : public virtual SetsAbstract {
  using Page = std::vector<candidate_t>;

 public:
  CuckooSets(uint64_t total_page_num, uint64_t page_size,
             stats::LocalStatsCollector& set_stats,
             cache::ZoneCache* ref_cache);
  ~CuckooSets() {}

  std::vector<candidate_t> insert(std::vector<candidate_t> items);

  bool find(candidate_t item);

  // some no need funciton
  std::vector<candidate_t> insert(uint64_t set_num,
                                  std::vector<candidate_t> items) {
    return {};
  }

  std::unordered_set<uint64_t> findSetNums(candidate_t item) {
    std::unordered_set<uint64_t> set;
    set.emplace(calcSetNums(item.id));
    return set;
  }

  double ratioCapacityUsed() { return _total_size / (double)_total_capacity; }

  double calcWriteAmp() {
    return _set_stats["bytes_written"] /
           (double)_set_stats["stores_requested_bytes"];
  }

  double ratioEvictedToCapacity() { return 0; }
  void flushStats() {}  // does not print update before clearing
  uint64_t calcMemoryConsumption() { return 0; }
  bool trackHit(candidate_t item) {
    return 0;
  }  // hit on object in sets elsewhere in cache, for nru
  void enableDistTracking() { return; }
  void enableHitDistributionOverSets() { return; }

 private:
  stats::LocalStatsCollector& _set_stats;
  const uint64_t _num_sets;
  CuckooHashMap _index;
  uint64_t _total_size;
  const uint64_t _total_capacity;
  const uint64_t _page_size;
  uint64_t _curr_timestamp;
  cache::ZoneCache* _cache;

  uint32_t calcSetNums(uint64_t id);

  void updateStatsActualStore(uint64_t num_sets_updated) {
    _set_stats["bytes_written"] += (num_sets_updated * _page_size);
  }

  void updateStatsRequestedStore(candidate_t item) {
    _set_stats["stores_requested"]++;
    _set_stats["stores_requested_bytes"] += item.obj_size;
  }
};
}  // namespace flashCache