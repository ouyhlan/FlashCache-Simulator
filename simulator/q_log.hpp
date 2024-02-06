#pragma once

#include <cstdint>
#include <unordered_set>
#include <vector>

#include "candidate.hpp"
#include "log_abstract.hpp"
#include "stats/stats.hpp"
#include "utils/QuotientIndexArray.h"

namespace flashCache {

class QLog : public virtual LogAbstract {
 public:
  QLog(uint64_t log_capacity, stats::LocalStatsCollector &log_stats);

  // Insert new items and return evicted items
  std::vector<candidate_t> insert(std::vector<candidate_t> items);

  bool find(candidate_t item);
  void readmit(std::vector<candidate_t> items);

  // unused function
  void insertFromSets(candidate_t item);

  double ratioCapacityUsed() { return _index.ratioCapacityUsed(); }

  double calcWriteAmp() {
    return _log_stats["bytes_written"] /
           (double)_log_stats["stores_requested_bytes"];
  }

  void flushStats() {}

 private:
  stats::LocalStatsCollector &_log_stats;
  QuotientIndexArray _index;
  uint64_t _total_capacity;
  uint64_t _total_size;
};
}  // namespace flashCache