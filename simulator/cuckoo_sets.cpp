#include "cuckoo_sets.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <unordered_set>
#include <vector>

#include "candidate.hpp"
#include "utils/CuckooHashConfig.h"

#define LOW_MASK(n) ((1ull << (n)) - 1ull)

constexpr static uint8_t bitCalc(const uint64_t capacity) {
  uint8_t bit_to_use = 0;
  for (bit_to_use = 0; (1ull << bit_to_use) < capacity; ++bit_to_use)
    ;
  return bit_to_use;
}

namespace flashCache {

CuckooSets::CuckooSets(uint64_t total_page_num, uint64_t page_size,
                       stats::LocalStatsCollector &set_stats,
                       cache::ZoneCache *ref_cache)
    : _set_stats(set_stats),
      _num_sets(total_page_num / kDefaultSlotPerBucket),
      _index(bitCalc(_num_sets)),
      _total_size(0),
      _total_capacity(total_page_num),
      _page_size(page_size),
      _curr_timestamp(0),
      _cache(ref_cache) {}

std::vector<candidate_t> CuckooSets::insert(std::vector<candidate_t> items) {
  uint64_t num_cluster = items.size() / kQFSubIndexSize;

  // out order: hit_count high && obj_size small first to have more element
  // insert
  constexpr auto comp_fn = [](const candidate_t &i1, const candidate_t &i2) {
    if (i1.hit_count != i2.hit_count) {
      return i1.hit_count > i2.hit_count;
    }
    return i1.obj_size < i2.obj_size;
  };

  assert(num_cluster > 0);
  for (int i = 0; i < num_cluster; i++) {
    uint64_t beg = i * kQFSubIndexSize;
    uint64_t end = (i + 1) * kQFSubIndexSize;
    std::sort(items.begin() + beg, items.begin() + end, comp_fn);

    Page new_page;
    uint64_t curr_page_size = 0;
    std::unordered_map<uint32_t, uint64_t> set_count;
    for (int k = beg; k < end; k++) {
      if (curr_page_size + items[k].obj_size <= _page_size) {
        new_page.push_back(items[k]);
        curr_page_size += items[k].obj_size;
        updateStatsRequestedStore(items[k]);

        uint32_t set_id = calcSetNums(items[k].id);
        if (set_count.count(set_id) == 0) {
          set_count[set_id] = 1;
        } else {
          set_count[set_id]++;
        }
      } else {
        // if (items[k].hit_count > 0) {
        _cache->readmitToLogFromSets(items[k]);
        // }
      }
    }

    auto max_set = std::max_element(set_count.begin(), set_count.end());
    // std::cout << "Insert " << max_set->first << " from " << beg << " to " <<
    // end
    //           << " #item: " << new_page.size()
    //           << " page_size: " << curr_page_size << std::endl;
    Page replaced_page;
    _index.insert(max_set->first, ++_curr_timestamp, std::move(new_page),
                  replaced_page);
    updateStatsActualStore(1);

    // std::sort(replaced_page.begin(), replaced_page.end(),
    //           [](const candidate_t &i1, const candidate_t &i2) {
    //             return i1.hit_count > i2.hit_count;
    //           });

    // int readmit_count = 2;
    for (auto &item : replaced_page) {
      if (item.hit_count > 0) {
        item.hit_count = 0;  // refresh hit count
        _cache->readmitToLogFromSets(item);
      }
    }
  }
  return {};
}

bool CuckooSets::find(candidate_t item) {
  // calculate find range
  // ID: [Header][Set_id][(kDefaultSlotPerBucket)]
  constexpr uint8_t offsetBits = bitCalc(kQFSubIndexSize);
  constexpr uint8_t slotBits = bitCalc(kDefaultSlotPerBucket);
  constexpr uint8_t setRangeBits = offsetBits - slotBits;
  constexpr uint32_t setRangeMask = LOW_MASK(setRangeBits);
  constexpr uint32_t setRange = 1 << setRangeBits;

  uint32_t set_id = calcSetNums(item.id);
  uint32_t left = set_id & (~setRangeMask);  // [Partial Key]0000

  uint64_t val;
  bool success = false;
  for (uint32_t i = 0; i < setRange && !success; i++) {
    success |= _index.find(left + i, item, val);
  }
  return success;
}

uint32_t CuckooSets::calcSetNums(uint64_t id) {
  uint64_t res = id >> bitCalc(kDefaultSlotPerBucket);
  return static_cast<uint32_t>(res % (_num_sets));
}

}  // namespace flashCache