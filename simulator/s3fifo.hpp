#pragma once

#include <netdb.h>

#include <cassert>
#include <cstdint>
#include <queue>
#include <unordered_map>
#include <vector>

#include "candidate.hpp"
#include "mem_cache.hpp"
#include "stats/stats.hpp"

namespace memcache {

class S3FIFO : public virtual MemCache {
 public:
  S3FIFO(uint64_t max_size, stats::LocalStatsCollector& mem_stats)
      : _max_size(max_size),
        _current_size(0),
        _ghost_size(0),
        _mem_stats(mem_stats) {
    _mem_stats["s3fifoCacheCapacity"] = _max_size;
  }

  std::vector<candidate_t> insert(candidate_t item) {
    assert(tags.count(item.id) == 0);

    std::vector<candidate_t> evicted;
    std::vector<candidate_t> ghost_evicted;
    if ((uint64_t)item.obj_size > _max_size) {
      _mem_stats["numEvictions"]++;
      _mem_stats["sizeEvictions"] += item.obj_size;
      evicted.push_back(item);
      return evicted;
    }

    // check ghost list
    if (ghost_tags.count(item.id) > 0) {
      ghost_tags[item.id].hit_count++;
      _mem_stats["numEvictions"]++;
      _mem_stats["sizeEvictions"] += item.obj_size;
      evicted.push_back(item);

      _ghost_size -= ghost_tags[item.id].obj_size;
      ghost_tags.erase(item.id);
      return evicted;
    }

    while (_current_size + item.obj_size > _max_size) {
      int64_t evict_id = queue.front();
      _mem_stats["numEvictions"]++;
      _mem_stats["sizeEvictions"] += tags[evict_id].obj_size;

      if (tags[evict_id].hit_count > 0) {
        evicted.push_back(tags[evict_id]);
      } else {
        ghost_evicted.push_back(tags[evict_id]);
      }
      _current_size -= tags[evict_id].obj_size;

      queue.pop();
      tags.erase(evict_id);
    }

    ghostInsert(ghost_evicted);

    assert(item.hit_count == 0);
    queue.push(item.id);
    tags[item.id] = item;
    _current_size += item.obj_size;
    _mem_stats["current_size"] = _current_size;
    assert(_current_size <= _max_size);
    return evicted;
  }

  bool find(candidate_t item) {
    if (tags.count(item.id) > 0) {
      tags[item.id].hit_count++;
      _mem_stats["hits"]++;
      return true;
    } else {
      _mem_stats["misses"]++;
      return false;
    }
  }

  void flushStats() {
    _mem_stats["hits"] = 0;
    _mem_stats["misses"] = 0;
    _mem_stats["numEvictions"] = 0;
    _mem_stats["sizeEvictions"] = 0;
  }

 private:
  std::queue<int64_t> queue;
  std::queue<int64_t> ghost;
  std::unordered_map<int64_t, candidate_t> tags;
  std::unordered_map<int64_t, candidate_t> ghost_tags;
  uint64_t _max_size;
  uint64_t _current_size;
  uint64_t _ghost_size;
  stats::LocalStatsCollector& _mem_stats;

  void ghostInsert(std::vector<candidate_t> items) {
    for (auto& item : items) {
      assert(item.hit_count == 0);

      while (_ghost_size + item.obj_size > _max_size) {
        int64_t evict_id = ghost.front();
        if (ghost_tags.count(evict_id) > 0) {
          if (ghost_tags[evict_id].hit_count == 0) {
            _ghost_size -= ghost_tags[evict_id].obj_size;
          }
          ghost_tags.erase(evict_id);
        }

        ghost.pop();  // evict the first item of the ghost
      }

      ghost.push(item.id);
      ghost_tags[item.id] = item;
      _ghost_size += item.obj_size;
    }
  }
};
}  // namespace memcache