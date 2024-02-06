#pragma once

#include <sys/types.h>

#include <cstdint>
#include <queue>
#include <unordered_map>

#include "mem_cache.hpp"
#include "stats/stats.hpp"

namespace memcache {

template <typename DataT>
struct List {
  typedef DataT Data;

  struct Entry {
    Data data;
    Entry* prev;
    Entry* next;

    void remove() {
      assert(prev != nullptr);
      prev->next = next;
      assert(next != nullptr);
      next->prev = prev;
    }
  };

  List()
      : _head(new Entry{Data(), nullptr, nullptr}),
        _tail(new Entry{Data(), nullptr, nullptr}) {
    _head->next = _tail;
    _tail->prev = _head;
  }

  ~List() {
    Entry* entry = _head;

    while (entry) {
      auto* next = entry->next;
      delete entry;
      entry = next;
    }
  }

  void insert_front(Entry* entry) {
    entry->prev = _head;
    entry->next = _head->next;
    _head->next->prev = entry;
    _head->next = entry;
  }

  void insert_back(Entry* entry) {
    entry->prev = _tail->prev;
    entry->next = _tail;
    _tail->prev->next = entry;
    _tail->prev = entry;
  }

  Data& front() { return _head->next->data; }

  Data& back() { return _tail->prev->data; }

  bool empty() const { return _head->next == _tail; }

  Entry* begin() const { return _head->next; }

  Entry* end() const { return _tail; }

  Entry *_head, *_tail;
};

template <typename Data>
struct Tags : public std::unordered_map<int64_t, typename List<Data>::Entry*> {
  typedef typename List<Data>::Entry Entry;

  Entry* lookup(int64_t id) const {
    auto itr = this->find(id);
    if (itr != this->end()) {
      return itr->second;
    } else {
      return nullptr;
    }
  }

  Entry* allocate(candidate_t item) {
    auto* entry = new Entry{item, nullptr, nullptr};
    (*this)[item.id] = entry;
    return entry;
  }

  Entry* evict(int64_t id) {
    auto itr = this->find(id);
    assert(itr != this->end());

    auto* entry = itr->second;
    this->erase(itr);
    return entry;
  }
};

class S3LRU : public virtual MemCache {
 public:
  S3LRU(uint64_t _max_size, stats::LocalStatsCollector& mem_stats)
      : max_size(_max_size),
        current_size(0),
        ghost_size(0),
        _mem_stats(mem_stats) {
    _mem_stats["lruCacheCapacity"] = _max_size;
  }

  void update(candidate_t item) {
    auto* entry = tags.lookup(item.id);
    if (entry) {
      entry->remove();
    } else {
      entry = tags.allocate(item);
    }

    list.insert_front(entry);
  }

  void replaced(candidate_t item) {
    auto* entry = tags.evict(item.id);
    current_size -= item.obj_size;
    entry->remove();
    delete entry;
  }

  candidate_t rank() { return list.back(); }

  std::vector<candidate_t> insert(candidate_t id) {
    assert(id.hit_count == 0);

    std::vector<candidate_t> evicted;
    std::vector<candidate_t> ghost_evicted;
    if ((uint64_t)id.obj_size > max_size) {
      _mem_stats["numEvictions"]++;
      _mem_stats["sizeEvictions"] += id.obj_size;
      evicted.push_back(id);
      return evicted;
    }

    // check ghost list
    if (ghost_tags.count(id.id) > 0) {
      ghost_tags[id.id].hit_count++;
      _mem_stats["numEvictions"]++;
      _mem_stats["sizeEvictions"] += id.obj_size;
      evicted.push_back(id);

      ghost_size -= ghost_tags[id.id].obj_size;
      ghost_tags.erase(id.id);
      return evicted;
    }

    while (current_size + id.obj_size > max_size) {
      candidate_t evict_item = rank();
      _mem_stats["numEvictions"]++;
      _mem_stats["sizeEvictions"] += evict_item.obj_size;

      if (evict_item.hit_count > 0) {
        evicted.push_back(evict_item);
      } else {
        ghost_evicted.push_back(evict_item);
      }

      replaced(evict_item);
    }

    ghostInsert(ghost_evicted);

    update(id);
    current_size += id.obj_size;
    _mem_stats["current_size"] = current_size;
    assert(current_size <= max_size);
    return evicted;
  }

  bool find(candidate_t item) {
    auto* entry = tags.lookup(item.id);
    if ((bool)entry) {
      update(item);
      entry->data.hit_count++;
      _mem_stats["hits"]++;
    } else {
      _mem_stats["misses"]++;
    }
    return (bool)entry;
  }

  void flushStats() {
    _mem_stats["hits"] = 0;
    _mem_stats["misses"] = 0;
    _mem_stats["numEvictions"] = 0;
    _mem_stats["sizeEvictions"] = 0;
  }

 private:
  List<candidate_t> list;
  Tags<candidate_t> tags;
  std::queue<int64_t> ghost;
  std::unordered_map<int64_t, candidate_t> ghost_tags;
  typedef typename List<candidate_t>::Entry Entry;
  uint64_t max_size;
  uint64_t current_size;
  uint64_t ghost_size;
  stats::LocalStatsCollector& _mem_stats;

  void ghostInsert(std::vector<candidate_t> items) {
    for (auto& item : items) {
      assert(item.hit_count == 0);

      while (ghost_size + item.obj_size > max_size) {
        int64_t evict_id = ghost.front();
        if (ghost_tags.count(evict_id) > 0) {
          if (ghost_tags[evict_id].hit_count == 0) {
            ghost_size -= ghost_tags[evict_id].obj_size;
          }
          ghost_tags.erase(evict_id);
        }

        ghost.pop();  // evict the first item of the ghost
      }

      ghost.push(item.id);
      ghost_tags[item.id] = item;
      ghost_size += item.obj_size;
    }
  }
};

}  // namespace memcache
