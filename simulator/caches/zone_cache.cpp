#include "caches/zone_cache.hpp"

#include <cstdint>
#include <vector>

#include "caches/cache.hpp"
#include "candidate.hpp"
#include "config.hpp"
#include "constants.hpp"
#include "cuckoo_sets.hpp"
#include "q_log.hpp"
#include "s3fifo.hpp"
#include "s_log.hpp"

namespace cache {

ZoneCache::ZoneCache(stats::StatsCollector *sc, stats::LocalStatsCollector &gs,
                     const libconfig::Setting &settings)
    : Cache(sc, gs, settings) {
  misc::ConfigReader cfg(settings);

  uint64_t flash_size_mb = (uint64_t)cfg.read<int>("cache.flashSizeMB");
  uint64_t page_size = (uint64_t)cfg.read<int>("sets.setCapacity");
  uint64_t flash_size = flash_size_mb * 1024 * 1024;
  uint64_t total_page_num = flash_size / page_size;

  auto &set_stats = statsCollector->createLocalCollector("sets");
  _sets =
      new flashCache::CuckooSets(total_page_num, page_size, set_stats, this);

  auto &log_stats = statsCollector->createLocalCollector("log");
  _log = new flashCache::SLog(total_page_num, log_stats);

  uint64_t memory_size =
      (uint64_t)cfg.read<int>("cache.memorySizeMB") * 1024 * 1024;
  uint64_t mem_cache_capacity = memory_size;
  auto &memory_cache_stats = statsCollector->createLocalCollector("memCache");
  _memCache = new memcache::S3FIFO(mem_cache_capacity, memory_cache_stats);

  // if (cfg.exists("preLogAdmission")) {
  //   std::string policyType = cfg.read<const char
  //   *>("preLogAdmission.policy"); policyType.append(".preLogAdmission");
  //   const libconfig::Setting &admission_settings =
  //       cfg.read<libconfig::Setting &>("preLogAdmission");
  //   auto &admission_stats = statsCollector->createLocalCollector(policyType);
  //   _prelog_admission = admission::Policy::create(admission_settings, _sets,
  //                                                 _log, admission_stats);
  // }

  /* slow warmup */
  if (cfg.exists("cache.slowWarmup")) {
    warmed_up = true;
  }

  assert(warmed_up);
}

ZoneCache::~ZoneCache() {
  delete _sets;
  delete _log;
  delete _memCache;
}

void ZoneCache::insert(candidate_t id) {
  std::vector<candidate_t> ret = _memCache->insert(id);
  // if (warmed_up && _prelog_admission) {
  //   auto admitted = _prelog_admission->admit(ret);
  //   ret.clear();
  //   for (auto set : admitted) {
  //     ret.insert(ret.end(), set.second.begin(), set.second.end());
  //   }
  // }
  if (ret.size()) {
    ret = _log->insert(ret);
  }
  if (ret.size()) {
    ret = _sets->insert(ret);
  }
}

bool ZoneCache::find(candidate_t id) {
  if (_memCache->find(id)) {
    globalStats["memHits"]++;
    return true;
  } else if (_log->find(id)) {
    globalStats["logHits"]++;
    return true;
  } else if (_sets->find(id)) {
    globalStats["setHits"]++;
    return true;
  }

  return false;
}

double ZoneCache::calcFlashWriteAmp() {
  double set_write_amp = _sets->calcWriteAmp();
  double flash_write_amp = set_write_amp + _log->calcWriteAmp();
  if (warmed_up && _prelog_admission) {
    // includej all bytes not written to structs
    return flash_write_amp * _prelog_admission->byteRatioAdmitted();
  } else {
    return flash_write_amp;
  }
}

}  // namespace cache