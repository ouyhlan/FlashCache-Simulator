#include <gtest/gtest.h>
#include <sys/types.h>

#include <cstdint>
#include <random>
#include <unordered_set>

#include "lib/zipf.h"
#include "stats/stats.hpp"
#include "utils/QuotientIndex.h"

uint8_t test_bucket_size = 15;  // 2^3=8 buckets

auto sc = new stats::StatsCollector("test_QuotientIndex.out");

TEST(QuotientIndexTest, SimpleInsertion) {
  uint32_t total_key_num = (1 << test_bucket_size);

  QuotientIndex qindex(test_bucket_size,
                       sc->createLocalCollector("SimpleInsertion"));

  for (uint64_t i = 0; i < total_key_num; ++i) {
    bool res = qindex.insert(i, i);
    EXPECT_TRUE(res);
  }

  for (uint64_t i = 0; i < total_key_num; ++i) {
    uint64_t val;
    bool found = qindex.find(i, val);
    EXPECT_TRUE(found);

    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(i, val);
    }
  }
}

TEST(QuotientIndexTest, FullRandomKeyInsertion) {
  uint32_t total_key_num = (1 << test_bucket_size);

  QuotientIndex qindex(test_bucket_size,
                       sc->createLocalCollector("FullRandomKeyInsertion"));

  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis(
      0, std::numeric_limits<uint64_t>::max());
  // std::uniform_int_distribution<uint64_t> dis(0, 1000);

  std::vector<uint64_t> vec(total_key_num);
  std::unordered_set<uint64_t> set;

  for (uint64_t i = 0; i < total_key_num; ++i) {
    uint64_t key;
    do {
      key = dis(gen);
    } while (set.count(key) > 0);

    set.emplace(key);
    vec[i] = key;
  }

  for (uint64_t i = 0; i < total_key_num; ++i) {
    bool res = qindex.insert(vec[i], vec[i]);
    EXPECT_TRUE(res);
  }

  for (uint64_t i = 0; i < total_key_num; ++i) {
    uint64_t val;
    bool found = qindex.find(vec[i], val);
    EXPECT_TRUE(found);

    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(vec[i], val);
    }
  }
}

TEST(QuotientIndexTest, SixtyLoadFactorRandomKeyInsertion) {
  uint32_t total_key_num = (1 << test_bucket_size) * 0.6;
  QuotientIndex qindex(
      test_bucket_size,
      sc->createLocalCollector("SixtyLoadFactorRandomKeyInsertion"));

  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis(
      0, std::numeric_limits<uint64_t>::max());
  // std::uniform_int_distribution<uint64_t> dis(0, 1000);

  std::vector<uint64_t> vec(total_key_num);
  std::unordered_set<uint64_t> set;

  for (uint64_t i = 0; i < total_key_num; ++i) {
    uint64_t key;
    do {
      key = dis(gen);
    } while (set.count(key) > 0);

    set.emplace(key);
    vec[i] = key;
  }

  std::cout << "Generate " << total_key_num << " keys" << std::endl;

  for (uint64_t i = 0; i < total_key_num; ++i) {
    bool res = qindex.insert(vec[i], vec[i]);
    EXPECT_TRUE(res);
  }

  for (uint64_t i = 0; i < total_key_num; ++i) {
    uint64_t val;
    bool found = qindex.find(vec[i], val);
    EXPECT_TRUE(found);

    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(vec[i], val);
    }
  }
}

TEST(QuotientIndexTest, SeventyLoadFactorRandomKeyInsertion) {
  uint32_t total_key_num = (1 << test_bucket_size) * 0.7;
  QuotientIndex qindex(
      test_bucket_size,
      sc->createLocalCollector("SeventyLoadFactorRandomKeyInsertion"));

  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis(
      0, std::numeric_limits<uint64_t>::max());
  // std::uniform_int_distribution<uint64_t> dis(0, 1000);

  std::vector<uint64_t> vec(total_key_num);
  std::unordered_set<uint64_t> set;

  for (uint64_t i = 0; i < total_key_num; ++i) {
    uint64_t key;
    do {
      key = dis(gen);
    } while (set.count(key) > 0);

    set.emplace(key);
    vec[i] = key;
  }

  std::cout << "Generate " << total_key_num << " keys" << std::endl;

  for (uint64_t i = 0; i < total_key_num; ++i) {
    bool res = qindex.insert(vec[i], vec[i]);
    EXPECT_TRUE(res);
  }

  for (uint64_t i = 0; i < total_key_num; ++i) {
    uint64_t val;
    bool found = qindex.find(vec[i], val);
    EXPECT_TRUE(found);

    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(vec[i], val);
    }
  }
}

TEST(QuotientIndexTest, EightyLoadFactorRandomKeyInsertion) {
  uint32_t total_key_num = (1 << test_bucket_size) * 0.8;
  QuotientIndex qindex(
      test_bucket_size,
      sc->createLocalCollector("EightyLoadFactorRandomKeyInsertion"));

  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis(
      0, std::numeric_limits<uint64_t>::max());
  // std::uniform_int_distribution<uint64_t> dis(0, 1000);

  std::vector<uint64_t> vec(total_key_num);
  std::unordered_set<uint64_t> set;

  for (uint64_t i = 0; i < total_key_num; ++i) {
    uint64_t key;
    do {
      key = dis(gen);
    } while (set.count(key) > 0);

    set.emplace(key);
    vec[i] = key;
  }

  std::cout << "Generate " << total_key_num << " keys" << std::endl;

  for (uint64_t i = 0; i < total_key_num; ++i) {
    bool res = qindex.insert(vec[i], vec[i]);
    EXPECT_TRUE(res);
  }

  for (uint64_t i = 0; i < total_key_num; ++i) {
    uint64_t val;
    bool found = qindex.find(vec[i], val);
    EXPECT_TRUE(found);

    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(vec[i], val);
    }
  }
}

TEST(QuotientIndexTest, NightyLoadFactorRandomKeyInsertion) {
  uint32_t total_key_num = (1 << test_bucket_size) * 0.9;
  QuotientIndex qindex(
      test_bucket_size,
      sc->createLocalCollector("NightyLoadFactorRandomKeyInsertion"));

  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis(
      0, std::numeric_limits<uint64_t>::max());
  // std::uniform_int_distribution<uint64_t> dis(0, 1000);

  std::vector<uint64_t> vec(total_key_num);
  std::unordered_set<uint64_t> set;

  for (uint64_t i = 0; i < total_key_num; ++i) {
    uint64_t key;
    do {
      key = dis(gen);
    } while (set.count(key) > 0);

    set.emplace(key);
    vec[i] = key;
  }

  std::cout << "Generate " << total_key_num << " keys" << std::endl;

  for (uint64_t i = 0; i < total_key_num; ++i) {
    bool res = qindex.insert(vec[i], vec[i]);
    EXPECT_TRUE(res);
  }

  for (uint64_t i = 0; i < total_key_num; ++i) {
    uint64_t val;
    bool found = qindex.find(vec[i], val);
    EXPECT_TRUE(found);

    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(vec[i], val);
    }
  }
}

TEST(QuotientIndexTest, FiftyLoadFactorZipfRandomKeyInsertion) {
  uint32_t total_object_num = 1000000;
  uint32_t total_key_num = (1 << test_bucket_size) * 0.5;
  QuotientIndex qindex(
      test_bucket_size,
      sc->createLocalCollector("FiftyLoadFactorZipfRandomKeyInsertion"));

  ZipfRequests zipf("", total_key_num, 0.9, 1);
  std::cout << "Generate " << total_key_num << " keys" << std::endl;

  std::unordered_set<uint64_t> obj_set;
  uint64_t curr_num = 0;
  for (uint64_t i = 0; i < total_object_num; ++i) {
    uint64_t key;
    uint64_t value;
    bool done = false;
    do {
      zipf.Sample(key, value);
      if (obj_set.count(key) == 0) {
        if (curr_num > total_key_num) continue;
        obj_set.insert(key);
        ++curr_num;
        done = true;
      } else {
        done = true;
      }
    } while (!done);
    if (!qindex.find(key, value)) {
      EXPECT_TRUE(qindex.insert(key, key));
      EXPECT_TRUE(qindex.find(key, value));
      curr_num++;
    }
    EXPECT_EQ(key, value);
  }
}

TEST(QuotientIndexTest, SixtyLoadFactorZipfRandomKeyInsertion) {
  uint32_t total_object_num = 10000;
  uint32_t total_key_num = (1 << test_bucket_size) * 0.6;
  QuotientIndex qindex(
      test_bucket_size,
      sc->createLocalCollector("SixtyLoadFactorZipfRandomKeyInsertion"));

  ZipfRequests zipf("", total_key_num, 0.9, 1);
  std::cout << "Generate " << total_key_num << " keys" << std::endl;

  std::unordered_set<uint64_t> obj_set;
  uint64_t curr_num = 0;
  for (uint64_t i = 0; i < total_object_num; ++i) {
    uint64_t key;
    uint64_t value;
    bool done = false;
    do {
      zipf.Sample(key, value);
      if (obj_set.count(key) == 0) {
        if (curr_num > total_key_num) continue;
        obj_set.insert(key);
        ++curr_num;
        done = true;
      } else {
        done = true;
      }
    } while (!done);
    if (!qindex.find(key, value)) {
      EXPECT_TRUE(qindex.insert(key, key));
      EXPECT_TRUE(qindex.find(key, value));
      curr_num++;
    }
    EXPECT_EQ(key, value);
  }
}

TEST(QuotientIndexTest, SeventyLoadFactorZipfRandomKeyInsertion) {
  uint32_t total_object_num = 1000000;
  uint32_t total_key_num = (1 << test_bucket_size) * 0.7;
  QuotientIndex qindex(
      test_bucket_size,
      sc->createLocalCollector("SeventyLoadFactorZipfRandomKeyInsertion"));

  ZipfRequests zipf("", total_key_num, 0.9, 1);
  std::cout << "Generate " << total_key_num << " keys" << std::endl;

  std::unordered_set<uint64_t> obj_set;
  uint64_t curr_num = 0;
  for (uint64_t i = 0; i < total_object_num; ++i) {
    uint64_t key;
    uint64_t value;
    bool done = false;
    do {
      zipf.Sample(key, value);
      if (obj_set.count(key) == 0) {
        if (curr_num > total_key_num) continue;
        obj_set.insert(key);
        ++curr_num;
        done = true;
      } else {
        done = true;
      }
    } while (!done);
    if (!qindex.find(key, value)) {
      EXPECT_TRUE(qindex.insert(key, key));
      EXPECT_TRUE(qindex.find(key, value));
      curr_num++;
    }
    EXPECT_EQ(key, value);
  }

  sc->print();
}