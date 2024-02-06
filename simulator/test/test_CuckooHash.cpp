#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <random>
#include <unordered_set>
#include <vector>

#include "utils/CuckooHashConfig.h"
#include "utils/CuckooHashMap.h"

uint8_t test_bucket_size = 3;  // 2^3=8 buckets

TEST(CuckooHashTest, SimpleEachBucketInsertion) {
  CuckooHashMap map(test_bucket_size);

  uint32_t total_key_num = (1 << test_bucket_size);

  for (uint32_t i = 0; i < total_key_num; ++i) {
    bool res = map.insert(i, i);
    EXPECT_TRUE(res);
    if (res == false) {
      std::cout << "Failed insert key num: " << i << std::endl;
    }
  }

  for (uint32_t i = 0; i < total_key_num; ++i) {
    uint64_t val;
    bool found = map.find(i, val);
    EXPECT_TRUE(found);
    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(i, val);
    }
  }

  EXPECT_EQ(map.loadfactor(), 0.25);
}

TEST(CuckooHashTest, SimpleDuplicateKeyInsertion) {
  CuckooHashMap map(test_bucket_size);

  uint32_t bucket_size = 1 << test_bucket_size;
  uint32_t total_key_num = (1 << test_bucket_size) * kDefaultSlotPerBucket;
  uint32_t total_failed_found_key_num = 0;

  for (uint32_t i = 0; i < total_key_num; ++i) {
    bool res = map.insert(i % bucket_size, i);
    EXPECT_TRUE(res);
    if (res == false) {
      std::cout << "Failed insert key num: " << i % bucket_size << std::endl;
    }
  }

  for (uint32_t i = 0; i < total_key_num; ++i) {
    uint64_t val;
    bool found = map.find(i, val);
    // EXPECT_TRUE(found);
    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
      ++total_failed_found_key_num;
    } else {
      EXPECT_EQ(i, val);
    }
  }

  EXPECT_EQ(map.loadfactor(), 1);
}

TEST(CuckooHashTest, ExceedSlotDuplicateKeyInsertion) {
  CuckooHashMap map(test_bucket_size);

  uint32_t bucket_size = 1 << test_bucket_size;
  uint32_t total_key_num = (1 << test_bucket_size) * kDefaultSlotPerBucket * 2;

  for (uint32_t i = 0; i < total_key_num; ++i) {
    bool res = map.insert(i % bucket_size, i);
    EXPECT_TRUE(res);
    if (res == false) {
      std::cout << "Failed insert key num: " << i % bucket_size << std::endl;
    }
  }

  for (uint32_t i = 0; i < (1 << test_bucket_size) * kDefaultSlotPerBucket;
       ++i) {
    uint64_t val;
    bool found = map.find(i, val);
    EXPECT_FALSE(found);
    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(i, val);
    }
  }

  for (uint32_t i = (1 << test_bucket_size) * kDefaultSlotPerBucket;
       i < total_key_num; ++i) {
    uint64_t val;
    bool found = map.find(i, val);
    EXPECT_TRUE(found);
    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(i, val);
    }
  }

  EXPECT_EQ(map.loadfactor(), 1);
}

TEST(CuckooHashTest, ExceedSlotRandomKeyInsertion) {
  uint32_t bucket_size = 1 << 5;
  uint32_t total_slot_size = bucket_size * kDefaultSlotPerBucket;
  uint32_t total_key_num = total_slot_size * 2;

  CuckooHashMap map(5);

  // generalize random key
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

  for (uint32_t i = 0; i < total_key_num; ++i) {
    bool res = map.insert(vec[i] % bucket_size, vec[i]);
    EXPECT_TRUE(res);
    if (res == false) {
      std::cout << "Failed insert key num: " << vec[i] % bucket_size
                << std::endl;
    }
  }

  for (uint32_t i = 0; i < total_slot_size; ++i) {
    uint64_t val;
    bool found = map.find(vec[i], val);
    // EXPECT_FALSE(found);
    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(vec[i], val);
    }
  }

  for (uint32_t i = total_slot_size; i < total_key_num; ++i) {
    uint64_t val;
    bool found = map.find(vec[i], val);
    // EXPECT_TRUE(found);
    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(vec[i], val);
    }
  }

  EXPECT_EQ(map.loadfactor(), 1);
}

TEST(CuckooHashTest, PressureTest) {
  uint32_t bucket_size = 1 << 8;
  uint32_t total_slot_size = bucket_size * kDefaultSlotPerBucket;
  uint32_t total_key_num = total_slot_size * 256;

  CuckooHashMap map(8);

  // generalize random key
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

  for (uint32_t i = 0; i < total_key_num; ++i) {
    bool res = map.insert(vec[i] % bucket_size, vec[i]);
    EXPECT_TRUE(res);
    if (res == false) {
      std::cout << "Failed insert key num: " << vec[i] % bucket_size
                << std::endl;
    }
  }

  for (uint32_t i = 0; i < total_slot_size; ++i) {
    uint64_t val;
    bool found = map.find(vec[i], val);
    // EXPECT_FALSE(found);
    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(vec[i], val);
    }
  }

  for (uint32_t i = total_slot_size; i < total_key_num; ++i) {
    uint64_t val;
    bool found = map.find(vec[i], val);
    // EXPECT_TRUE(found);
    if (found == false) {
      std::cout << "Failed found key num: " << i << std::endl;
    } else {
      EXPECT_EQ(vec[i], val);
    }
  }

  EXPECT_EQ(map.loadfactor(), 1);
}