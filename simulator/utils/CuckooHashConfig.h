#pragma once

#include <cstddef>
#include <cstdint>

// quotient filter subindex size
constexpr size_t kQFSubIndexSize = 16;

// The default maximum number of keys per bucket
constexpr size_t kDefaultSlotPerBucket = 4;

// maximum number of locks
constexpr size_t kMaxNumLocks = 1ul << 16;

// Cuckoo replace maximum times
constexpr uint8_t kMaxBFSPathLen = 5;