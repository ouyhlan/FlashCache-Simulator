#pragma once

#include <cstdint>

#include "candidate.hpp"
class __attribute__((__packed__)) QuotientIndexEntry {
 public:
  QuotientIndexEntry()
      : valid_(0), occupied_(0), continuation_(0), shifted_(0){};
  QuotientIndexEntry(const candidate_t &item, uint8_t hits, bool occupied,
                     bool continuation, bool shifted) {
    setEntry(item, hits, occupied, continuation, shifted);
  }

  void setEntry(const candidate_t &item, uint8_t hits, bool occupied,
                bool continuation, bool shifted) {
    item_ = item;
    valid_ = 1;
    hits_ = hits;
    occupied_ = occupied;
    continuation_ = continuation;
    shifted_ = shifted;
  }

  void clearEntry() {
    valid_ = 0;
    hits_ = 0;
    occupied_ = 0;
    continuation_ = 0;
    shifted_ = 0;
  }

  void incrHits() {
    if (hits_ < 8) {
      hits_++;
    }
    item_.hit_count++;
  }

  uint64_t tag() { return item_.id; }
  candidate_t item() { return item_; }
  bool isValid() { return valid_; }
  bool isOccupied() { return occupied_; }
  void setOccupied() { occupied_ = 1; }
  void clrOccupied() { occupied_ = 0; }
  bool isContinuation() { return continuation_; }
  void setContinuation() { continuation_ = 1; }
  void clrContinuation() { continuation_ = 0; }
  bool isShifted() { return shifted_; }
  void setShifted() { shifted_ = 1; }
  void clrShifted() { shifted_ = 0; }
  bool isEmpty() { return !(occupied_ || continuation_ || shifted_); }
  bool isRunStart() { return !continuation_ && (occupied_ || shifted_); }
  bool isClusterStart() { return occupied_ && !continuation_ && !shifted_; }

 private:
  candidate_t item_;

  uint16_t valid_ : 1;
  uint16_t hits_ : 3;
  uint16_t occupied_ : 1;
  uint16_t continuation_ : 1;
  uint16_t shifted_ : 1;
};