#pragma once

#include <cstdint>
class __attribute__((__packed__)) QuotientFilterEntry {
 public:
  QuotientFilterEntry() : valid_(0){};
  QuotientFilterEntry(uint32_t tag, uint8_t hits, bool occupied,
                      bool continuation, bool shifted) {
    setEntry(tag, hits, occupied, continuation, shifted);
  }

  void setEntry(uint32_t tag, uint8_t hits, bool occupied, bool continuation,
                bool shifted) {
    tag_ = tag;
    valid_ = 1;
    hits_ = hits;
    occupied_ = occupied;
    continuation_ = continuation;
    shifted_ = shifted;
  }

  void clearEntry() {
    tag_ = 0;
    valid_ = 0;
    hits_ = 0;
    occupied_ = 0;
    continuation_ = 0;
    shifted_ = 0;
  }

  uint32_t tag() { return tag_; }
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
  uint16_t tag_ : 9;
  uint16_t valid_ : 1;
  uint16_t hits_ : 3;
  uint16_t occupied_ : 1;
  uint16_t continuation_ : 1;
  uint16_t shifted_ : 1;
};