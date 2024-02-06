#include "utils/QuotientIndex.h"

#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <vector>

#include "candidate.hpp"
#include "stats/stats.hpp"

#define LOW_MASK(n) ((1ull << (n)) - 1ull)

QuotientIndex::QuotientIndex(PowerType q)
    : indexMask_(LOW_MASK(q)), maxNumEntries_(1ull << q), numEntries_(0) {
  index_.resize(maxNumEntries_);
}

bool QuotientIndex::insert(SizeType offset, candidate_t item) {
  if (numEntries_ >= maxNumEntries_) {
    return false;
  }
  item.hit_count = 0;

  SizeType fq = offset;
  TagType fr = item.id;
  Entry oldFqEntry = index_[fq];
  Entry entry(item, 1, false, false, false);

  if (oldFqEntry.isEmpty()) {
    index_[fq].setEntry(item, 1, true, false, false);
    ++numEntries_;
    return true;
  }

  if (!index_[fq].isOccupied()) {
    index_[fq].setOccupied();
  }

  SizeType oldStartIdx = findRunStartIndex(fq);
  SizeType s = oldStartIdx;

  if (oldFqEntry.isOccupied()) {
    // move the cursor to the insert position in the fq run.
    do {
      TagType currRemainder = index_[s].tag();
      if (currRemainder == fr) {
        return true;
      } else if (currRemainder > fr) {
        break;
      }

      s = incr(s);
    } while (index_[s].isContinuation());

    if (s == oldStartIdx) {
      // old start of run becomes a continuation
      index_[oldStartIdx].setContinuation();
    } else {
      // new element is a continuation
      entry.setContinuation();
    }
  }

  // Set the shifted bit if we can't use the canonical slot
  if (s != fq) {
    entry.setShifted();
  }

  // Insert hk into QF[s], shifting over elements as necessary.
  Entry prev;
  Entry curr = entry;

  bool prevEmpty;
  do {
    prev = index_[s];
    prevEmpty = prev.isEmpty();
    if (!prevEmpty) {
      // swap prev and curr
      prev.setShifted();
      if (prev.isOccupied()) {
        curr.setOccupied();
        prev.clrOccupied();
      }
    }

    index_[s] = curr;
    curr = prev;
    s = incr(s);
  } while (!prevEmpty);

  ++numEntries_;
  return true;
}

// Return the cost of 1 find operation, -1 for failure
bool QuotientIndex::find(SizeType offset, candidate_t item) {
  SizeType fq = offset;
  TagType fr = item.id;

  if (!index_[fq].isOccupied() || numEntries_ == 0) {
    return false;
  }

  SizeType s = findRunStartIndex(fq);
  TagType remainder;
  do {
    remainder = index_[s].tag();
    if (remainder == fr) {
      break;
    }
    s = incr(s);
  } while (index_[s].isContinuation());

  if (remainder == fr) {
    index_[s].incrHits();  // update hit count
    return true;
  } else {
    return false;
  }
}

std::vector<candidate_t> QuotientIndex::removeAll() {
  std::vector<candidate_t> evicted;

  for (auto &item : index_) {
    if (item.isValid()) {
      evicted.push_back(item.item());
      item.clearEntry();
    }
  }

  numEntries_ = 0;
  return evicted;
}

// void QuotientIndex::remove(std::vector<candidate_t> evicted) {
//   auto sort_fn = [](candidate_t v1, candidate_t v2) { return v1.id > v2.id;
//   }; std::sort(evicted.begin(), evicted.end(), sort_fn);

//   for (auto &item : evicted) {
//     SizeType fq = getQuotientIndex(item.id);
//     TagType fr = item.id;

//     if (!index_[fq].isOccupied() || numEntries_ == 0) {
//       continue;
//     }

//     SizeType s = findRunStartIndex(fq);
//     TagType remainder;

//     // Find the offending table index (or give up).
//     do {
//       remainder = index_[s].tag();
//       if (remainder >= fr) {
//         break;
//       }

//       s = incr(s);
//     } while (index_[s].isContinuation());

//     assert(remainder == fr);
//     if (remainder != fr) {
//       continue;
//     }

//     deleteEntry(s, fq);

//     --numEntries_;
//   }
// }

// std::vector<candidate_t> QuotientIndex::getCluster(uint64_t key) {
//   SizeType fq = getQuotientIndex(key);
//   SizeType b = fq;
//   while (index_[b].isShifted()) {
//     b = decr(b);
//   }

//   SizeType s = b;
//   std::vector<candidate_t> res;
//   while (!index_[s].isEmpty()) {
//     res.push_back(index_[s].item());
//     ++s;
//   }
//   return res;
// }

QuotientIndex::SizeType QuotientIndex::decr(SizeType idx) {
  return ((uint64_t)idx - 1) & indexMask_;
}

QuotientIndex::SizeType QuotientIndex::incr(SizeType idx) {
  return ((uint64_t)idx + 1) & indexMask_;
}

QuotientIndex::SizeType QuotientIndex::findRunStartIndex(
    QuotientIndex::SizeType fq) {
  // Find the start of the cluster
  SizeType b = fq;
  while (index_[b].isShifted()) {
    b = decr(b);
  }

  // Find the start of the run for fq.
  SizeType s = b;
  while (b != fq) {
    do {
      s = incr(s);
    } while (index_[s].isContinuation());

    do {
      b = incr(b);
    } while (!index_[b].isOccupied());
  }

  return s;
}

// remove the entry in QF[s] and slide the rest of the cluster forward
// idx is the slot to remove
void QuotientIndex::deleteEntry(SizeType idx, SizeType fq) {
  bool replaceRunStart = index_[idx].isRunStart();

  // If we're deleting the last entry in a run, clear `is_occupied`
  if (replaceRunStart) {
    if (!index_[incr(idx)].isContinuation()) {
      index_[fq].clrOccupied();
    }
  }

  // Move the subsequent entry to left
  SizeType s = idx;
  SizeType sp = incr(s);
  Entry next;
  Entry curr = index_[s];
  while (true) {
    next = index_[sp];

    if (next.isEmpty() || next.isClusterStart() || sp == idx) {
      // move complete, next will not move to curr
      index_[s].clearEntry();
      break;
    } else {
      Entry updatedCurr = next;
      if (next.isRunStart()) {
        // Fix entries which slide back into canonical slots
        do {
          fq = incr(fq);
        } while (!index_[fq].isOccupied());

        if (curr.isOccupied() && fq == s) {
          updatedCurr.clrShifted();
        }
      }

      // Move don't change occupied flag
      if (curr.isOccupied()) {
        updatedCurr.setOccupied();
      } else {
        updatedCurr.clrOccupied();
      }
      index_[s] = updatedCurr;
      s = sp;
      sp = incr(sp);
      curr = next;
    }
  }

  // After deletion slot idx is a new start
  if (replaceRunStart) {
    if (index_[idx].isContinuation()) {
      index_[idx].clrContinuation();
    }
    if (idx == fq && index_[idx].isRunStart()) {
      index_[idx].clrShifted();
    }
  }
}