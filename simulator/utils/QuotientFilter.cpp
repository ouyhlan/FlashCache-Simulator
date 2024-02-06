#include "utils/QuotientFilter.h"

#include <cstdint>

#define LOW_MASK(n) ((1ull << (n)) - 1ull)

QuotientFilter::QuotientFilter(PowerType q, PowerType r)
    : qbits_(q),
      rbits_(r),
      indexMask_(LOW_MASK(q)),
      remainderMask_(LOW_MASK(r)),
      maxNumEntries_(1ull << q),
      numEntries_(0) {
  index_.resize(maxNumEntries_);
}

bool QuotientFilter::insert(uint64_t hk) {
  if (numEntries_ >= maxNumEntries_) {
    return false;
  }

  SizeType fq = getQuotientFromHash(hk);
  TagType fr = getRemainderFromHash(hk);
  Entry oldFqEntry = index_[fq];
  Entry entry(fr, 1, false, false, false);

  if (oldFqEntry.isEmpty()) {
    index_[fq].setEntry(fr, 1, true, false, false);
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

bool QuotientFilter::remove(uint64_t hk) {
  SizeType fq = getQuotientFromHash(hk);
  TagType fr = getRemainderFromHash(hk);

  if (!index_[fq].isOccupied() || numEntries_ == 0) {
    return true;
  }

  SizeType s = findRunStartIndex(fq);
  TagType remainder;

  // Find the offending table index (or give up).
  do {
    remainder = index_[s].tag();
    if (remainder == fr) {
      break;
    } else if (remainder > fr) {
      return true;
    }
    s = incr(s);
  } while (index_[s].isContinuation());
  if (remainder != fr) {
    return true;
  }

  deleteEntry(s, fq);

  --numEntries_;
  return true;
}

QuotientFilter::SizeType QuotientFilter::getQuotientFromHash(uint64_t hk) {
  return (hk >> rbits_) & indexMask_;
}

QuotientFilter::TagType QuotientFilter::getRemainderFromHash(uint64_t hk) {
  return hk & remainderMask_;
}

QuotientFilter::SizeType QuotientFilter::decr(SizeType idx) {
  return ((uint64_t)idx - 1) & indexMask_;
}

QuotientFilter::SizeType QuotientFilter::incr(SizeType idx) {
  return ((uint64_t)idx + 1) & indexMask_;
}

QuotientFilter::SizeType QuotientFilter::findRunStartIndex(
    QuotientFilter::SizeType fq) {
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
void QuotientFilter::deleteEntry(SizeType idx, SizeType fq) {
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