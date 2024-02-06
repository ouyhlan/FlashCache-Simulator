#include <cstdint>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "utils/QuotientFilter.h"
#include "utils/QuotientFilterEntry.h"

using namespace std;
vector<pair<string, uint16_t>> s1 = {
    {"i", 0b001000}, {"i", 0b001001}, {"i", 0b011010}, {"i", 0b011011},
    {"i", 0b011100}, {"i", 0b100101}, {"i", 0b110110}, {"i", 0b110111},
    {"d", 0b100101}, {"d", 0b011010}};

vector<uint32_t> r1 = {0b000100000000000000000000, 0b000100011000000000000000,
                       0b000100011100000000000000, 0b000100011100011000000000,
                       0b000100011100011011000000, 0b000100011100111011001000,
                       0b000100011100111011101001, 0b011100011100111011101001,
                       0b000100011100011011100011, 0b000100011100011000100011};

int main() {
  QuotientFilter qf(3, 3);

  for (auto c : s1) {
    cout << c.first << " " << hex << c.second << endl;
  }

  uint64_t i = 0;
  for (auto c : s1) {
    // op
    if (c.first == "i") {
      qf.insert(c.second);
    } else if (c.first == "d") {
      qf.remove(c.second);
    }

    // check status
    uint32_t correct_status = 0;
    uint32_t curr_status = r1[i++];
    for (int64_t j = 7; j >= 0; j--) {
      correct_status = qf.getIndex()[j].isOccupied();
      correct_status =
          (correct_status << 1) | qf.getIndex()[j].isContinuation();
      correct_status = (correct_status << 1) | qf.getIndex()[j].isShifted();
      if (correct_status == (curr_status & 0b111)) {
        cout << "Test #" << i << " Status #" << j << " pass" << endl;
      } else {
        cout << "Test #" << i << " Status #" << j << " failed" << endl;
        break;
      }

      curr_status >>= 3;
    }
  }

  return 0;
}