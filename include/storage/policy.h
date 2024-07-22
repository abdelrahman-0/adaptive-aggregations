#pragma once

#include <sys/sysinfo.h>

class PolicyAlwaysSpill {
  public:
    static bool spill() { return true; }
};

class PolicyNeverSpill {
  public:
    static bool spill() { return false; }
};

static bool spilled = false;
class PolicyMainMemoryFullSpill {
  public:
    static bool spill() {
        if (spilled) {
            return true;
        }
        struct ::sysinfo mem_info {};
        ::sysinfo(&mem_info);
        if (mem_info.freeram < 1'000'000'000) {
            spilled = true;
            return true;
        }
        return false;
    }
};