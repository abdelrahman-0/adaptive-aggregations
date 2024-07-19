#pragma once

class PolicyAlwaysSpill {
public:
    static bool spill() {
        return true;
    }
};

class PolicyNeverSpill {
public:
    static bool spill() {
        return false;
    }
};

static bool spilled = false;
class PolicySpill {
public:
    static bool spill() {
        if(spilled){
            return true;
        }
        if(/* TODO check condition */ true){
            spilled = true;
            return true;
        }
        return false;
    }
};