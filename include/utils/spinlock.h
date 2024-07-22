#pragma once

struct SpinLock {
    std::atomic<bool> locked{false};
    void lock() {
        bool mutex_locked = false;
        while (!locked.compare_exchange_strong(mutex_locked, true)) {
            mutex_locked = false;
        }
    }

    void unlock() { locked.store(false, std::memory_order_relaxed); };
};