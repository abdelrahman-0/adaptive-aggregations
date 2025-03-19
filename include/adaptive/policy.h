#pragma once

namespace adapt::policy {
enum PolicyType { STATIC, REGRESSION };

struct Policy
{
    PolicyType type;
    u32 time_out;
    u16 workers;

    Policy(const std::string& name, u32 _time_out, u16 _workers = 0) : type(name == "static" ? STATIC : REGRESSION), time_out(_time_out), workers(_workers)
    {
    }
};
} // namespace adapt::policy
