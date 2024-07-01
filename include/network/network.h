#pragma once

#include "concepts.h"

template <Can_Send_Recv T>
class Network {
private:
public:
    thread_local static T comm;
};
