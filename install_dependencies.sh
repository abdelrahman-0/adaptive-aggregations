#!/bin/bash

cd ~

sudo apt-get -y update
sudo apt-get -y install g++ make cmake mbw libgflags-dev libboost-all-dev iperf likwid linux-tools-6.5.0-1022-aws ncdu gdb bpfcc-tools linux-headers-$
(uname -r) libjemalloc-dev

# oneTBB
cd /tmp &&
git clone https://github.com/oneapi-src/oneTBB.git &&
cd oneTBB &&
mkdir build && cd build &&
cmake -DTBB_TEST=OFF .. &&
cmake --build . &&
sudo cmake --install .

# install liburing from source (for multishot receive)
cd ~
git clone https://github.com/axboe/liburing.git
cd liburing
./configure --cc=gcc --cxx=g++
make -j$(nproc)
sudo make install
cd ~

# install Apache's CPC sketch from source
git clone https://github.com/apache/datasketches-cpp.git
cd datasketches-cpp
cmake -S . -B build/Release -DCMAKE_BUILD_TYPE=Release
sudo cmake --build build/Release -t install
cd ~


# increase read and write socket buffers
sudo sysctl -w net.core.rmem_max=500000000
sudo sysctl -w net.core.wmem_max=500000000
sudo sysctl -p

# install hwdata tool
# sudo dpkg --remove linux-intel-iotg-tools-common
# echo 1 | sudo tee /proc/sys/kernel/sched_schedstats

