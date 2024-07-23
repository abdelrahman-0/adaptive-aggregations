sudo apt-get -y update
sudo apt-get -y install g++ make cmake libgflags-dev liburing-dev libboost-all-dev

# oneTBB
$(
  cd /tmp &&
  git clone https://github.com/oneapi-src/oneTBB.git &&
  cd oneTBB &&
  mkdir build && cd build &&
  cmake -DTBB_TEST=OFF .. &&
  cmake --build . &&
  sudo cmake --install .
)

# install liburing from source (for multishot receive)

# install hwdata tool

# sudo dpkg --remove linux-intel-iotg-tools-common

make build