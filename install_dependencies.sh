sudo apt-get -y update
sudo apt-get -y install cmake libgflags-dev liburing-dev libboost-all-dev

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