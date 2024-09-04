TARGET_PATH='./cmake-build-release/micro-benchmarks/shuffle/shuffle_homogeneous'
NODES=2

for THREADS in 1 2 4 8 16 32 64;
 do
  for MORSEL_SZ in 1 10 100;
   do
    $TARGET_PATH --local --random --npages=50000 --nodes=$NODES --threads=$THREADS --morselsz=$MORSEL_SZ > /dev/null
   done
done
