TARGET_PATH='../../build-release/micro-benchmarks/shuffle/shuffle_homogeneous'
LOCAL_NODE_ID="${NODE_ID:-0}"

NODES=8
FLAGS="--nolocal --random --npages=2500000 --nodes=${NODES}"
PRINT_HEADER='--print_header'

for THREADS in 1 2 4 8 16 32 64;
 do
  for MORSEL_SZ in 1 10 100;
   do
    $TARGET_PATH $PRINT_HEADER $FLAGS --threads=$THREADS --morselsz=$MORSEL_SZ > /dev/null
    sleep 1s
    if [[ "${LOCAL_NODE_ID}" == 0 ]]; then
      sleep 1s
    fi
    PRINT_HEADER='--noprint_header'
   done
done
