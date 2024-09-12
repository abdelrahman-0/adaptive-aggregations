TARGET_PATH='../../build-release/micro-benchmarks/shuffle/shuffle_homogeneous'
LOCAL_NODE_ID="${NODE_ID:-0}"

MAX_NODES=4
FLAGS="--nolocal --random --npages=500000 --morselsz=10"
PRINT_HEADER='--print_header'

for NODES in $(seq $MAX_NODES -1 $(($LOCAL_NODE_ID+1)));
 do
  for THREADS in 1 4 8 12 16 32 64;
   do
    for BUFS_PER_PEER in 2 4 10;
     do
       for TRY in $(seq 1 5);
        do
          if [[ "${LOCAL_NODE_ID}" == 0 ]]; then
            sleep 1s
          fi
          $TARGET_PATH $PRINT_HEADER $FLAGS --nodes=$NODES --threads=$THREADS --bufs_per_peer=$BUFS_PER_PEER > /dev/null
          sleep 1s
          PRINT_HEADER='--noprint_header'
        done
     done
 done
done