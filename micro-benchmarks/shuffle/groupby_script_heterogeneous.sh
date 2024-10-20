TARGET_PATH='../../build-release/micro-benchmarks/shuffle/groupby_heterogeneous'
LOCAL_NODE_ID="${NODE_ID:-0}"

MAX_NODES=4
FLAGS="--nolocal --random --npages=1000000 --morselsz=10 --pin"
PRINT_HEADER='--print_header'

for NODES in $(seq $MAX_NODES -1 $(($LOCAL_NODE_ID + 1))); do
  for qthreads_per_nthread in 1 2 3; do
    for THREADS in 4 6 12 24 30 32; do
      if [[ $(($THREADS % ($qthreads_per_nthread + 1))) -ne 0 ]]; then
        continue
      fi
      NTHREADS=$(($THREADS / ($qthreads_per_nthread + 1)))
      QTHREADS=$(($THREADS - $NTHREADS))
      for GROUPS in 1 10 100 1000 10000 100000 1000000; do
        for TRY in $(seq 1 5); do
          if [[ "${LOCAL_NODE_ID}" == 0 ]]; then
            sleep 1s
          fi
          $TARGET_PATH $PRINT_HEADER $FLAGS --nodes=$NODES --nthreads=$NTHREADS --qthreads=$QTHREADS --groups=$GROUPS >/dev/null
          sleep 1s
          PRINT_HEADER='--noprint_header'
        done
      done
    done
  done
done
