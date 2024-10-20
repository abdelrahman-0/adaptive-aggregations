TARGET_PATH='../../build-release/micro-benchmarks/shuffle/groupby_homogeneous'
LOCAL_NODE_ID="${NODE_ID:-0}"

MAX_NODES=4
FLAGS="--nolocal --random --npages=1000000 --morselsz=10 --pin"
PRINT_HEADER='--print_header'

for NODES in $(seq $MAX_NODES -1 $(($LOCAL_NODE_ID + 1))); do
  for THREADS in 1 4 6 12 24 30 32; do
    for NGROUPS in 1 10 100; do
      echo $THREADS $GROUPS
      for TRY in $(seq 1 5); do
        if [[ "${LOCAL_NODE_ID}" == 0 ]]; then
          sleep 1s
        fi
        $TARGET_PATH $PRINT_HEADER $FLAGS --nodes=$NODES --threads=$THREADS --groups=$NGROUPS >/dev/null
        sleep 1s
        PRINT_HEADER='--noprint_header'
      done
    done
  done
done
