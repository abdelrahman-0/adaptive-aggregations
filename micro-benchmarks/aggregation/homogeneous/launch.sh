TARGET_PATH='../../../build-release/micro-benchmarks/aggregation/aggregation_homogeneous'
LOCAL_NODE_ID="${NODE_ID:-0}"

MAX_NODES=4
FLAGS="--nolocal --random --morselsz=1000 --pin"
PRINT_HEADER='--print_header'

for NODES in $(seq $MAX_NODES -1 $(($LOCAL_NODE_ID + 1))); do
  if [[ "${NODES}" == 3 ]]; then
    continue
  fi
  for NPAGES in 240000 2400000; do
    for THREADS in 1 4 6 12 24 30 32; do
      for NGROUPS in 1 100 10000 100000 1000000 10000000; do
        for TRY in $(seq 1 5); do
          if [[ "${LOCAL_NODE_ID}" == 0 ]]; then
            sleep 1s
          fi
          $TARGET_PATH $PRINT_HEADER $FLAGS --nodes=$NODES --npages=$NPAGES --threads=$THREADS --groups=$NGROUPS >/dev/null
          sleep 1s
          PRINT_HEADER='--noprint_header'
        done
      done
    done
  done
done
