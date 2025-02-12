TARGET_PATH='../../../build-release/micro-benchmarks/aggregation/aggregation_homogeneous'
LOCAL_NODE_ID="${NODE_ID:-0}"

MAX_NODES=4
FLAGS="--nolocal --random --morselsz=1000 --pin --config=../../../configs/config_aws_4_workers.json"
# --seed=${LOCAL_NODE_ID}
PRINT_HEADER='--print_header'

for NODES in $(seq $MAX_NODES -1 $(($LOCAL_NODE_ID + 1))); do
  if [[ "${NODES}" == 3 ]]; then
    continue
  fi
#  if [[ "${NODES}" == 2 ]]; then
#    continue
#  fi
  for NPAGES in 24000000; do
    for THREADS in 6 12 32; do
      for NGROUPS in 10 1000000 100000000; do
        for NPARTS in 64; do
          for PRTGRPSZ in 4; do
            for TRY in $(seq 1 1); do
              if [[ "${LOCAL_NODE_ID}" == 0 ]]; then
                sleep 2s
              fi
              $TARGET_PATH $PRINT_HEADER $FLAGS --nodes=$NODES --npages=$NPAGES --threads=$THREADS --groups=$NGROUPS --partitions=$NPARTS --partgrpsz=$PRTGRPSZ >/dev/null
              sleep 2s
              PRINT_HEADER='--noprint_header'
            done
          done
        done
      done
    done
  done
done
