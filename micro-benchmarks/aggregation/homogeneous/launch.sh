TARGET_PATH='../../../build-release/micro-benchmarks/aggregation/aggregation_homogeneous'
LOCAL_NODE_ID="${NODE_ID:-0}"

MAX_NODES=4
FLAGS="--nolocal --random --morselsz=100 --pin --config=../../../configs/config_aws.json"
# --seed=${LOCAL_NODE_ID}
PRINT_HEADER='--print_header'

for NODES in $(seq $MAX_NODES -1 $(($LOCAL_NODE_ID + 1))); do
  #  if [[ "${NODES}" == 3 ]]; then
  #    continue
  #  fi
  for NPAGES in 2400000; do
    for THREADS in 1 4 6 12 24 32; do
      for NGROUPS in 1 100 100000 1000000 10000000; do
        for NPARTS in 32 64 96 128 256; do
          for PRTGRPSZ in 1 4 8; do
            for TRY in $(seq 1 3); do
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
