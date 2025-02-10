TARGET_PATH='../build-release/adaptive-aggregation/coordinator'
LOCAL_NODE_ID="${NODE_ID:-0}"

MAX_NODES=4
FLAGS="--config=../configs/config_aws_4_workers.json"
PRINT_HEADER='--print_header'

for NPAGES in 2400000; do
  for THREADS in 6 12 32; do
    for NGROUPS in 10 1000 100000 10000000; do
      for NPARTS in 64; do
        for PRTGRPSZ in 4; do
          for TRY in $(seq 1 3); do
            $TARGET_PATH $PRINT_HEADER $FLAGS --nodes=$MAX_NODES --npages=$NPAGES
            sleep 1s
            PRINT_HEADER='--noprint_header'
          done
        done
      done
    done
  done
done
