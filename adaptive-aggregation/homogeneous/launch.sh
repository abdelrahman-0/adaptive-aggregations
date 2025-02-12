TARGET_PATH='../../build-release/adaptive-aggregation/worker_homogeneous'
LOCAL_NODE_ID="${NODE_ID:-0}"

MAX_NODES=4
FLAGS="--nolocal --random --morselsz=1000 --pin --config=../../configs/config_aws_4_workers.json --policy=static --static_workers=4"
PRINT_HEADER='--print_header'

for TIMEOUT in 1000; do
  for NPAGES in 24000000; do
    for THREADS in 6 12 32; do
      for NGROUPS in 10 1000000 100000000; do
        for NPARTS in 64; do
          for PRTGRPSZ in 4; do
            for TRY in $(seq 1 1); do
              if [[ "${LOCAL_NODE_ID}" == 0 ]]; then
                sleep 10s
              fi
              $TARGET_PATH $PRINT_HEADER $FLAGS --timeout=$TIMEOUT --nodes=$MAX_NODES --npages=$NPAGES --threads=$THREADS --groups=$NGROUPS --partitions=$NPARTS --partgrpsz=$PRTGRPSZ >/dev/null
              sleep 10s
              PRINT_HEADER='--noprint_header'
            done
          done
        done
      done
    done
  done
done
