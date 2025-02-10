TARGET_PATH='../../build-release/adaptive-aggregation/worker_homogeneous'
LOCAL_NODE_ID="${NODE_ID:-0}"

MAX_NODES=4
FLAGS="--nolocal --random --morselsz=1000 --pin --config=../../configs/config_aws_4_workers.json --policy=regression --static_workers=4"
PRINT_HEADER='--print_header'

for TIMEOUT in 150 250; do
  for NPAGES in 2400000; do
    for THREADS in 6 12 32; do
      for NGROUPS in 10 1000 100000 10000000; do
        for NPARTS in 64; do
          for PRTGRPSZ in 4; do
            for TRY in $(seq 1 3); do
              if [[ "${LOCAL_NODE_ID}" == 0 ]]; then
                sleep 5s
              fi
              $TARGET_PATH $PRINT_HEADER $FLAGS --timeout=$TIMEOUT --nodes=$MAX_NODES --npages=$NPAGES --threads=$THREADS --groups=$NGROUPS --partitions=$NPARTS --partgrpsz=$PRTGRPSZ >/dev/null
              sleep 5s
              PRINT_HEADER='--noprint_header'
            done
          done
        done
      done
    done
  done
done
