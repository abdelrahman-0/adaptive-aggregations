TARGET_PATH='../../../build-release/micro-benchmarks/aggregation/aggregation_local'

FLAGS="--nolocal --random --npages=1000000 --morselsz=10 --pin --partitions=32 --slots=8192"
PRINT_HEADER='--print_header'

for NGROUPS in 1 100 1000 10000 100000 1000000 10000000 100000000; do
  for THREADS in 1 4 6 12 24 32; do
    for TRY in $(seq 1 5); do
      $TARGET_PATH $PRINT_HEADER $FLAGS --groups=$NGROUPS --threads=$THREADS >/dev/null
      PRINT_HEADER='--noprint_header'
    done
  done
done
