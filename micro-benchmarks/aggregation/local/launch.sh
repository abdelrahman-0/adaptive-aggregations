TARGET_PATH='../../../cmake-build-release/micro-benchmarks/aggregation/aggregation_local'

FLAGS="--nolocal --random --npages=1000000 --morselsz=10 --pin --partitions=16 --slots=8192"
PRINT_HEADER='--print_header'

for NGROUPS in 1 100 10000 1000000; do
  for THREADS in 1 4 6 12 24 32; do
    for TRY in $(seq 1 1); do
      $TARGET_PATH $PRINT_HEADER $FLAGS --groups=$NGROUPS --threads=$THREADS >/dev/null
      PRINT_HEADER='--noprint_header'
    done
  done
done
