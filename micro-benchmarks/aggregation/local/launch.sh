TARGET_PATH='../../../build-release/micro-benchmarks/aggregation/aggregation_local'

FLAGS="--nolocal --random --morselsz=1000 --pin --partitions=32 --slots=8192"
PRINT_HEADER='--print_header'

for NPAGES in 2400000; do
  for NGROUPS in 1 100 10000 100000 1000000 10000000 100000000; do
    for THREADS in 1 4 6 12 24 32; do
      for TRY in $(seq 1 5); do
        $TARGET_PATH $PRINT_HEADER $FLAGS --npages=$NPAGES --groups=$NGROUPS --threads=$THREADS >/dev/null
        PRINT_HEADER='--noprint_header'
      done
    done
  done
done
