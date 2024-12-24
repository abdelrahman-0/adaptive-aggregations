TARGET_PATH='../../../build-release/micro-benchmarks/aggregation/aggregation_local'

FLAGS="--random --morselsz=1000 --pin --slots=8192"
PRINT_HEADER='--print_header'

for NPAGES in 2400000; do
  for NPARTS in 16 32 64 128 256; do
    for NGROUPS in 1 100 10000 100000 1000000 10000000; do
      for THREADS in 1 2 4 8 16 32; do
        for TRY in $(seq 1 3); do
          $TARGET_PATH $PRINT_HEADER $FLAGS --npages=$NPAGES --partitions=$NPARTS --groups=$NGROUPS --threads=$THREADS >/dev/null
          PRINT_HEADER='--noprint_header'
        done
      done
    done
  done
done
