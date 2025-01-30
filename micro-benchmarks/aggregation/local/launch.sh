TARGET_PATH='../../../build-release/micro-benchmarks/aggregation/aggregation_local'

FLAGS="--random --morselsz=1000 --pin"
PRINT_HEADER='--print_header'

for NPAGES in 2400000; do
  for NPARTS in 64; do
    for NGROUPS in 10 100000 10000000; do
      for THREADS in 8 16; do
        for TRY in $(seq 1 3); do
          $TARGET_PATH $PRINT_HEADER $FLAGS --npages=$NPAGES --partitions=$NPARTS --groups=$NGROUPS --threads=$THREADS >/dev/null
          PRINT_HEADER='--noprint_header'
        done
      done
    done
  done
done
