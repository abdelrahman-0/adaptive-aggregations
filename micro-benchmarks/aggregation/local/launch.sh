TARGET_PATH='../../../build-release/micro-benchmarks/aggregation/aggregation_local'

FLAGS="--random --morselsz=1000 --pin"
PRINT_HEADER='--print_header'

for HTF in 2; do
  for NPAGES in 2400000; do
    for NPARTS in 64; do
      for NGROUPS in 10 1000 100000 10000000; do
        for THREADS in 16 12 32; do
          for TRY in $(seq 1 3); do
            $TARGET_PATH $PRINT_HEADER $FLAGS --npages=$NPAGES --partitions=$NPARTS --groups=$NGROUPS --threads=$THREADS --htfactor=$HTF >/dev/null
            PRINT_HEADER='--noprint_header'
          done
        done
      done
    done
  done
done
