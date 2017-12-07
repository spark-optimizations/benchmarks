# Benchmarks: Broadcast Join

* Set `INPUT_FILE=${SMALL_INPUT_FILE}` and `IS_SMALL_CONST=N` to run benchmark for variable *small* rdd size.
* Set `INPUT_FILE=${BIG_INPUT_FILE}` and `IS_SMALL_CONST=Y` to run benchmark for variable *big* rdd size.
* Set number of iterations for each type of run with `ITERATIONS`
* run `make all`