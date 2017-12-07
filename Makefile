OUT_ROOT=out_bj
CLASSES_PATH=${OUT_ROOT}/classes
ARTIFACTS_PATH=${OUT_ROOT}/artifacts

JAR_NAME=${ARTIFACTS_PATH}/bj_benchmark.jar
LIB_PATH=lib-min

INPUT_PATH=input/
OUTPUT_PATH=output/results/
STATS_PATH=output/stats/

SMALL_INPUT_FILE=${INPUT_PATH}/similar_artists_small.csv.gz
BIG_INPUT_FILE=${INPUT_PATH}/similar_artists_big.csv.gz

INPUT_FILE=${SMALL_INPUT_FILE}
IS_SMALL_CONST=N
ITERATIONS=5

all: build run

build: setup
	@mkdir -p "${CLASSES_PATH}/main/resources/"
	@mkdir -p ${STATS_PATH}
	@${SCALA_BIN_PATH}scalac -cp "./${LIB_PATH}/*" \
		-d ${CLASSES_PATH} \
		src/main/scala/org/so/benchmark/util/*.scala \
		src/main/scala/org/so/benchmark/bj/*.scala
	@jar cf ${JAR_NAME} \
		-C ${CLASSES_PATH} .

run:
	@${SPARK_PATH}spark-submit \
		--jars "./${LIB_PATH}/broadcast-join.jar" \
		--packages net.liftweb:lift-json_2.11:3.1.1 \
		--master local --driver-memory 6g \
		--class org.so.benchmark.bj.BroadcastJoinSuite ${JAR_NAME} \
		${INPUT_FILE} ${OUTPUT_PATH} ${STATS_PATH} ${ITERATIONS} ${IS_SMALL_CONST}

setup: clean
	@rm -rf ${OUTPUT_PATH}
	@mkdir -p ${CLASSES_PATH}
	@mkdir -p ${ARTIFACTS_PATH}

clean:
	@rm -rf ${OUT_ROOT}
	@rm -rf ${OUTPUT_PATH}
	@rm -rf ${STATS_PATH}
