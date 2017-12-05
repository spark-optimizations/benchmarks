OUT_ROOT=out
CLASSES_PATH=${OUT_ROOT}/classes
ARTIFACTS_PATH=${OUT_ROOT}/artifacts

JAR_NAME=${ARTIFACTS_PATH}/test.jar

LIB_PATH=lib-min
PLUGIN_JAR_NAME=${LIB_PATH}/build-plugin.jar

INPUT_PATH=input/
OUTPUT_PATH=${OUT_ROOT}/run_results/

METRICS_FILE=results/stats/timings.csv

NUM_ITER = 100

# Path to spark-submit executable
SPARK_SUBMIT = "/Users/manthanthakar/spark-2.2.0/bin/spark-submit"
SCALAC = "/Users/manthanthakar/scala-2.11.8/bin/scalac"

all: run

build_reg:
	(time -p $(SCALAC) -d ${CLASSES_PATH} \
		-cp "./${LIB_PATH}/*" \
		src/main/scala/org/so/benchmark/util/*.scala \
		src/main/scala/org/so/benchmark/plugin/*.scala \
		 ) 2>&1 | grep "real" | sed 's/^/reg	/' >> ${METRICS_FILE}
	jar cf ${JAR_NAME} \
    	-C ${CLASSES_PATH} .

build_plu:
	(time  -p $(SCALAC) -d ${CLASSES_PATH} \
		-cp "./${LIB_PATH}/*" \
		-Xplugin:${PLUGIN_JAR_NAME} \
		src/main/scala/org/so/benchmark/util/*.scala \
		src/main/scala/org/so/benchmark/plugin/*.scala \
         ) 2>&1 | grep "real" | sed 's/^/plugin	/' >> ${METRICS_FILE}
	jar cf ${JAR_NAME} \
    	-C ${CLASSES_PATH} .

run_reg: build_reg
	$(SPARK_SUBMIT) \
	 	--master local --driver-memory 5g \
    	--class org.so.benchmark.plugin.Main ${JAR_NAME} \
    	${INPUT_PATH} ${OUTPUT_PATH} ${METRICS_FILE} run_reg ${NUM_ITER}

run_plu: build_plu
	$(SPARK_SUBMIT) \
	 	--master local --driver-memory 5g \
    	--class org.so.benchmark.plugin.Main ${JAR_NAME}  \
          ${INPUT_PATH} ${OUTPUT_PATH} ${METRICS_FILE} run_plugin ${NUM_ITER}

run_diff: setup run_reg run_plu
	@echo "Running diff to validate outputs"
	@for f in $$(ls ${OUTPUT_PATH});do \
		diff -a -q ${OUTPUT_PATH}$$f/run_plugin/part-00000 ${OUTPUT_PATH}$$f/run_reg/part-00000; \
	done
	@echo "End diff to validate outputs"

run: setup run_diff

setup: clean
	@mkdir -p ${CLASSES_PATH}
	@mkdir -p ${ARTIFACTS_PATH}

clean:
	@rm -rf ${OUT_ROOT}

make_subset:
	cd input &&  \
	head -100001 all/similar_artists.csv > similar_artists.csv && \
	gzip -f similar_artists.csv