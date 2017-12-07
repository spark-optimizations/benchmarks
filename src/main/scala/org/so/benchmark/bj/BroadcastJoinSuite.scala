package org.so.benchmark.bj

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.so.benchmark.util._

import scala.reflect.ClassTag

/**
  * Driver class executing entire Broadcast Join Test Suite.
  *
  * @author Tirthraj
  */
object BroadcastJoinSuite {
  val ss: SparkSession = SparkUtil.createSparkSession()
  val sc: SparkContext = SparkUtil.createSparkContext(ss)
  val shuffStats: ShuffleStats = new ShuffleStats

  /**
    * Entry point of Broadcast Join Test Suite. Based IS_SMALL_CONST is set `Y` or `N`
    * it performs respective action. For any other values of IS_SMALL_CONST it performs both kind of actions
    * sequentially.
    *
    * @param args Array of INPUT_FILE, OUTPUT_PATH, STATS_PATH, ITERATIONS, IS_SMALL_CONST
    */
  def main(args: Array[ String ]): Unit = {
    if ("Y".equals(args(4)))
      variableBigRDDTS(args)
    else if ("N".equals(args(4)))
      variableSmallRDDTS(args)
    else {
      variableSmallRDDTS(args)
      variableBigRDDTS(args)
    }
  }

  /**
    * Benchmark for variable small RDD size and fixed bid RDD size.
    * Small RDD Size: 8mb to 512mb on a scaled of power of 2
    * Big RDD Size: 512mb
    *
    * @param args Array of INPUT_FILE, OUTPUT_PATH, STATS_PATH, ITERATIONS, IS_SMALL_CONST
    */
  def variableSmallRDDTS(args: Array[ String ]): Unit = {
    val dg: DataLoadGenerator = new DataLoadGenerator(sc)
    // 2^9 * 1 mb iterations = 512 mb load
    val bigRDD = dg.createLoad(args(0), 10, isLoadSmall = true)
    // 2^2 to 2^8 * 2 mb iterations
    for (i <- 3 to 9) {
      println("** " + i + " **")
      val mulFactor = (4096 * Math.pow(2, i)).toInt
      config.autoBroadcastJoinThreshold = mulFactor * 256
      val smallRDD = extractSmallRDD(bigRDD, mulFactor)

      for (iteration <- 1 to args(3).toInt) {
        println("--- " + iteration + " ---")
        rightSmallTestSuite(args(2) + "vs_", bigRDD, smallRDD, i, iteration)
      }
    }
  }

  /**
    * Benchmark join of bigRDD.join(smallRDD) using broadcast join and shuffle join
    *
    * @param statsPath path to dump performance statistics
    * @param bigRDD    Big RDD which is on left side of the join
    * @param smallRDD  Small RDD which is on right side of the join
    * @param sizePower power of 2, which is multiplying factor of rdd size
    * @param iteration number representing iteration of the operation
    * @tparam K Key type for `rdd1` and `rdd2`
    * @tparam V Value type for `rdd1` and `rdd2`
    */
  def rightSmallTestSuite[ K: ClassTag, V: ClassTag ](statsPath: String,
                                                      bigRDD: RDD[ (K, V) ],
                                                      smallRDD: RDD[ (K, V) ],
                                                      sizePower: Int,
                                                      iteration: Int): Unit = {
    val joinExec = new JoinExec(sc)

    println("\t--- right small: BJ ---")
    val t1 = joinExec.broadcastJoinExec(bigRDD, smallRDD, statsPath + "rs_bj_sizeEst")
    TestUtil.dumpStat(iteration + ";" + t1 + ";" + sizePower, statsPath + "rs_bj_exec")

    println("\t--- right small: shuffle ---")
    shuffStats.shuffleBytes(sc.applicationId, "localhost")
    val t2 = joinExec.shuffleJoinExec(bigRDD, smallRDD)
    val sb = shuffStats.shuffleBytes(sc.applicationId, "localhost")
    TestUtil.dumpStat(iteration + ";" + t2 + ";" + sizePower, statsPath + "rs_shuffle_exec")
    TestUtil.dumpStat(iteration + ";" + sb + ";" + sizePower, statsPath + "rs_shuffle_bytes")
  }

  def extractSmallRDD[ K: ClassTag, V: ClassTag ](rdd: RDD[ (K, V) ], numOfRows: Int): RDD[ (K, V) ] = {
    sc.parallelize(rdd.take(numOfRows))
  }

  /**
    * Benchmark for variable big RDD size and fixed small RDD size.
    * Small RDD Size: 20mb = default broadcast join small rdd threshold
    * Big RDD Size: 256mb to 1gb on a scaled of power of 2
    *
    * @param args Array of INPUT_FILE, OUTPUT_PATH, STATS_PATH, ITERATIONS, IS_SMALL_CONST
    */
  def variableBigRDDTS(args: Array[ String ]): Unit = {
    // 2^0 to 2^5 * 256 mb iterations
    for (i <- 1 to 6) {
      println("** " + i + " **")
      val dg: DataLoadGenerator = new DataLoadGenerator(sc)
      val bigRDD = dg.createLoad(args(0), i)
      // 20971520/256 = 81920; Here 20971520 = 20mb threshold we have set for small rdd for BJ
      val smallRDD = extractSmallRDD(bigRDD, 81920)

      for (iteration <- 1 to args(3).toInt) {
        println("--- " + iteration + " ---")
        rightSmallTestSuite(args(2) + "vb_", bigRDD, smallRDD, i, iteration)
        bothBigTestSuite(args(2) + "vb_", bigRDD, i, iteration)
      }
    }
  }

  /**
    * Benchmark join of two big rdd using broadcast join and shuffle join
    *
    * @param statsPath path to dump performance statistics
    * @param rdd       Big RDD on which join is to be performed
    * @param sizePower power of 2, which is multiplying factor of rdd size
    * @param iteration number representing iteration of the operation
    * @tparam K Key type for `rdd1` and `rdd2`
    * @tparam V Value type for `rdd1` and `rdd2`
    */
  def bothBigTestSuite[ K: ClassTag, V: ClassTag ](statsPath: String,
                                                   rdd: RDD[ (K, V) ],
                                                   sizePower: Int,
                                                   iteration: Int): Unit = {
    val joinExec = new JoinExec(sc)

    println("\t--- Big: BJ ---")
    val t1 = joinExec.broadcastJoinExec(rdd, rdd, statsPath + "big_bj_sizeEst")
    TestUtil.dumpStat(iteration + ";" + t1 + ";" + sizePower, statsPath + "big_bj_exec")

    println("\t--- Big: shuffle ---")
    shuffStats.shuffleBytes(sc.applicationId, "localhost")
    val t2 = joinExec.shuffleJoinExec(rdd, rdd)
    val sb = shuffStats.shuffleBytes(sc.applicationId, "localhost")
    TestUtil.dumpStat(iteration + ";" + t2 + ";" + sizePower, statsPath + "big_shuffle_exec")
    TestUtil.dumpStat(iteration + ";" + sb + ";" + sizePower, statsPath + "big_shuffle_bytes")
  }

  /**
    * Benchmark join of smallRDD.join(bigRDD) using broadcast join and shuffle join
    *
    * @param statsPath path to dump performance statistics
    * @param bigRDD    Big RDD which is on right side of the join
    * @param smallRDD  Small RDD which is on left side of the join
    * @param sizePower power of 2, which is multiplying factor of rdd size
    * @param iteration number representing iteration of the operation
    * @tparam K Key type for `rdd1` and `rdd2`
    * @tparam V Value type for `rdd1` and `rdd2`
    */
  def leftSmallTestSuite[ K: ClassTag, V: ClassTag ](statsPath: String,
                                                     bigRDD: RDD[ (K, V) ],
                                                     smallRDD: RDD[ (K, V) ],
                                                     sizePower: Int,
                                                     iteration: Int): Unit = {
    val joinExec = new JoinExec(sc)

    println("\t--- left small: BJ ---")
    val t1 = joinExec.broadcastJoinExec(smallRDD, bigRDD, statsPath + "ls_bj_sizeEst")
    TestUtil.dumpStat(iteration + ";" + t1 + ";" + sizePower, statsPath + "ls_bj_exec")


    println("\t--- left small: shuffle ---")
    shuffStats.shuffleBytes(sc.applicationId, "localhost")
    val t2 = joinExec.shuffleJoinExec(smallRDD, bigRDD)
    val sb = shuffStats.shuffleBytes(sc.applicationId, "localhost")
    TestUtil.dumpStat(iteration + ";" + t2 + ";" + sizePower, statsPath + "ls_shuffle_exec")
    TestUtil.dumpStat(iteration + ";" + sb + ";" + sizePower, statsPath + "ls_shuffle_bytes")
  }
}
