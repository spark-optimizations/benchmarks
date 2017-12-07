package org.so.benchmark.bj

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.so.benchmark.util._

import scala.reflect.ClassTag

/**
  * @author Tirthraj
  */
object BroadcastJoinSuite {
  val ss: SparkSession = SparkUtil.createSparkSession()
  val sc: SparkContext = SparkUtil.createSparkContext(ss)
  val shuffStats: ShuffleStats = new ShuffleStats

  def main(args: Array[ String ]): Unit = {
    if("Y".equals(args(4)))
      variableBigRDDTS(args)
    else if("N".equals(args(4)))
      variableSmallRDDTS(args)
    else
      variableSmallRDDTS(args)
      variableBigRDDTS(args)
  }

  def variableSmallRDDTS(args: Array[ String ]): Unit = {
    val dg: DataLoadGenerator = new DataLoadGenerator(sc)
    // 2^9 * 1 mb iterations = 512 mb load
    val bigRDD = dg.createLoad(args(0), 10, isLoadSmall = true)
    // 2^0 to 2^8 * 2 mb iterations
    for (i <- 1 to 9) {
      println("** " + i + " **")
      val mulFactor = (4096 * Math.pow(2, i)).toInt
      config.autoBroadcastJoinThreshold = mulFactor * 256
      val smallRDD = extractSmallRDD(bigRDD, mulFactor)

      for (iteration <- 1 to args(3).toInt) {
        println("--- " + iteration + " ---")
        rightSmallTestSuite(args(1) + "vs_", args(2) + "vs_", bigRDD, smallRDD, i, iteration)
        bothBigTestSuite(args(1) + "vs_", args(2) + "vs_", bigRDD, i, iteration)
      }
    }
  }

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
        rightSmallTestSuite(args(1) + "vb_", args(2) + "vb_", bigRDD, smallRDD, i, iteration)
        bothBigTestSuite(args(1) + "vb_", args(2) + "vb_", bigRDD, i, iteration)
      }
    }
  }

  def bothBigTestSuite[ K: ClassTag, V: ClassTag ](outputPath: String,
                                                   statsPath: String,
                                                   rdd: RDD[ (K, V) ],
                                                   sizePower: Int,
                                                   iteration: Int): Unit = {
    val joinExec = new JoinExec(sc)

    println("\t--- Big: BJ ---")
    val t1 = joinExec.broadcastJoinExec(rdd, rdd, outputPath + "big_bj_" + iteration, statsPath + "big_bj_sizeEst")
    TestUtil.dumpStat(iteration + ";" + t1 + ";" + sizePower, statsPath + "big_bj_exec")

    println("\t--- Big: shuffle ---")
    shuffStats.shuffleBytes(sc.applicationId, "localhost")
    val t2 = joinExec.shuffleJoinExec(rdd, rdd, outputPath + "big_shuffle_" + iteration)
    val sb = shuffStats.shuffleBytes(sc.applicationId, "localhost")
    TestUtil.dumpStat(iteration + ";" + t2 + ";" + sizePower, statsPath + "big_shuffle_exec")
    TestUtil.dumpStat(iteration + ";" + sb + ";" + sizePower, statsPath + "big_shuffle_bytes")

    Util.deleteRecursively(new File(outputPath))
  }

  def rightSmallTestSuite[ K: ClassTag, V: ClassTag ](outputPath: String,
                                                      statsPath: String,
                                                      bigRDD: RDD[ (K, V) ],
                                                      smallRDD: RDD[ (K, V) ],
                                                      sizePower: Int,
                                                      iteration: Int): Unit = {
    val joinExec = new JoinExec(sc)

    println("\t--- right small: BJ ---")
    val t1 = joinExec.broadcastJoinExec(bigRDD, smallRDD, outputPath + "rs_bj_" + iteration, statsPath + "rs_bj_sizeEst")
    TestUtil.dumpStat(iteration + ";" + t1 + ";" + sizePower, statsPath + "rs_bj_exec")

    println("\t--- right small: shuffle ---")
    shuffStats.shuffleBytes(sc.applicationId, "localhost")
    val t2 = joinExec.shuffleJoinExec(bigRDD, smallRDD, outputPath + "rs_shuffle_" + iteration)
    val sb = shuffStats.shuffleBytes(sc.applicationId, "localhost")
    TestUtil.dumpStat(iteration + ";" + t2 + ";" + sizePower, statsPath + "rs_shuffle_exec")
    TestUtil.dumpStat(iteration + ";" + sb + ";" + sizePower, statsPath + "rs_shuffle_bytes")

    Util.deleteRecursively(new File(outputPath))
  }

  def extractSmallRDD[ K: ClassTag, V: ClassTag ](rdd: RDD[ (K, V) ], numOfRows: Int): RDD[ (K, V) ] = {
    sc.parallelize(rdd.take(numOfRows))
  }

  def leftSmallTestSuite[ K: ClassTag, V: ClassTag ](outputPath: String,
                                                     statsPath: String,
                                                     bigRDD: RDD[ (K, V) ],
                                                     smallRDD: RDD[ (K, V) ],
                                                     sizePower: Int,
                                                     iteration: Int): Unit = {
    val joinExec = new JoinExec(sc)

    println("\t--- left small: BJ ---")
    val t1 = joinExec.broadcastJoinExec(smallRDD, bigRDD, outputPath + "ls_bj_" + iteration, statsPath + "ls_bj_sizeEst")
    TestUtil.dumpStat(iteration + ";" + t1 + ";" + sizePower, outputPath + "ls_bj_exec")


    println("\t--- left small: shuffle ---")
    shuffStats.shuffleBytes(sc.applicationId, "localhost")
    val t2 = joinExec.shuffleJoinExec(smallRDD, bigRDD, outputPath + "ls_shuffle_" + iteration)
    val sb = shuffStats.shuffleBytes(sc.applicationId, "localhost")
    TestUtil.dumpStat(iteration + ";" + t2 + ";" + sizePower, statsPath + "ls_shuffle_exec")
    TestUtil.dumpStat(iteration + ";" + sb + ";" + sizePower, statsPath + "ls_shuffle_bytes")

    Util.deleteRecursively(new File(outputPath))
  }

  def fetchBigRDD[ K: ClassTag, V: ClassTag ](inputFile: String): RDD[ (String, AnyRef) ] = {
    sc.textFile(inputFile)
      .mapPartitionsWithIndex {
        case (0, iter) => iter.drop(1)
        case (_, iter) => iter
      }
      .map(line => (line.split(";")(0), line))
  }
}
