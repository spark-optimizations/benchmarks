package org.so.benchmark.bj

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.neu.so.bj.BroadcastJoin
import org.neu.so.bj.RDDSizeEstimator
import org.so.benchmark.util.{SparkUtil, TestUtil}

import scala.reflect.ClassTag

/**
  * @author Tirthraj
  */
object BroadcastJoinSuite {
  val ss: SparkSession = SparkUtil.createSparkSession()
  val sc: SparkContext = SparkUtil.createSparkContext(ss)

  def main(args: Array[ String ]): Unit = {
    val testData = fetchTestData(args(0) + "similar_artists.csv.gz")
    val smallRDD = extractSmallRDD(testData, 2000)
    if(args(4).equals("Y"))
      leftSmallTestSuite(args, testData, smallRDD)
    else
      rightSmallTestSuite(args, testData, smallRDD)
  }

  def leftSmallTestSuite[K:ClassTag, V:ClassTag](args: Array[String],
                                                 testData: RDD[(K, V)],
                                                 smallRDD: RDD[(K, V)]): Unit = {
    for (i <- 1 to args(3).toInt) {
      println("--- " + i + " ---")
      println("\t--- left small: BJ ---")
      broadcastJoinExec(smallRDD, testData, args(1) + "ls_bj_" + i, args(2) + "ls_bj_exec", args(2) + "ls_bj_sizeEst")
      println("\t--- left small: shuffle ---")
      normalJoinExec(smallRDD, testData, args(1) + "ls_shuffle_" + i, args(2) + "ls_shuffle_exec")
      println("\t--- big: BJ ---")
      broadcastJoinExec(testData, testData, args(1) + "big_bj_l_" + i,
        args(2) + "big_bj_exec_l", args(2) + "big_bj_sizeEst_l")
      println("\t--- big: shuffle ---")
      normalJoinExec(testData, testData, args(1) + "big_shuffle_l_" + i, args(2) + "big_shuffle_exec_l")
    }
  }

  def rightSmallTestSuite[K:ClassTag, V:ClassTag](args: Array[String],
                                                  testData: RDD[(K, V)],
                                                  smallRDD: RDD[(K, V)]): Unit = {
    for (i <- 1 to args(3).toInt) {
      println("--- " + i + " ---")
      println("\t--- right small: BJ ---")
      broadcastJoinExec(testData, smallRDD, args(1) + "rs_bj_" + i, args(2) + "rs_bj_exec", args(2) + "rs_bj_sizeEst")
      println("\t--- right small: shuffle ---")
      normalJoinExec(testData, smallRDD, args(1) + "rs_shuffle_" + i, args(2) + "rs_shuffle_exec")
      println("\t--- big: BJ ---")
      broadcastJoinExec(testData, testData, args(1) + "big_bj_r_" + i,
        args(2) + "big_bj_exec_r", args(2) + "big_bj_sizeEst_r")
      println("\t--- big: shuffle ---")
      normalJoinExec(testData, testData, args(1) + "big_shuffle_r_" + i, args(2) + "big_shuffle_exec_r")
    }
  }

  def extractSmallRDD[ K: ClassTag, V: ClassTag ](rdd: RDD[ (K, V) ], numOfRows: Int): RDD[ (K, V) ] = {
    sc.parallelize(rdd.take(numOfRows))
  }

  def fetchTestData[ K: ClassTag, V: ClassTag ](inputFile: String): RDD[ (String, AnyRef) ] = {
    sc.textFile(inputFile)
      .mapPartitionsWithIndex {
        case (0, iter) => iter.drop(1)
        case (_, iter) => iter
      }
      .map(line => (line.split(";")(0), line))
  }

  def broadcastJoinExec[ K: ClassTag, V: ClassTag ](rdd: RDD[ (K, V) ], smallRDD: RDD[ (K, V) ],
                                                    outputPath: String,
                                                    statsPath: String,
                                                    sizeEstStatsPath: String): Unit = {
    val bj = new BroadcastJoin(sc)
    bj.statsPath = sizeEstStatsPath
    TestUtil.timeBlock(
      bj.join(rdd, smallRDD, new RDDSizeEstimator {})
        .coalesce(1, shuffle = false)
        .saveAsTextFile(outputPath),
      statsPath
    )
  }

  def normalJoinExec[ K: ClassTag, V: ClassTag ](rdd: RDD[ (K, V) ], smallRDD: RDD[ (K, V) ],
                                                 outputPath: String,
                                                 statsPath: String): Unit = {
    TestUtil.timeBlock(
      rdd.join(smallRDD)
        .coalesce(1, shuffle = false)
        .saveAsTextFile(outputPath),
      statsPath
    )
  }
}
