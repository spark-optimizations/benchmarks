package org.so.benchmark.bj

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.so.benchmark.util.SparkUtil

import scala.reflect.ClassTag

/**
  * @author Tirthraj
  */
object BroadcastJoinSuite {
  val ss: SparkSession = SparkUtil.createSparkSession()
  val sc: SparkContext = SparkUtil.createSparkContext(ss)

  def main(args: Array[ String ]): Unit = {
    val bigRDD = fetchBigRDD(args(0))
    val smallRDD = extractSmallRDD(bigRDD, 2000)
    if (args(4).equals("Y"))
      leftSmallTestSuite(args, bigRDD, smallRDD)
    else
      rightSmallTestSuite(args, bigRDD, smallRDD)
  }

  def leftSmallTestSuite[ K: ClassTag, V: ClassTag ](args: Array[ String ],
                                                     bigRDD: RDD[ (K, V) ],
                                                     smallRDD: RDD[ (K, V) ]): Unit = {
    val joinExec = new JoinExec(sc)
    for (i <- 1 to args(3).toInt) {
      println("--- " + i + " ---")

      println("\t--- left small: BJ ---")
      joinExec.broadcastJoinExec(smallRDD, bigRDD,
        args(1) + "ls_bj_" + i, args(2) + "ls_bj_exec", args(2) + "ls_bj_sizeEst")

      println("\t--- left small: shuffle ---")
      joinExec.shuffleJoinExec(smallRDD, bigRDD, args(1) + "ls_shuffle_" + i, args(2) + "ls_shuffle_exec")

      println("\t--- left small: data frame ---")
      joinExec.dfJoinExec(smallRDD, bigRDD, args(1) + "ls_df_" + i, args(2) + "ls_df_exec")

      println("\t--- big: BJ ---")
      joinExec.broadcastJoinExec(bigRDD, bigRDD,
        args(1) + "big_bj_l_" + i, args(2) + "big_bj_exec_l", args(2) + "big_bj_sizeEst_l")

      println("\t--- big: shuffle ---")
      joinExec.shuffleJoinExec(bigRDD, bigRDD, args(1) + "big_shuffle_l_" + i, args(2) + "big_shuffle_exec_l")
    }
  }

  def rightSmallTestSuite[ K: ClassTag, V: ClassTag ](args: Array[ String ],
                                                      bigRDD: RDD[ (K, V) ],
                                                      smallRDD: RDD[ (K, V) ]): Unit = {
    val joinExec = new JoinExec(sc)
    for (i <- 1 to args(3).toInt) {
      println("--- " + i + " ---")

      println("\t--- right small: BJ ---")
      joinExec.broadcastJoinExec(bigRDD, smallRDD,
        args(1) + "rs_bj_" + i, args(2) + "rs_bj_exec", args(2) + "rs_bj_sizeEst")

      println("\t--- right small: shuffle ---")
      joinExec.shuffleJoinExec(bigRDD, smallRDD, args(1) + "rs_shuffle_" + i, args(2) + "rs_shuffle_exec")

      println("\t--- right small: data frame ---")
      joinExec.dfJoinExec(bigRDD, smallRDD, args(1) + "rs_df_" + i, args(2) + "rs_df_exec")

      println("\t--- big: BJ ---")
      joinExec.broadcastJoinExec(bigRDD, bigRDD,
        args(1) + "big_bj_r_" + i, args(2) + "big_bj_exec_r", args(2) + "big_bj_sizeEst_r")

      println("\t--- big: shuffle ---")
      joinExec.shuffleJoinExec(bigRDD, bigRDD, args(1) + "big_shuffle_r_" + i, args(2) + "big_shuffle_exec_r")
    }
  }

  def extractSmallRDD[ K: ClassTag, V: ClassTag ](rdd: RDD[ (K, V) ], numOfRows: Int): RDD[ (K, V) ] = {
    sc.parallelize(rdd.take(numOfRows))
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
