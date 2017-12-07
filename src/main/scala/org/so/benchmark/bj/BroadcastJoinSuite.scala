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
    // 2^1 to 2^7 gb iterations
    for (i <- 1 to 7) {
      println("** " + i + " **")
      val dg: DataLoadGenerator = new DataLoadGenerator(sc)
      val bigRDD = dg.createLoad(args(0), i)
      val smallRDD = extractSmallRDD(bigRDD, 100)

      for (iteration <- 1 to args(3).toInt) {
        println("--- " + iteration + " ---")
        if (args(4).equals("Y"))
          leftSmallTestSuite(args, bigRDD, smallRDD, i, iteration)
        else if (args(4).equals("N"))
          rightSmallTestSuite(args, bigRDD, smallRDD, i, iteration)
        else {
          leftSmallTestSuite(args, bigRDD, smallRDD, i, iteration)
          rightSmallTestSuite(args, bigRDD, smallRDD, i, iteration)
        }
      }
    }
  }

  def leftSmallTestSuite[ K: ClassTag, V: ClassTag ](args: Array[ String ],
                                                     bigRDD: RDD[ (K, V) ],
                                                     smallRDD: RDD[ (K, V) ],
                                                     sizePower: Int,
                                                     iteration: Int): Unit = {
    val joinExec = new JoinExec(sc)

    println("\t--- left small: BJ ---")
    val t1 = joinExec.broadcastJoinExec(smallRDD, bigRDD, args(1) + "ls_bj_" + iteration, args(2) + "ls_bj_sizeEst")
    TestUtil.dumpStat(iteration + ";" + t1 + ";" + sizePower, args(2) + "ls_bj_exec")


    println("\t--- left small: shuffle ---")
    val t2 = joinExec.shuffleJoinExec(smallRDD, bigRDD, args(1) + "ls_shuffle_" + iteration)
    val sb = shuffStats.shuffleBytes(sc.applicationId, "localhost")
    TestUtil.dumpStat(iteration + ";" + t2 + ";" + sizePower, args(2) + "ls_shuffle_exec")
    TestUtil.dumpStat(iteration + ";" + sb + ";" + sizePower, args(2) + "ls_shuffle_bytes")
    //      println("\t--- left small: data frame ---")
    //      joinExec.dfJoinExec(smallRDD, bigRDD, args(1) + "ls_df_" + i, args(2) + "ls_df_exec")

    //      println("\t--- big: BJ ---")
    //      val t3 = joinExec.broadcastJoinExec(bigRDD, bigRDD, args(1) + "big_bj_l_" + i, args(2) + "big_bj_sizeEst_l")
    //      TestUtil.dumpStat(t3, sizePower, args(2) + "big_bj_exec_l")
    //
    //      println("\t--- big: shuffle ---")
    //      val t4 = joinExec.shuffleJoinExec(bigRDD, bigRDD, args(1) + "big_shuffle_l_" + i)
    //      TestUtil.dumpStat(t4, sizePower, args(2) + "big_shuffle_exec_l")

    Util.deleteRecursively(new File(args(1)))
  }

  def rightSmallTestSuite[ K: ClassTag, V: ClassTag ](args: Array[ String ],
                                                      bigRDD: RDD[ (K, V) ],
                                                      smallRDD: RDD[ (K, V) ],
                                                      sizePower: Int,
                                                      iteration: Int): Unit = {
    val joinExec = new JoinExec(sc)

    println("\t--- right small: BJ ---")
    val t1 = joinExec.broadcastJoinExec(bigRDD, smallRDD, args(1) + "rs_bj_" + iteration, args(2) + "rs_bj_sizeEst")
    TestUtil.dumpStat(iteration + ";" + t1 + ";" + sizePower, args(2) + "rs_bj_exec")

    println("\t--- right small: shuffle ---")
    val t2 = joinExec.shuffleJoinExec(bigRDD, smallRDD, args(1) + "rs_shuffle_" + iteration)
    val sb = shuffStats.shuffleBytes(sc.applicationId, "localhost")
    TestUtil.dumpStat(iteration + ";" + t2 + ";" + sizePower, args(2) + "rs_shuffle_exec")
    TestUtil.dumpStat(iteration + ";" + sb + ";" + sizePower, args(2) + "rs_shuffle_bytes")

    //      println("\t--- right small: data frame ---")
    //      joinExec.dfJoinExec(bigRDD, smallRDD, args(1) + "rs_df_" + i, args(2) + "rs_df_exec")

    //      println("\t--- big: BJ ---")
    //      val t3 = joinExec.broadcastJoinExec(bigRDD, bigRDD, args(1) + "big_bj_r_" + i, args(2) + "big_bj_sizeEst_r")
    //      TestUtil.dumpStat(t3, sizePower, args(2) + "big_bj_exec_r")
    //
    //      println("\t--- big: shuffle ---")
    //      val t4 = joinExec.shuffleJoinExec(bigRDD, bigRDD, args(1) + "big_shuffle_r_" + i)
    //      TestUtil.dumpStat(t4, sizePower, args(2) + "big_shuffle_exec_r")

    Util.deleteRecursively(new File(args(1)))
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
