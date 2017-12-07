package org.so.benchmark.bj

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.neu.so.bj.{BroadcastJoin, RDDSizeEstimator}
import org.so.benchmark.util.TestUtil

import scala.reflect.ClassTag

/**
  * @author Tirthraj
  */
class JoinExec(sc: SparkContext) {
  def broadcastJoinExec[ K: ClassTag, V: ClassTag ](rdd1: RDD[ (K, V) ], rdd2: RDD[ (K, V) ],
                                                    outputPath: String,
                                                    sizeEstStatsPath: String): Float = {
    val bj = new BroadcastJoin(sc)
    bj.statsPath = sizeEstStatsPath
    TestUtil.timeBlock(
      bj.join(rdd1, rdd2, new RDDSizeEstimator {})
        .coalesce(1, shuffle = false)
        .saveAsTextFile(outputPath)
    )
  }

  def shuffleJoinExec[ K: ClassTag, V: ClassTag ](rdd1: RDD[ (K, V) ], rdd2: RDD[ (K, V) ],
                                                  outputPath: String): Float = {
    TestUtil.timeBlock(
      rdd1.join(rdd2)
        .coalesce(1, shuffle = false)
        .saveAsTextFile(outputPath)
    )
  }

//  def dfJoinExec[ K: ClassTag, V: ClassTag ](rdd1: RDD[ (K, V) ], rdd2: RDD[ (K, V) ],
//                                             outputPath: String, statsPath: String): Unit = {
//    import ss.implicits._
//    val df = rdd1.toDF("1", "2")
//    val smallDF = rdd2.toDF("3", "4")
//    TestUtil.timeBlock(
//      df.join(smallDF, df.col("1") === smallDF.col("3"))
//        .write
//        .save(outputPath),
//      statsPath
//    )
//  }
}
