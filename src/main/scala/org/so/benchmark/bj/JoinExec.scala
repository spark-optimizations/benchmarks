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
    bj.autoBroadcastJoinThreshold = config.autoBroadcastJoinThreshold
    bj.statsPath = sizeEstStatsPath
    TestUtil.timeBlock(
      bj.join(rdd1, rdd2, new RDDSizeEstimator {})
        .take(1)
    )
  }

  def shuffleJoinExec[ K: ClassTag, V: ClassTag ](rdd1: RDD[ (K, V) ], rdd2: RDD[ (K, V) ],
                                                  outputPath: String): Float = {
    TestUtil.timeBlock(
      rdd1.join(rdd2)
        .take(1)
    )
  }
}
