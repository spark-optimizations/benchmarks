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
  /**
    * Perform broadcast join as `rdd1.join(rdd2)` and return execution time in seconds
    *
    * @param rdd1             left side rdd for join operation
    * @param rdd2             right side rdd for join operation
    * @param sizeEstStatsPath path to dump rdd size estimation done by broadcast join
    * @tparam K Key type for `rdd1` and `rdd2`
    * @tparam V Value type for `rdd1` and `rdd2`
    * @return Execution time of broadcast join on `rdd1` and `rdd2` in seconds
    */
  def broadcastJoinExec[ K: ClassTag, V: ClassTag ](rdd1: RDD[ (K, V) ], rdd2: RDD[ (K, V) ],
                                                    sizeEstStatsPath: String): Float = {
    val bj = new BroadcastJoin(sc)
    bj.autoBroadcastJoinThreshold = config.autoBroadcastJoinThreshold
    bj.statsPath = sizeEstStatsPath
    TestUtil.timeBlock(
      bj.join(rdd1, rdd2, new RDDSizeEstimator {})
        .take(1)
    )
  }

  /**
    * Perform shuffle join as `rdd1.join(rdd2)` and return execution time in seconds
    *
    * @param rdd1 left side rdd for join operation
    * @param rdd2 right side rdd for join operation
    * @tparam K Key type for `rdd1` and `rdd2`
    * @tparam V Value type for `rdd1` and `rdd2`
    * @return Execution time of shuffle join on `rdd1` and `rdd2` in seconds
    */
  def shuffleJoinExec[ K: ClassTag, V: ClassTag ](rdd1: RDD[ (K, V) ], rdd2: RDD[ (K, V) ]): Float = {
    TestUtil.timeBlock(
      rdd1.join(rdd2)
        .take(1)
    )
  }
}
