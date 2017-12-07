package org.so.benchmark.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class DataLoadGenerator(sc: SparkContext) {

  def createLoad[ K: ClassTag ](inputFile: String, loadPower: Int): RDD[ (Long, Array[ Long ]) ] = {
    // total rows: 1048576
    val rdd = sc.textFile(inputFile)
    explodeRDD(baseRDD(rdd), loadPower)
  }

  def explodeRDD(rdd: RDD[ (Long, Array[ Long ]) ], pow: Int): RDD[ (Long, Array[ Long ]) ] = {
    if (pow == 1) return rdd
    val rdd2 = rdd.map {
      case(k,v) => (k*2, v)
    }
    val newRDD = rdd.union(rdd2)
    explodeRDD(newRDD, pow - 1)
  }

  private[ this ] def baseRDD(baseRDD: RDD[ String ]): RDD[ (Long, Array[ Long ]) ] = {
    // 1048576 * 128 bytes = 128 mb
    baseRDD.map {
      line =>
        val key = line.split(";")(0).take(15)
        // 16 long values = 16 * 8 = 128 bytes
        (key.hashCode.toLong, key.toArray.map(_.hashCode.toLong))
      }
  }
}
