package org.so.benchmark.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class DataLoadGenerator(sc: SparkContext) {

  def createLoad[ K: ClassTag ](inputFile: String, loadPower: Int, isLoadSmall: Boolean = false)
  : RDD[ (Long, Array[ Long ]) ] = {
    if(isLoadSmall) {
      // total rows: 4096
      val rdd = sc.textFile(inputFile)
      explodeRDD(baseRDD(rdd), loadPower)
    }
    else {
      // total rows: 1048576
      val rdd = sc.textFile(inputFile)
      explodeRDD(baseRDD(rdd), loadPower)
    }
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
    baseRDD.map {
      line =>
        val k = line.split(";")(0)
        val k256 = k.concat(k.charAt(0).toString)
        // 256 bytes
        (k256.hashCode.toLong, k256.toArray.map(_.hashCode.toLong))
      }
  }
}
