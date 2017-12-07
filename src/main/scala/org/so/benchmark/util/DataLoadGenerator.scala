package org.so.benchmark.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Class responsible for generating data for Broadcast Join Test Suite
  *
  * @author Tirthraj
  */
class DataLoadGenerator(sc: SparkContext) {

  /**
    * Create data of a size = (loadPower of 2)*basedRDDSize. Here basedRDDSize is totalRows*rowSize.
    * For smallLoad, totalRows=4096 and rowSize=256 bytes
    * For bigLoad, totalRows=1048576 and rowSize=256 bytes
    *
    * @param inputFile   based file from which keys are retrieved to generate data
    * @param loadPower   power of 2, which is multiplying factor of rdd size
    * @param isLoadSmall if true generate smallLoad; otherwise generate bigLoad
    * @return generated rdd load of given size, derived from data at `inputFile`
    */
  def createLoad(inputFile: String, loadPower: Int, isLoadSmall: Boolean = false): RDD[ (Long, Array[ Long ]) ] = {
    if (isLoadSmall) {
      // total rows: 4096
      val rdd = sc.textFile(inputFile)
      expandRDD(baseRDD(rdd), loadPower)
    }
    else {
      // total rows: 1048576
      val rdd = sc.textFile(inputFile)
      expandRDD(baseRDD(rdd), loadPower)
    }
  }

  /**
    * Expand given `rdd` recursively to double its size until `pow` is 1. When `pow` is 1, return expanded rdd.
    *
    * @param rdd rdd to being expanded
    * @param pow power of 2 based which is multiplying factor of rdd size
    * @return expanded rdd, which is double the size of `rdd`
    */
  def expandRDD(rdd: RDD[ (Long, Array[ Long ]) ], pow: Int): RDD[ (Long, Array[ Long ]) ] = {
    if (pow == 1) return rdd
    val rdd2 = rdd.map {
      case (k, v) => (k * 2, v)
    }
    val newRDD = rdd.union(rdd2)
    expandRDD(newRDD, pow - 1)
  }

  /**
    * Base RDD created from given `rdd` which is then expanded according to desired size with each row taking 256 bytes.
    *
    * @param rdd the rdd from which base rdd is to be derived
    * @return rdd derived from `rdd` according to calculated size with each row taking 256 bytes
    */
  private[ this ] def baseRDD(rdd: RDD[ String ]): RDD[ (Long, Array[ Long ]) ] = {
    rdd.map {
      line =>
        val k = line.split(";")(0)
        val k256 = k.concat(k.charAt(0).toString)
        // 256 bytes
        (k256.hashCode.toLong, k256.toArray.map(_.hashCode.toLong))
    }
  }
}
