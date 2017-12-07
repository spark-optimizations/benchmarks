package org.so.benchmark.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author Tirthraj
  */
object SparkUtil {
  /**
    * Creae a Spark Session with app name and configuration.
    *
    * @return created spark session
    */
  def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("Broadcast Join Test Suite")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
  }

  /**
    * Create a spark context from given spark context
    *
    * @param ss spark session which is used to get spark context
    * @return created spark context
    */
  def createSparkContext(ss: SparkSession): SparkContext = {
    val sc = ss.sparkContext
    sc.setLogLevel("ERROR")
    sc
  }
}
