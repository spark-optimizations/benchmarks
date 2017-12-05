package org.so.benchmark.plugin

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.so.benchmark.Util.Util

/**
  * @author shabbir.ahussain
  */
object Main {
  var outDir = "out/results/plugin/"
  var inDir = "input/"
  var timeFile = "results/stats/timings.csv"
  var runId = ""
  var numIter = 10

  def timeNdSave(key: String,
                 directory: String,
                 rdd: RDD[(Long, Long)]): Unit = {
    def saveRDD(rdd: RDD[(Long, Long)], outFile: String)
    : Unit = rdd
      .sortByKey().coalesce(1, shuffle = false)
      .saveAsTextFile(outFile)

    println("Running " + key)

    val outFile = outDir + directory + "/" + runId
    Util.deleteRecursively(new File(outFile))

    val t0 = System.currentTimeMillis()
    saveRDD(rdd, outFile)
    val t1 = System.currentTimeMillis()

    scala.tools.nsc.io.File(timeFile)
      .appendAll(key + "\treal\t" + (t1 - t0) / 1000.0 + "\n")
  }

  def main(args: Array[String]) {
    if (args.length > 0) inDir = args(0)
    if (args.length > 1) outDir = args(1)
    if (args.length > 2) timeFile = args(2)
    if (args.length > 3) runId = args(3)
    if (args.length > 4) numIter = args(4).toInt

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val dat = sc.textFile(inDir + "similar_artists.csv.gz")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map { x => (x.split(";")(0).hashCode.toLong, null) }.persist()


    // Warmup iteration
    timeNdSave(runId + "_0_0", "_0_0", tstJoinTuple_0_0(dat))
    tstJoinTuple_0_0(dat).count()
    tstJoinTuple_0_0(dat).count()
    tstJoinTuple_0_0(dat).count()

    // Actual iteration
    val rnd = new scala.util.Random
    for (i <- 1 to numIter) {
      print ("Iter " + i + " of " + numIter + "\t:")
      val r = rnd.nextInt(27)
      r match {
        case 0 => timeNdSave(runId + "_1_0", "_1_0", tstJoinTuple_1_0(dat))
        case 1 => timeNdSave(runId + "_1_1", "_1_1", tstJoinTuple_1_1(dat))
        case 2 => timeNdSave(runId + "_5_0", "_5_0", tstJoinTuple_5_0(dat))
        case 3 => timeNdSave(runId + "_5_1", "_5_1", tstJoinTuple_5_1(dat))
        case 4 => timeNdSave(runId + "_5_5", "_5_5", tstJoinTuple_5_5(dat))
        case 5 => timeNdSave(runId + "_10_0", "_10_0", tstJoinTuple_10_0(dat))
        case 6 => timeNdSave(runId + "_10_1", "_10_1", tstJoinTuple_10_1(dat))
        case 7 => timeNdSave(runId + "_10_5", "_10_5", tstJoinTuple_10_5(dat))
        case 8 => timeNdSave(runId + "_10_10", "_10_10", tstJoinTuple_10_10(dat))
        case 9 => timeNdSave(runId + "_15_0", "_15_0", tstJoinTuple_15_0(dat))
        case 10 => timeNdSave(runId + "_15_1", "_15_1", tstJoinTuple_15_1(dat))
        case 11 => timeNdSave(runId + "_15_5", "_15_5", tstJoinTuple_15_5(dat))
        case 12 => timeNdSave(runId + "_15_10", "_15_10", tstJoinTuple_15_10(dat))
        case 13 => timeNdSave(runId + "_15_15", "_15_15", tstJoinTuple_15_15(dat))
        case 14 => timeNdSave(runId + "_20_0", "_20_0", tstJoinTuple_20_0(dat))
        case 15 => timeNdSave(runId + "_20_1", "_20_1", tstJoinTuple_20_1(dat))
        case 16 => timeNdSave(runId + "_20_5", "_20_5", tstJoinTuple_20_5(dat))
        case 17 => timeNdSave(runId + "_20_10", "_20_10", tstJoinTuple_20_10(dat))
        case 18 => timeNdSave(runId + "_20_15", "_20_15", tstJoinTuple_20_15(dat))
        case 19 => timeNdSave(runId + "_20_20", "_20_20", tstJoinTuple_20_20(dat))
        case 20 => timeNdSave(runId + "_22_0", "_22_0", tstJoinTuple_22_0(dat))
        case 21 => timeNdSave(runId + "_22_1", "_22_1", tstJoinTuple_22_1(dat))
        case 22 => timeNdSave(runId + "_22_5", "_22_5", tstJoinTuple_22_5(dat))
        case 23 => timeNdSave(runId + "_22_10", "_22_10", tstJoinTuple_22_10(dat))
        case 24 => timeNdSave(runId + "_22_15", "_22_15", tstJoinTuple_22_15(dat))
        case 25 => timeNdSave(runId + "_22_20", "_22_20", tstJoinTuple_22_20(dat))
        case 26 => timeNdSave(runId + "_22_22", "_22_22", tstJoinTuple_22_22(dat))
        case _ =>
      }
    }
  }
  // Warmup
  def tstJoinTuple_0_0(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, null) }
      .join(dat)
      .mapValues { x => 0 }
  }

  // 1 Column tuple
  val tupe1 = (1l)
  def tstJoinTuple_1_0(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tupe1) }
      .join(dat)
      .mapValues { x => 0 }
  }

  def tstJoinTuple_1_1(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tupe1) }
      .join(dat)
      .mapValues { x => x._1 }
  }

  // 5 Column tuple
  val tup5 = (1l, 1l, 1l, 1l, 1l)
  def tstJoinTuple_5_0(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup5) }
      .join(dat)
      .mapValues { x => 0 }
  }

  def tstJoinTuple_5_1(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup5) }
      .join(dat)
      .mapValues { x => x._1._1 }
  }
  def tstJoinTuple_5_5(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup5) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 }
  }

  // 10 Column tuple
  val tup10 = (1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l)
  def tstJoinTuple_10_0(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup10) }
      .join(dat)
      .mapValues { x => 0 }
  }
  def tstJoinTuple_10_1(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup10) }
      .join(dat)
      .mapValues { x => x._1._1 }
  }
  def tstJoinTuple_10_5(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup10) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 }
  }
  def tstJoinTuple_10_10(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup10) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 + x._1._6 + x._1._7 + x._1._8 + x._1._9 + x._1._10 }

  }

  // 15 Column tuple
  val tup15 = (1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l)
  def tstJoinTuple_15_0(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup15) }
      .join(dat)
      .mapValues { x => 0 }
  }
  def tstJoinTuple_15_1(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup15) }
      .join(dat)
      .mapValues { x => x._1._1 }
  }
  def tstJoinTuple_15_5(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup15) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 }
  }
  def tstJoinTuple_15_10(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup15) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 + x._1._6 + x._1._7 + x._1._8 + x._1._9 + x._1._10 }
  }
  def tstJoinTuple_15_15(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup15) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 + x._1._6 + x._1._7 + x._1._8 + x._1._9 + x._1._10 + x._1._11 + x._1._12 + x._1._13 + x._1._14 + x._1._15}
  }

  // 20 Column tuple
  val tup20 = (1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l)
  def tstJoinTuple_20_0(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup20) }
      .join(dat)
      .mapValues { x => 0 }
  }
  def tstJoinTuple_20_1(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup20) }
      .join(dat)
      .mapValues { x => x._1._1 }
  }
  def tstJoinTuple_20_5(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup20) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 }
  }
  def tstJoinTuple_20_10(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup20) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 + x._1._6 + x._1._7 + x._1._8 + x._1._9 + x._1._10 }
  }
  def tstJoinTuple_20_15(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup20) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 + x._1._6 + x._1._7 + x._1._8 + x._1._9 + x._1._10 + x._1._11 + x._1._12 + x._1._13 + x._1._14 + x._1._15}
  }
  def tstJoinTuple_20_20(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup20) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 + x._1._6 + x._1._7 + x._1._8 + x._1._9 + x._1._10 + x._1._11 + x._1._12 + x._1._13 + x._1._14 + x._1._15 + x._1._16 + x._1._17 + x._1._18 + x._1._19 + x._1._20}
  }

  // 22 Column tuple
  val tup22 = (1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l, 1l)
  def tstJoinTuple_22_0(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup22) }
      .join(dat)
      .mapValues { x => 0 }
  }
  def tstJoinTuple_22_1(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup22) }
      .join(dat)
      .mapValues { x => x._1._1 }
  }
  def tstJoinTuple_22_5(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup22) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 }
  }
  def tstJoinTuple_22_10(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup22) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 + x._1._6 + x._1._7 + x._1._8 + x._1._9 + x._1._10 }
  }
  def tstJoinTuple_22_15(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup22) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 + x._1._6 + x._1._7 + x._1._8 + x._1._9 + x._1._10 + x._1._11 + x._1._12 + x._1._13 + x._1._14 + x._1._15}
  }
  def tstJoinTuple_22_20(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup22) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 + x._1._6 + x._1._7 + x._1._8 + x._1._9 + x._1._10 + x._1._11 + x._1._12 + x._1._13 + x._1._14 + x._1._15 + x._1._16 + x._1._17 + x._1._18 + x._1._19 + x._1._20}
  }
  def tstJoinTuple_22_22(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map { x => (x._1, tup22) }
      .join(dat)
      .mapValues { x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 + x._1._6 + x._1._7 + x._1._8 + x._1._9 + x._1._10 + x._1._11 + x._1._12 + x._1._13 + x._1._14 + x._1._15 + x._1._16 + x._1._17 + x._1._18 + x._1._19 + x._1._20 + x._1._21 + x._1._22}
  }
}