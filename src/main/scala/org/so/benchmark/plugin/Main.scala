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
  var inDir  = "input/"
  var timeFile = "results/stats/timings.csv"
  var runId = ""
  var numIter = 10

  def timeNdSave(key:String,
                 directory: String,
                 rdd: RDD[(Long, Long)]): Unit = {
    def saveRDD(rdd : RDD[(Long, Long)], outFile: String)
    : Unit = rdd
      .sortByKey().coalesce(1, shuffle = false)
      .saveAsTextFile(outFile)
    val outFile = outDir + directory + "/" + runId
    Util.deleteRecursively(new File(outFile))

    val t0 = System.currentTimeMillis()
    saveRDD(rdd, outFile)
    val t1 = System.currentTimeMillis()

    scala.tools.nsc.io.File(timeFile)
      .appendAll(key + "\treal\t" + (t1 - t0)/1000.0 + "\n")
  }

  def main(args: Array[String]) {
    if (args.length > 0) inDir    = args(0)
    if (args.length > 1) outDir   = args(1)
    if (args.length > 2) timeFile = args(2)
    if (args.length > 3) runId    = args(3)
    if (args.length > 4) numIter  = args(4).toInt

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local")
    val sc = new SparkContext(conf)

    val dat = sc.textFile(inDir + "similar_artists.csv.gz")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map{x => (x.split(";")(0).hashCode.toLong, null)}.persist()


    // Warmup iteration
    timeNdSave(runId + "_0_0"  , "_0_0"  , tstJoinTuple_0_0(dat))
    tstJoinTuple_0_0(dat).count()
    tstJoinTuple_0_0(dat).count()
    tstJoinTuple_0_0(dat).count()

    // Actual iteration
    val rnd = new scala.util.Random
    for (i <- 1 to numIter) {
      val r = rnd.nextInt(9)
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
        case _ =>
      }
    }
  }

  def tstJoinTuple_0_0(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map{x => (x._1, null)}
      .join(dat)
      .mapValues{x => 0}
  }

  def tstJoinTuple_1_0(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map{x => (x._1, (1l))}
      .join(dat)
      .mapValues{x => 0}
  }

  def tstJoinTuple_1_1(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map{x => (x._1, (1l))}
      .join(dat)
      .mapValues{x => x._1}
  }

  def tstJoinTuple_5_0(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map{x => (x._1, (1l,1l,1l,1l,1l))}
      .join(dat)
      .mapValues{x => 0}
  }
  def tstJoinTuple_5_1(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map{x => (x._1, (1l,1l,1l,1l,1l))}
      .join(dat)
      .mapValues{x => x._1._1}
  }
  def tstJoinTuple_5_5(dat: RDD[(Long, Null)])
    : RDD[(Long, Long)] = {
    dat.map{x => (x._1, (1l,1l,1l,1l,1l))}
      .join(dat)
      .mapValues{x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5}
  }

  def tstJoinTuple_10_0(dat: RDD[(Long, Null)])
    : RDD[(Long, Long)] = {
    dat.map{x => (x._1, (1l,1l,1l,1l,1l,1l,1l,1l,1l,1l))}
      .join(dat)
      .mapValues{x => 0}
  }
  def tstJoinTuple_10_1(dat: RDD[(Long, Null)])
    : RDD[(Long, Long)] = {
    dat.map{x => (x._1, (1l,1l,1l,1l,1l,1l,1l,1l,1l,1l))}
      .join(dat)
      .mapValues{x => x._1._1}
  }
  def tstJoinTuple_10_5(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] = {
    dat.map{x => (x._1, (1l,1l,1l,1l,1l,1l,1l,1l,1l,1l))}
      .join(dat)
      .mapValues{x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5}
  }

  def tstJoinTuple_10_10(dat: RDD[(Long, Null)])
  : RDD[(Long, Long)] =
    dat.map{x => (x._1, (1l,1l,1l,1l,1l,1l,1l,1l,1l,1l))}
      .join(dat)
      .mapValues{x => x._1._1 + x._1._2 + x._1._3 + x._1._4 + x._1._5 + x._1._6 + x._1._7 + x._1._8 + x._1._9 + x._1._10}

}
