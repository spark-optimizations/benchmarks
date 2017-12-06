package org.so.benchmark.util

import scala.io.Source.fromURL
import net.liftweb.json.{DefaultFormats, parse}

/**
  * @author Manthan Thakar
  * This class captures the extracted shuffle information from the spark's metrics REST API
  */
case class StageShuffleStats(shuffleWriteBytes: Long, shuffleReadBytes: Long)

/**
  * @author Manthan Thakar
  */
object ShuffleStats {
  implicit val formats: DefaultFormats = DefaultFormats

  def shuffleBytes(appID: String): Long = {
    println("App ID", appID)
    var totalShuffledBytes: Long = 0l
    val response = fromURL("http://localhost:4040/api/v1/applications/" + appID + "/stages").mkString
    try {
      val stats = parse(response)
      val d = stats.extract[List[StageShuffleStats]]
      totalShuffledBytes = d.map(_.shuffleWriteBytes).sum
    } catch {
      case e: Exception => println(e)
    }
    totalShuffledBytes
  }
}
