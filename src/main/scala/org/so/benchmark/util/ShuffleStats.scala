package org.so.benchmark.util

import scala.io.Source.fromURL
import net.liftweb.json.{DefaultFormats, parse}

import scala.collection.mutable


/**
  * @author Manthan Thakar
  */
class ShuffleStats() {
  var port = 0

  /**
    * @author Manthan Thakar
    * This class captures the extracted shuffle information from the spark's metrics REST API
    */
  case class StageShuffleStats(shuffleWriteBytes: Long, shuffleReadBytes: Long)
  implicit val formats: DefaultFormats = DefaultFormats

  val oldShufMap = new mutable.HashMap[Long, Long]()

  /** Gets shuffle bytes from the previous call.
    *
    * @return Number of shuffle Bytes from spark endpoint.
    */
  def shuffleBytes(appID: String): Long = {
    if (port == 0) port = findStupidSparkPort(appID)

    val appIDL = appID.hashCode
    var sBytes: Long =  - oldShufMap.getOrElse(appIDL, 0l)
    val response = getStatsFromSparkUI(appID)
    try {
      val stats = parse(response)
      val d = stats.extract[List[StageShuffleStats]]
      sBytes += d.map(_.shuffleWriteBytes).sum
    } catch {
      case e: Exception => println(e)
    }
    oldShufMap.put(appIDL, sBytes)
    sBytes
  }


  /** Finds stupid sparkUI port.
    *
    * @return return the port number if spark application is found on it otherwise returns 0 after 30 attempts.
    */
  def findStupidSparkPort(appID: String) : Int = {
    for (i <- 4040 to 4070){
      try {
        port = i
        getStatsFromSparkUI(appID)
        return i
      } catch { case _:Exception => println("Trying port:" + i + " ...")}
    }
    return 0
  }

  /** Gets Json response from SparkUI.
    *
    * @throws Exception when application is not found on the URI.
    * @return String of the response.
    */
  @throws[Exception]
  def getStatsFromSparkUI(appID: String) : String = {
    fromURL("http://localhost:" + port + "/api/v1/applications/" + appID + "/stages").mkString
  }
}
