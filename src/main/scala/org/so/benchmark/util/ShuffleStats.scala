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
    * @param appID is the application master id.
    * @param yarnIP is the application master URL.
    * @return Number of shuffle Bytes from spark endpoint.
    */
  def shuffleBytes(appID: String, yarnIP: String): Long = {
    if (port == 0) port = findStupidSparkPort(yarnIP, appID)

    val appIDL = appID.hashCode
    var sBytes: Long =  0
    var currentBytes: Long = 0
    val response = getStatsFromSparkUI(yarnIP, appID)
    try {
      val stats = parse(response)
      val d = stats.extract[List[StageShuffleStats]]
      currentBytes = d.map(_.shuffleWriteBytes).sum
      sBytes = currentBytes - oldShufMap.getOrElse(appIDL, 0l)
    } catch {
      case e: Exception => println(e)
    }
    oldShufMap.put(appIDL, currentBytes)
    sBytes
  }


  /** Finds stupid sparkUI port.
    *
    * @param appID is the application master id.
    * @param yarnIP is the application master URL.
    * @return return the port number if spark application is found on it otherwise returns 0 after 30 attempts.
    */
  def findStupidSparkPort(yarnIP: String ,appID: String) : Int = {
    for (i <- 4040 to 4070){
      try {
        port = i
        getStatsFromSparkUI(yarnIP, appID)
        return i
      } catch { case _:Exception => println("Trying port:" + i + " ...")}
    }
    return 0
  }

  /** Gets Json response from SparkUI.
    *
    * @param appID is the application master id.
    * @param yarnIP is the application master URL.
    * @throws Exception when application is not found on the URI.
    * @return String of the response.
    */
  @throws[Exception]
  def getStatsFromSparkUI(yarnIP: String, appID: String) : String = {
    fromURL("http://" + yarnIP + ":" + port + "/api/v1/applications/" + appID + "/stages").mkString
  }
}
