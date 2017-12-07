package org.so.benchmark.util

import java.io.{FileWriter, PrintWriter}

/**
  * @author Tirthraj
  */
object TestUtil {
  /**
    * Execute given `block` and return the execution time
    */
  def timeBlock[ F ](block: => F): Float = {
    val startTime = System.currentTimeMillis
    block
    (System.currentTimeMillis - startTime) / 1000f
  }

  /**
    * Dump given `stat` into given `filePath` with a new line at the end
    */
  def dumpStat(stat: String, filePath: String): Unit = {
    val pw = new PrintWriter(new FileWriter(filePath, true))
    pw.write(stat + "\n")
    pw.close()
  }
}
