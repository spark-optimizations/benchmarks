package org.so.benchmark.util

import java.io.{FileWriter, PrintWriter}

/**
  * @author Tirthraj
  */
object TestUtil {
  def timeBlock[ F ](block: => F): Float = {
    val startTime = System.currentTimeMillis
    block
    (System.currentTimeMillis - startTime) / 1000f
  }

  def dumpStat(stat: String, filePath: String): Unit = {
    val pw = new PrintWriter(new FileWriter(filePath, true))
    pw.write(stat + "\n")
    pw.close()
  }
}
