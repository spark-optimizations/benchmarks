package org.so.benchmark.util

import java.io.{FileWriter, PrintWriter}

/**
  * @author Tirthraj
  */
object TestUtil {
  def timeBlock[ F ](block: => F, filePath: String): Unit = {
    val startTime = System.currentTimeMillis
    block
    dumpStat(filePath, (System.currentTimeMillis - startTime) / 1000f)
  }

  private[ this ] def dumpStat(filePath: String, stat: Float): Unit = {
    val pw = new PrintWriter(new FileWriter(filePath, true))
    pw.write(stat.toString + "\n")
    pw.close()
  }
}
