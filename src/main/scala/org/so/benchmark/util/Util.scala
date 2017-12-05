package org.so.benchmark.Util

import java.io.{File, FileOutputStream, PrintStream}
import java.text.SimpleDateFormat
import java.util.Date

import scala.io.Source

/**
  * @author shabbir.ahussain
  */
object Util {
  val DF = new SimpleDateFormat("yyyy/mm/dd HH:mm:ss")


  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  def mergeFiles(file: File, headers:Seq[String], prefix:String):Unit={
    if (!file.isDirectory) return

    val ps = new PrintStream(new FileOutputStream(file.getPath + "/" + prefix + "merged.csv"))
    ps.println(headers.mkString(";"))

    file.listFiles.foreach(d=> {
      if (d.isDirectory && d.getName.startsWith(prefix)) {
        d.listFiles.foreach(f=>{
          if (f.getName.startsWith("part-")) {
            Source.fromFile(f)
              .getLines
              .foreach(ps.println)
          }
        })
        deleteRecursively(d)
      }
    })
  }
}
