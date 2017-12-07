package org.so.benchmark.bj

/**
  * @author Tirthraj
  */
object config {
  // local threshold for auto broadcast join which is in turn passed on to broadcast join library
  var autoBroadcastJoinThreshold: Long = 20 * 1024 * 1024
}
