package org.nibali.klister

import scala.compat.Platform

object Util
{
  /**
  * Given a sequence of boundaries (b0, b1, ..., bn), find the "bucket" in which
  * elem lies.
  * If elem <= b0, 0 will be returned, if elem > b0 and <= b1, 1 will be
  * returned, etc.
  */
  def findBucket[W:Ordering](boundaries:Seq[W], elem:W):Int = {
    val ord = Ordering[W]
    var bucket = 0
    boundaries.foreach(b =>
      if(ord.gt(elem, b))
        bucket += 1
    )
    return bucket
  }
  
  def time(f: => Unit):Long = {
    val time = Platform.currentTime
    f
    return Platform.currentTime - time
  }
}
