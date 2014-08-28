package org.nibali.klister

import scala.compat.Platform
import org.apache.spark.rdd.RDD

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
  
  def log2(x:Int):Int = x match {
    case 0 => 0
    case 1 => 1
    case x if x > 1 => log2(x >> 1) + 1
  }
  
  def lambertW(z:Double, guess:Double = 1):Double = {
    require(z > 0)
    val expGuess = math.exp(guess)
    guess * expGuess match {
      case zed if math.abs(zed - z) < 0.0001 => return guess
      case _ => return lambertW(z, guess - (guess * expGuess - z) / (expGuess + guess * expGuess))
    }
  }
}
