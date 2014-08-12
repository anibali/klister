package org.nibali.klister

import org.nibali.klister.Klister._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.control.Breaks._

object SimpleRegionMapper
{
  def apply[S:ClassTag,T:ClassTag](s:RDD[S], t:RDD[T], nReducers:Int): SimpleRegionMapper[S,T] = {
    return new SimpleRegionMapper(s.count(), t.count(), nReducers)
  }
}

class SimpleRegionMapper[S:ClassTag,T:ClassTag]
  (private val sCount:Long, tCount:Long, nReducers:Int)
  extends RegionMapper[S,T](nReducers)
{
  var nhor = 0
  var nver = 0
  var size = 0.0

  init()

  private def init()
  {
    require(sCount > 0, "sCount must be positive")
    require(tCount > 0, "tCount must be positive")
    breakable { (1L to tCount).foreach(i => {
      nhor = math.ceil(tCount / i.toFloat).toInt
      nver = math.ceil(sCount / i.toFloat).toInt
      if(nhor * nver <= nReducers) {
        size = i
        break
      }
    })}
  }

  def getSRegions(sRecord:S):Seq[Int] = {
    val row = math.abs(util.Random.nextLong()) % sCount
    val offset = (row / size.toInt) * nhor
    return (0L until nhor).map(h => (h + offset).toInt).toList
  }

  def getTRegions(tRecord:T):Seq[Int] = {
    val col = math.abs(util.Random.nextLong()) % tCount
    val offset = col / size.toInt
    return (0L until nver).map(v => (v * nhor + offset).toInt).toList
  }
}