package org.nibali.klister

import org.nibali.klister.Klister._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object ComparatorRegionMapper
{
  def apply[K:ClassTag:Ordering,V,W](s:RDD[(K,V)], t:RDD[(K,W)], op:Comparison.Comparison, nReducers:Int): ComparatorRegionMapper[K,V,W] = {
    val sCount = s.count()
    val tCount = t.count()
    val sKeys = s.map(_._1)
    val tKeys = t.map(_._1)
    val bothKeys = sKeys.union(tKeys)
    val nSamples = math.min(sCount + tCount, 1024)
    val q = math.min(64, (nSamples / 2.0).toInt)
    val quants = bothKeys.quantiles(q, nSamples.toFloat/(sCount + tCount)).distinct.toArray
    val sHistogram = sKeys.histo(quants)
    val tHistogram = tKeys.histo(quants)

    var candidateRanges = List[Range2D[K]]()
    for(i <- 0 until sHistogram.size) {
      for(j <- 0 until tHistogram.size) {
        if((i == j || op.get.contains(i.compareTo(j))) && sHistogram(i) > 0 && tHistogram(j) > 0) {
          candidateRanges = new Range2D(
            quants.lift(i-1), quants.lift(j-1), quants.lift(i), quants.lift(j)
          ) :: candidateRanges
        }
      }
    }

    return new ComparatorRegionMapper[K,V,W](candidateRanges, nReducers)
  }
}

class ComparatorRegionMapper[K:ClassTag:Ordering,V,W]
  (private val candidateRanges:List[Range2D[K]], nReducers:Int)
  extends RegionMapper[(K,V),(K,W)](nReducers)
{
  private val ordering = implicitly[Ordering[K]]

  // FIXME: This current modulo method can produce duplicates in output!
  // Perhaps a BSP-style approach will yield good results

  def getSRegions(sRecord:(K,V)):Seq[Int] = {
    var regions = List[Int]()
    var i = 0
    candidateRanges.foreach(r2d => {
      if(r2d.containsX(sRecord._1)) {
        regions = (i % nReducers) :: regions
      }
      i += 1
    })
    return regions.distinct
  }

  def getTRegions(tRecord:(K,W)):Seq[Int] = {
    var regions = List[Int]()
    var i = 0
    candidateRanges.foreach(r2d => {
      if(r2d.containsY(tRecord._1)) {
        regions = (i % nReducers) :: regions
      }
      i += 1
    })
    return regions.distinct
  }
}