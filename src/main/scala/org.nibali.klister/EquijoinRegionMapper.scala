package org.nibali.klister

import org.nibali.klister.Klister._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object EquijoinRegionMapper
{
  def apply[K:ClassTag:Ordering,V,W](s:RDD[(K,V)], t:RDD[(K,W)], nReducers:Int): EquijoinRegionMapper[K,V,W] = {
    val sCount = s.count()
    val tCount = t.count()
    val sKeys = s.map(_._1)
    val tKeys = t.map(_._1)
    val bothKeys = sKeys.union(tKeys)
    val nSamples = math.min(sCount + tCount, 1024)
    val q = math.min(64, (nSamples / 2.0).toInt)
    val quants = bothKeys.quantiles(q, nSamples.toFloat/(sCount + tCount)).distinct
    val sHistogram = sKeys.histo(quants)
    val tHistogram = tKeys.histo(quants)
    // I think this is the only statement which needs to change for inequality join
    val boolHist = sHistogram.zip(tHistogram).map(_ match {
      case (0, _) => false
      case (_, 0) => false
      case _ => true
    })
    var candidateAreas = List[((K,K),(K,K))]()
    if(boolHist(0)) {
      candidateAreas = (null,(quants(0),quants(0))) :: candidateAreas
    }
    var i = 0
    quants.sliding(2).foreach(x => {
      i += 1
      if(boolHist(i)) {
        candidateAreas = ((x(0), x(0)), (x(1), x(1))) :: candidateAreas
      }
    })
    if(boolHist.last) {
      candidateAreas = ((quants.last,quants.last),null) :: candidateAreas
    }

    return new EquijoinRegionMapper[K,V,W](candidateAreas, nReducers)
  }
}

class EquijoinRegionMapper[K:ClassTag:Ordering,V,W]
  (private val candidateAreas:List[((K,K),(K,K))], nReducers:Int)
  extends RegionMapper[(K,V),(K,W)](nReducers)
{
  private val ordering = implicitly[Ordering[K]]

  // TODO: Think about whether there is a better way to select regions

  def getSRegions(sRecord:(K,V)):Seq[Int] = {
    var regions = List[Int]()
    var i = 0
    candidateAreas.foreach(a => {a match {
      case (null, (ux, uy)) =>
        if(ordering.lteq(sRecord._1, ux))
          regions = (i % nReducers) :: regions
      case ((lx, ly), null) =>
        if(ordering.gt(sRecord._1, lx))
          regions = (i % nReducers) :: regions
      case ((lx, ly), (ux, uy)) =>
        if(ordering.lteq(sRecord._1, ux) && ordering.gt(sRecord._1, lx))
          regions = (i % nReducers) :: regions
    } ; i += 1})
    return regions.distinct
  }

  def getTRegions(tRecord:(K,W)):Seq[Int] = {
    var regions = List[Int]()
    var i = 0
    candidateAreas.foreach(a => {a match {
      case (null, (ux, uy)) =>
        if(ordering.lteq(tRecord._1, uy))
          regions = (i % nReducers) :: regions
      case ((lx, ly), null) =>
        if(ordering.gt(tRecord._1, ly))
          regions = (i % nReducers) :: regions
      case ((lx, ly), (ux, uy)) =>
        if(ordering.lteq(tRecord._1, uy) && ordering.gt(tRecord._1, ly))
          regions = (i % nReducers) :: regions
    } ; i += 1})
    return regions.distinct
  }
}