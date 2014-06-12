package org.nibali.klister

abstract class RegionMapper[S,T](val nReducers:Int) extends Serializable
{
  def getSRegions(sRecord:S):Seq[Int]
  def getTRegions(tRecord:T):Seq[Int]
}