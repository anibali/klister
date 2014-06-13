package org.nibali.klister

/**
* Represents a 2D range, including upper bound and excluding lower bound
*/
class Range2D[K](lx:Option[K], ly:Option[K], ux:Option[K], uy:Option[K])
  (implicit ordering: Ordering[K])
  extends Serializable
{
  def containsX(value:K):Boolean = {
    isBetween(value, lx, ux)
  }

  def containsY(value:K):Boolean = {
    isBetween(value, ly, uy)
  }

  def contains(xVal:K, yVal:K):Boolean = {
    return containsX(xVal) && containsY(yVal)
  }

  private def isBetween(value:K, l:Option[K], u:Option[K]):Boolean = {
    (l, u) match {
      case (None, Some(upper)) =>
        return ordering.lteq(value, upper)
      case (Some(lower), None) =>
        return ordering.gt(value, lower)
      case (Some(lower), Some(upper)) =>
        return ordering.lteq(value, upper) && ordering.gt(value, lower)
      case _ =>
        return false
    }
  }

  override def toString():String = {
    val toS = (a:Option[K]) => {a match {
      case Some(thing) => thing.toString()
      case None => "..."
    }}
    return "Range2D[x: %s -> %s, y: %s -> %s]" format (toS(lx), toS(ux), toS(ly), toS(uy))
  }
}
