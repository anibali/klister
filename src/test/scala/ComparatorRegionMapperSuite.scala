package org.nibali.klister

import org.nibali.klister.Klister._

import org.scalatest._
 
class ComparatorRegionMapperSuite extends FlatSpec with Matchers {
  "ComparatorRegionMapper" should "not cause output duplication" in {
    // A list of ranges covering all 'true' cells in our join matrix
    val ranges = List(
      (1, 1, 2, 2),
      (2, 1, 3, 2),
      (2, 2, 3, 3)
    ).map(x => new Range2D(Some(x._1), Some(x._2), Some(x._3), Some(x._4)))
    val crm = new ComparatorRegionMapper[Int, Any, Any](ranges, 2)
    // A list of pairs of join keys which are candidates for join output
    val candidates = List(
      (2, 2),
      (3, 3),
      (3, 2)
    )
    // Check that each candidate is sent to *exactly one* reducer
    candidates.foreach(_ match {
      case (sKey, tKey) =>
        val sRegs = crm.getSRegions((3, null))
        val tRegs = crm.getTRegions((2, null))
        val intersection = sRegs.toSet.intersect(tRegs.toSet)
        intersection should have size 1
    })
  }
}