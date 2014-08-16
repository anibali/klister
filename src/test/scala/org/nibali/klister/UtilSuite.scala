package org.nibali.klister

import org.nibali.klister.Klister._

import org.scalatest.FunSuite
 
class UtilSuite extends FunSuite {
  test("findBucket") {
    val boundaries = Seq(2, 6, 9)
    assert(Util.findBucket(boundaries, 2) === 0)
    assert(Util.findBucket(boundaries, 3) === 1)
    assert(Util.findBucket(boundaries, 10) === 3)
  }
}
