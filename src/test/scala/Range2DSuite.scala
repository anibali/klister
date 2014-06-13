package org.nibali.klister

import org.nibali.klister.Klister._

import org.scalatest.FunSuite
 
class Range2DSuite extends FunSuite {
  test("containsX") {
    val r2d = new Range2D(Some(0), Some(10), Some(5), Some(15))
    assert(r2d.containsX(3) === true)
    assert(r2d.containsX(5) === true)
    assert(r2d.containsX(0) === false)
    assert(r2d.containsX(12) === false)
    assert(r2d.containsX(-2) === false)
  }

  test("containsY") {
    val r2d = new Range2D(Some(0), Some(10), Some(5), Some(15))
    assert(r2d.containsY(4) === false)
    assert(r2d.containsY(15) === true)
    assert(r2d.containsY(10) === false)
    assert(r2d.containsY(12) === true)
    assert(r2d.containsY(23) === false)
  }
}