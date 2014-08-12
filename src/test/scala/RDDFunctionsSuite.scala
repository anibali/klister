package org.nibali.klister

import org.nibali.klister.Klister._

import org.scalatest.FunSuite

import org.apache.spark.SparkContext._
import org.apache.spark.SharedSparkContext
 
class RDDFunctionsSuite extends FunSuite with SharedSparkContext {
  val nReducers = 2

  test("quantiles") {
    val s = sc.parallelize(Array(3, 6, 7, 8, 8, 10, 13, 15, 16, 20), 2)
    val quants = s.quantiles(4, 1f)
    assert(quants.toList === List(7, 8, 15))
  }

  test("histo") {
    val s = sc.parallelize(Array(3, 6, 7, 8, 8, 10, 13, 15, 16, 20), 2)
    val hist = s.histo(List(10, 15, 20))
    assert(hist === Array(6, 2, 2, 0))
    val hist2 = s.histo(List(8, 8, 8))
    assert(hist2 === Array(5, 0, 0, 5))
  }

  test("kartesian") {
    val s = sc.parallelize(Array((2, "m"), (1, "a"), (1, "b")))
    val t = sc.parallelize(Array((5, "Z"), (1, "C"), (1, "D"), (2, "N")))
    val crossed = s.kartesian(t, nReducers).collect()
    assert(crossed.size === 12)
  }

  test("thetaJoin") {
    val s = sc.parallelize(Array((1, "m"), (2, "a"), (2, "b")))
    val t = sc.parallelize(Array((5, "Z"), (6, "C"), (12, "D"), (2, "N")))
    val joined = s.thetaJoin(t, _ * _ == 12, nReducers).collect()
    assert(joined.size === 3)
    assert(joined.toSet === Set(
      ((1, "m"), (12, "D")),
      ((2, "a"), (6, "C")),
      ((2, "b"), (6, "C"))
    ))
  }

  test("inequalityJoin with 'less than'") {
    val s = sc.parallelize(Array((2, "m"), (1, "a"), (1, "b")))
    val t = sc.parallelize(Array((5, "Z"), (1, "C"), (1, "D"), (2, "N")))
    val joined = s.inequalityJoin(t, Comparison.Less, nReducers).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      ((1, "a"), (5, "Z")),
      ((1, "b"), (5, "Z")),
      ((2, "m"), (5, "Z")),
      ((1, "a"), (2, "N")),
      ((1, "b"), (2, "N"))
    ))
  }

  test("inequalityJoin with 'not equal'") {
    val s = sc.parallelize(Array((2, "m"), (1, "a"), (1, "b")))
    val t = sc.parallelize(Array((5, "Z"), (1, "C"), (1, "D"), (2, "N")))
    val joined = s.inequalityJoin(t, Comparison.NotEq, nReducers).collect()
    assert(joined.size === 7)
    assert(joined.toSet === Set(
      ((1, "a"), (5, "Z")),
      ((1, "b"), (5, "Z")),
      ((1, "a"), (2, "N")),
      ((1, "b"), (2, "N")),
      ((2, "m"), (5, "Z")),
      ((2, "m"), (1, "C")),
      ((2, "m"), (1, "D"))
    ))
  }

  test("equijoin") {
    val s = sc.parallelize(Array((2, "m"), (1, "a"), (1, "b")))
    val t = sc.parallelize(Array((5, "Z"), (1, "C"), (1, "D"), (2, "N")))
    val joined = s.equijoin(t, nReducers).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, ("a", "C")),
      (1, ("a", "D")),
      (1, ("b", "C")),
      (1, ("b", "D")),
      (2, ("m", "N"))
    ))
  }

  test("naiveSimilarityJoin") {
    val s = sc.parallelize(Array(("rocket", 1), ("spark", 2)))
    val t = sc.parallelize(Array(("raps", "Z"), ("sprocket", "C"), ("spark", "D")))
    val joined = s.naiveSimilarityJoin(t, 2, 0.5f).collect()
    assert(joined.size === 2)
    assert(joined.toSet === Set(
      (("spark", 2), ("spark", "D")),
      (("rocket", 1), ("sprocket", "C"))
    ))
  }
}