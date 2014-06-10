package org.nibali.klister

import scala.language.implicitConversions
import scala.reflect.ClassTag
import org.apache.spark._
import org.apache.spark.rdd.RDD

object Klister {
  object Comparison extends Enumeration {
    trait Gettable {
      // Corresponding comparison function results (subset of {-1,0,1})
      val value: Set[Int]
      def get():Set[Int] = { value }
    }
    type Comparison = Value with Gettable
    def ComparisonValue(set: Set[Int]):Comparison = new Val() with Gettable { val value = set}

    val Less = ComparisonValue(Set(-1))
    val LessEq = ComparisonValue(Set(-1, 0))
    val Greater = ComparisonValue(Set(1))
    val GreaterEq = ComparisonValue(Set(0, 1))
    val Eq = ComparisonValue(Set(0))
    val NotEq = ComparisonValue(Set(-1, 1))
  }

  implicit def rddToKlisterRDDFunctions[T: ClassTag](rdd: RDD[T]) = {
    new KlisterRDDFunctions(rdd)
  }

  implicit def rddToKlisterPairRDDFunctions[K, V](rdd: RDD[(K, V)])
      (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) = {
    new KlisterPairRDDFunctions(rdd)
  }
}
