package org.nibali.klister

import org.nibali.klister.okcan.MatrixPartitioner
import org.nibali.klister.Klister._

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.SparkContext._

/**
* Extra functions on RDDs provided by Klister through an implicit
* conversion. Import `org.nibali.klister.Klister._` at the top of your program
* to use these functions.
*/
class KlisterRDDFunctions[T:ClassTag](self: RDD[T])
  extends Logging
  with Serializable
{
  def kartesian[U](other: RDD[U], nReducers:Int = 1):RDD[(T,U)] = {
    var s = self
    var sCount = s.count.toInt
    var t = other
    var tCount = t.count.toInt

    val mp = new MatrixPartitioner(sCount, tCount, nReducers)

    val ss = s.flatMap(e => {
      val row = Math.abs(scala.util.Random.nextLong()) % sCount
      mp.regionsForRow(row).map(region => (region, e))
    })

    val tt = t.flatMap(e => {
      val col = Math.abs(scala.util.Random.nextLong()) % tCount
      mp.regionsForCol(col).map(region => (region, e))
    })

    val cross = ss.join(tt, nReducers).map(_._2)

    return cross
  }
}
