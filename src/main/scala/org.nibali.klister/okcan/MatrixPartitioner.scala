package org.nibali.klister.okcan

import scala.util.control.Breaks._

class MatrixPartitioner(private val rows:Long, cols:Long, reducers:Int) extends Serializable {
  var nhor = 0
  var nver = 0
  var size = 0.0

  init()

  private def init()
  {
    require(rows > 0, "There must be more than 0 rows")
    require(cols > 0, "There must be more than 0 columns")

    breakable { (1L to cols).foreach(i => {
      nhor = math.ceil(cols / i.toFloat).toInt
      nver = math.ceil(rows / i.toFloat).toInt
      if(nhor * nver <= reducers) {
        size = i
        break
      }
    })}
    //printf("%d %d %d\n", nhor, nver, size.toInt)
  }

  def regionsForRow(row:Long):Seq[Int] = {
    val offset = (row / size.toInt) * nhor
    return (0L until nhor).map(h => (h + offset).toInt).toList
  }

  def regionsForCol(col:Long):Seq[Int] = {
    val offset = col / size.toInt
    return (0L until nver).map(v => (v * nhor + offset).toInt).toList
  }

  def getPartition(row:Long, col:Long):Int = {
    regionsForRow(row).intersect(regionsForCol(col)).apply(0).toInt
  }

  def dump() {
    var row, col = 0
    for(row <- 0L until rows) {
      for(col <- 0L until cols) {
        printf("%-2d", getPartition(row, col))
      }
      println()
    }
  }

  override def toString():String = {
    rows + ", " + cols
  }
}