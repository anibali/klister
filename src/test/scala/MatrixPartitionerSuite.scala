package org.nibali.klister.okcan

import org.scalatest.FlatSpec

class MatrixPartitionerSuite extends FlatSpec {
  "A MatrixPartitioner" should "divide the matrix into squares if possible" in {
    val mp = new MatrixPartitioner(16, 16, 4)
    assert(mp.getPartition(7, 7) === 0)
    assert(mp.getPartition(7, 8) === 1)
    assert(mp.getPartition(8, 7) === 2)
    assert(mp.getPartition(8, 8) === 3)
  }
}