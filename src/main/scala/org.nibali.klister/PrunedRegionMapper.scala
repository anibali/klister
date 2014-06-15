package org.nibali.klister

import org.nibali.klister.Klister._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class PrunedRegionMapper[K:ClassTag:Ordering,V,W]
  (private val candidateRanges:List[Range2D[K]], nReducers:Int)
  extends RegionMapper[(K,V),(K,W)](nReducers)
{
  private val ordering = implicitly[Ordering[K]]
  private val bspRoot = init()

  private def init():BSPNode = {
    val root = bsp(candidateRanges, nReducers)
    var reg = 0
    val stack = collection.mutable.Stack[BSPNode](root)
    while(!stack.isEmpty) {
      stack.pop match {
        case leaf:BSPLeaf[K] =>
          leaf.region = reg
          reg += 1
        case branch:BSPBranch =>
          stack.push(branch.leftChild)
          stack.push(branch.rightChild)
      }
    }
    return root
  }

  def getSRegions(sRecord:(K,V)):Seq[Int] = {
    var regions = List[Int]()
    val stack = collection.mutable.Stack[BSPNode](bspRoot)
    while(!stack.isEmpty) {
      stack.pop match {
        case leaf:BSPLeaf[K] =>
          if(leaf.containsX(sRecord._1)) regions ::= leaf.region
        case hb:BSPHCut[K] =>
          stack.push(hb.leftChild)
          stack.push(hb.rightChild)
        case vb:BSPVCut[K] =>
          if(ordering.gt(sRecord._1, vb.cutPos)) {
            stack.push(vb.rightChild)
          } else {
            stack.push(vb.leftChild)
          }
      }
    }
    return regions
  }

  def getTRegions(tRecord:(K,W)):Seq[Int] = {
    var regions = List[Int]()
    val stack = collection.mutable.Stack[BSPNode](bspRoot)
    while(!stack.isEmpty) {
      stack.pop match {
        case leaf:BSPLeaf[K] =>
          if(leaf.containsY(tRecord._1)) regions ::= leaf.region
        case hb:BSPHCut[K] =>
          if(ordering.gt(tRecord._1, hb.cutPos)) {
            stack.push(hb.rightChild)
          } else {
            stack.push(hb.leftChild)
          }
        case vb:BSPVCut[K] =>
          stack.push(vb.leftChild)
          stack.push(vb.rightChild)
      }
    }
    return regions
  }

  // This prioritises reducer output balancing using a greedy approach
  private def bsp(ranges:List[Range2D[K]], nRegions:Int):BSPNode = {
    if(nRegions < 2) {
      return new BSPLeaf(ranges)
    } else {
      var bestCut:BSPBranch = null
      var minRatio = Float.MaxValue
      var bestRanges1, bestRanges2 = List[Range2D[K]]()
      var bestNRegions1, bestNRegions2 = 0

      val vCuts = collection.mutable.Set[K]()
      ranges.foreach(range => {
        range.lx match {
          case Some(pos) => vCuts.add(pos)
          case _ =>
        }
        range.ux match {
          case Some(pos) => vCuts.add(pos)
          case _ =>
        }
      })
      vCuts.remove(vCuts.min)
      vCuts.remove(vCuts.max)
      vCuts.foreach(vCut => {
        val grouped = ranges.groupBy(_.lx match {
          case Some(pos) => ordering.gteq(pos, vCut)
          case None => false
        })
        var r1, r2 = List[Range2D[K]]()
        (grouped.lift(false), grouped.lift(true)) match {
          case (Some(a), Some(b)) =>
            r1 = a
            r2 = b
          case (Some(a), None) =>
            r1 = a
          case _ =>
            println("We shouldn't be here. Oops.")
        }
        for(nRegions1 <- 1 until nRegions) {
          val nRegions2 = nRegions - nRegions1
          val ratio = math.abs(1 - (r2.size.toFloat / r1.size) / (nRegions2.toFloat / nRegions1))
          if(ratio < minRatio) {
            minRatio = ratio
            bestRanges1 = r1
            bestRanges2 = r2
            bestNRegions1 = nRegions1
            bestNRegions2 = nRegions2
            bestCut = new BSPVCut(vCut)
          }
        }
      })

      val hCuts = collection.mutable.Set[K]()
      ranges.foreach(range => {
        range.ly match {
          case Some(pos) => hCuts.add(pos)
          case _ =>
        }
        range.uy match {
          case Some(pos) => hCuts.add(pos)
          case _ =>
        }
      })
      hCuts.remove(hCuts.min)
      hCuts.remove(hCuts.max)
      hCuts.foreach(hCut => {
        val grouped = ranges.groupBy(_.ly match {
          case Some(pos) => ordering.gteq(pos, hCut)
          case None => false
        })
        var r1, r2 = List[Range2D[K]]()
        (grouped.lift(false), grouped.lift(true)) match {
          case (Some(a), Some(b)) =>
            r1 = a
            r2 = b
          case (Some(a), None) =>
            r1 = a
          case _ =>
            println("We shouldn't be here. Oops.")
        }
        for(nRegions1 <- 1 until nRegions) {
          val nRegions2 = nRegions - nRegions1
          val ratio = math.abs(1 - (r2.size.toFloat / r1.size) / (nRegions2.toFloat / nRegions1))
          if(ratio < minRatio) {
            minRatio = ratio
            bestRanges1 = r1
            bestRanges2 = r2
            bestNRegions1 = nRegions1
            bestNRegions2 = nRegions2
            bestCut = new BSPHCut(hCut)
          }
        }
      })
      if(bestCut == null) {
        return new BSPLeaf(ranges)
      } else {
        bestCut.leftChild = bsp(bestRanges1, bestNRegions1)
        bestCut.rightChild = bsp(bestRanges2, bestNRegions2)
        return bestCut
      }
    }
  }

  private abstract class BSPNode extends Serializable {
  }

  private class BSPVCut[K:Ordering](var cutPos:K)
    extends BSPBranch
  {
    override def toString():String = {
      "V:" + cutPos + "[" + leftChild + "]" + "[" + rightChild + "]"
    }
  }

  private class BSPHCut[K:Ordering](var cutPos:K)
    extends BSPBranch
  {
    override def toString():String = {
      "H:" + cutPos + "[" + leftChild + "]" + "[" + rightChild + "]"
    }
  }

  private abstract class BSPBranch
    extends BSPNode
  {
    var leftChild:BSPNode = null
    var rightChild:BSPNode = null
  }

  private class BSPLeaf[K:Ordering](var ranges:List[Range2D[K]])
    extends BSPNode
  {
    private val ordering = implicitly[Ordering[K]]
    var region:Int = -1

    def containsX(x:K):Boolean = {
      ranges.find(_.containsX(x)).isDefined
    }

    def containsY(y:K):Boolean = {
      ranges.find(_.containsY(y)).isDefined
    }

    override def toString():String = {
      region.toString
    }
  }
}
