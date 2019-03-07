package com.sparkghsom.main.mr_ghsom

import scala.math.{sqrt,pow}
import com.sparkghsom.main.datatypes.{DimensionType, DimensionTypeEnum}

class InstanceMT( private val _id : Int, private val _label : Array[String], private val _attributeVector : Array[DimensionType] ) extends Serializable {

  def id = _id

  def label = _label

  def attributeVector = _attributeVector

  override def toString() : String = {
    return "InstanceMT : [" + _label + ":" + attributeVector.mkString(",") + "]"
  }

  override def equals(that : Any) : Boolean = {
    that match {
      case other : InstanceMT => (this.label == other.label) &&
        (this.attributeVector.zip(other.attributeVector)
          .forall(elemTup => elemTup._1.equals(elemTup._2))
          )
      case _ => false
    }
  }

  override def hashCode = {
    this.label.hashCode() + this.attributeVector.reduce(_ + _).hashCode()
  }

  def +(that : InstanceMT) : InstanceMT = {
    new InstanceMT(0,Array("+"),
      this.attributeVector.zip(that.attributeVector)
        .map( t => t._1 + t._2 )
    )
  }

  def -(that : InstanceMT) : InstanceMT = {
    new InstanceMT (0,Array("-"),
      this.attributeVector.zip(that.attributeVector).map(t => t._1 - t._2))
  }

  def getLabel = { this._label }

  def getDistanceFrom( other : InstanceMT ) : Double = {

    var distance = 0.0

    this.attributeVector.zip(other.attributeVector).foreach {
      elem =>
        distance += pow(elem._1.getDissimilarityValue(elem._2),2)
    }
    distance // variance instead of deviation
    //sqrt(distance)
  }

  def /(num : Double) : InstanceMT = {
    new InstanceMT(this.id,this.label, this.attributeVector.map(value => value/num))
  }
}


object InstanceMTFunctions {
  def getInstanceMTWithNeighbourhoodFactor(instance : InstanceMT, factor : Double) : InstanceMT = {
    // TODO : change * to applyNeighbourhoodFactor
    InstanceMT(0,Array(""), instance.attributeVector.map(attribute => attribute.applyNeighbourhoodFactor(factor)))
    //instance.attributeVector.map { attrib => attrib * factor }  
  }

  def getAverageInstanceMT(instances : InstanceMT*) : InstanceMT = {

    var avgAttributeVector : Array[DimensionType] = instances(0).attributeVector.map(elem => elem.cloneMe)

    var skippedFirst = false

    for (instance <- instances) {
      if (!skippedFirst)
        skippedFirst = true
      else {
        avgAttributeVector = avgAttributeVector.zip(instance.attributeVector).map(t => t._1 + t._2)
      }
    }

    avgAttributeVector = avgAttributeVector.map( _ / instances.size )

    new InstanceMT(
      0,
      Array(""),
      avgAttributeVector
    )
  }

  /*
  def getQEInstanceMT(instance1 : InstanceMT, instance2 : InstanceMT) : InstanceMT = {
    new InstanceMT("qe", instance1.attributeVector.zip(instance2.attributeVector).map(tup => (tup._1 - tup._2).getAbs))
  }
  * 
  */

  def combineNeighbourhoodFactorUpdates(instance1 : InstanceMT, instance2 : InstanceMT) : InstanceMT = {
    new InstanceMT(
      0,
      Array(""),
      instance1.attributeVector.zip(instance2.attributeVector)
        .map(elem => elem._1 + elem._2)
    )
  }

  def divideInstanceMTByCumulativeNeighbourhoodFactor(instance : InstanceMT, factor : Double) : InstanceMT = {
    new InstanceMT(
      0,
      Array(""),
      instance.attributeVector.map(elem => elem.divideByCumulativeNeighbourhoodFactor(factor))
    )
  }

  def computeUpdateInstanceMT(oldInstanceMT : InstanceMT, newInstanceMT : InstanceMT) : InstanceMT = {

    val newAttributeVector = newInstanceMT.attributeVector.zip(oldInstanceMT.attributeVector)
      .map { elem =>
        elem._1.getUpdateValue(elem._2)
      }

    new InstanceMT(0, Array(""), newAttributeVector)
  }

}

object InstanceMT extends Serializable {
  def apply(id: Int, label : Array[String], attributeVector : Array[DimensionType]) = {
    new InstanceMT (id, label, attributeVector)
  }

  def averageInstanceMT(instance1 : InstanceMT, instance2 : InstanceMT) : InstanceMT = {
    InstanceMT(
      0,
      Array(""),
      instance1.attributeVector.zip(instance2.attributeVector)
        .map(t => (t._1.avgForGrowingLayerWith(t._2))))
  }
}
