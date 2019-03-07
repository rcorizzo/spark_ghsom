package com.sparkghsom.main.mr_ghsom

import scala.math.{sqrt,pow}
import com.sparkghsom.main.datatypes.{DimensionType, DimensionTypeEnum}

class Instance( private val _id : Int, private val _label : String, private val _attributeVector : Array[DimensionType] ) extends Serializable {

  def id = _id

  def label = _label 
  
  def attributeVector = _attributeVector
  
  override def toString() : String = {
    return "Instance : [" + _label + ":" + attributeVector.mkString(",") + "]"
  }
  
  override def equals(that : Any) : Boolean = {
    that match {
      case other : Instance => (this.label == other.label) &&
                               (this.attributeVector.zip(other.attributeVector)
                                                    .forall(elemTup => elemTup._1.equals(elemTup._2))
                               )  
      case _ => false
    }
  }
  
  override def hashCode = {
    this.label.hashCode() + this.attributeVector.reduce(_ + _).hashCode()
  }
  
  def +(that : Instance) : Instance = {
    new Instance(0,"+",
                  this.attributeVector.zip(that.attributeVector)
                                      .map( t => t._1 + t._2 )               
        )
  }
  
  def -(that : Instance) : Instance = {
    new Instance (0,"-",
                  this.attributeVector.zip(that.attributeVector).map(t => t._1 - t._2))
  }

  def getLabel = { this._label }

  def getDistanceFrom( other : Instance ) : Double = { 
    
    var distance = 0.0
    
    this.attributeVector.zip(other.attributeVector).foreach { 
      elem => 
        distance += pow(elem._1.getDissimilarityValue(elem._2),2)
    }
    distance // variance instead of deviation
    //sqrt(distance)
  }
  
  def /(num : Double) : Instance = {
    new Instance(this.id,this.label, this.attributeVector.map(value => value/num))
  }
}


object InstanceFunctions {
  def getInstanceWithNeighbourhoodFactor(instance : Instance, factor : Double) : Instance = {
    // TODO : change * to applyNeighbourhoodFactor
    Instance(0,"", instance.attributeVector.map(attribute => attribute.applyNeighbourhoodFactor(factor)))
    //instance.attributeVector.map { attrib => attrib * factor }  
  }
  
  def getAverageInstance(instances : Instance*) : Instance = {
    
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
    
    new Instance(
        0,
        "",
        avgAttributeVector
        )
  }
  
  /*
  def getQEInstance(instance1 : Instance, instance2 : Instance) : Instance = {
    new Instance("qe", instance1.attributeVector.zip(instance2.attributeVector).map(tup => (tup._1 - tup._2).getAbs))
  }
  * 
  */
  
  def combineNeighbourhoodFactorUpdates(instance1 : Instance, instance2 : Instance) : Instance = {
    new Instance(
        0,
        "", 
        instance1.attributeVector.zip(instance2.attributeVector)
                                 .map(elem => elem._1 + elem._2)
        )
  }
  
  def divideInstanceByCumulativeNeighbourhoodFactor(instance : Instance, factor : Double) : Instance = {
    new Instance(
      0,
      "",
      instance.attributeVector.map(elem => elem.divideByCumulativeNeighbourhoodFactor(factor))
    )
  }

  def divideInstanceMTByCumulativeNeighbourhoodFactor(instance : InstanceMT, factor : Double) : InstanceMT = {
    new InstanceMT(
      0,
      Array(""),
      instance.attributeVector.map(elem => elem.divideByCumulativeNeighbourhoodFactor(factor))
    )
  }
  
  def computeUpdateInstance(oldInstance : Instance, newInstance : Instance) : Instance = {
    
    val newAttributeVector = newInstance.attributeVector.zip(oldInstance.attributeVector)
                                                        .map { elem => 
                                                                 elem._1.getUpdateValue(elem._2)   
                                                        }
    
    new Instance(0, "", newAttributeVector)
  }
  
}

object Instance extends Serializable {
  def apply(id: Int, label : String, attributeVector : Array[DimensionType]) = {
    new Instance (id, label, attributeVector)
  }
  
  def averageInstance(instance1 : Instance, instance2 : Instance) : Instance = {
    Instance(
      0,
        "", 
        instance1.attributeVector.zip(instance2.attributeVector)
                                 .map(t => (t._1.avgForGrowingLayerWith(t._2))))
  }
}
