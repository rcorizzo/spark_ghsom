package com.sparkghsom.main.datatypes


import scala.util.Random 
import scala.compat.Platform
import scala.collection.mutable

/**
 * Trait for individual dimension of an attribute vector
 */
trait DimensionType extends Serializable with Ordered[DimensionType] {
  
  def attributeName : String
  //def attributeName_=(value : String) : Unit
  def attributeType : DimensionTypeEnum.Value
  //def attributeType_=(value : DimensionTypeEnum.Value)
  
  //def getDistanceFrom( that : DimensionType ) : Double 
  //def getRandomDimensionValue() : DimensionType
  def equals( d2 : DimensionType ) : Boolean
  override def toString() : String
  def !=(d2 : DimensionType) : Boolean
  def ==(d2 : DimensionType) : Boolean
  def <(d2 : DimensionType) : Boolean
  def <=(d2 : DimensionType) : Boolean
  def >(d2 : DimensionType) : Boolean
  def >=(d2 : DimensionType) : Boolean
  def +(d2 : DimensionType) : DimensionType
  def -(d2 : DimensionType) : DimensionType
  def getDissimilarityValue(d2 : DimensionType) : Double
  def /(num : Long) : DimensionType
  def /(num : Double) : DimensionType
  def /(d2 : DimensionType) : DimensionType
  def *(d2 : DimensionType) : DimensionType 
  
  override def hashCode : Int
  def applyNeighbourhoodFactor(factor : Double) : DimensionType
  def divideByCumulativeNeighbourhoodFactor(factor : Double) : DimensionType
  def getUpdateValue(oldValue : DimensionType) : DimensionType
  
  // For dimensions like nominal which store additional information.
  // This method is expected to return a new dimension trimming out the
  // additional information. Introduced for case where new neuron instance
  // had to be created for the new level layer in the hierarchy
  def getFreshDimension : DimensionType
  
  // Average instance when growing a layer (average between error neuron and the most distant neighbour)
  def avgForGrowingLayerWith(that : DimensionType) : DimensionType
  
//  def /(d2 : DimensionType) : DimensionType
//  def *(num : Double) : DimensionType
  def cloneMe : DimensionType 
  def getImagePixelValue (domainValues : Array[_ <: Any] = null) : Double
//  def getAbs : DimensionType
//  def isZero : Boolean
//  def getValue : Double
}

object DimensionType {
  
  val randomGenerator = new Random(Platform.currentTime)
  
  def getMax( a : DimensionType, b : DimensionType ) : DimensionType = {
    a match {
      /*case value_a : DoubleDimension => 
        if (value_a > b.asInstanceOf[DoubleDimension]) {
          value_a
        }
        else {
          b
        }
      case value_a : NominalDimension => 
        if (value_a > b.asInstanceOf[NominalDimension]) {
          value_a
        }
        else {
          b
        }*/
      case value_a : DistanceHierarchyDimension =>
        if (value_a > b.asInstanceOf[DistanceHierarchyDimension]) {
          value_a
        }
        else {
          b
        }
      case _ => throw new IllegalArgumentException("Not supported DimensionType")
    }
  }
  
  def getMin( a : DimensionType, b : DimensionType ) : DimensionType = {
    a match {
      /*case value_a : DoubleDimension => 
        if (value_a < b.asInstanceOf[DoubleDimension]) {
          value_a
        }
        else {
          b
        }
      case value_a : NominalDimension => 
        if (value_a < b.asInstanceOf[NominalDimension]) {
          value_a
        }
        else {
          b
        }*/
      case value_a : DistanceHierarchyDimension =>
        if (value_a > b.asInstanceOf[DistanceHierarchyDimension]) {
          value_a
        }
        else {
          b
        }
      case _ => throw new IllegalArgumentException("Not supported DimensionType")
    }
  }
  
  /*def computeDomainValues( a : DimensionType, b : DimensionType ) : DimensionType = {
    a match {
      case value_a : NominalDimension =>
        value_a + b.asInstanceOf[NominalDimension]
      case value_a : DoubleDimension =>
        DoubleDimension(value_a.attributeName, 0.0)
      case _ => throw new IllegalArgumentException("Not supported DimensionType")
    }
  }*/
  
}

object DimensionTypeEnum extends Enumeration {
  type DimensionTypeEnum = Value
  val DISTANCE_HIERARCHY_NUMERIC, DISTANCE_HIERARCHY_NOMINAL = Value
  //val NUMERIC, NOMINAL, ORDINAL, DISTANCE_HIERARCHY_NUMERIC, DISTANCE_HIERARCHY_NOMINAL = Value
}

