package com.sparkghsom.main.datatypes

import org.apache.commons.lang.IllegalClassException
import scala.util.Random
import scala.compat.Platform
import com.sparkghsom.main.mr_ghsom.Attribute
import scala.math.abs
import com.sparkghsom.main.globals.GHSomConfig

case class DistanceHierarchyElem(
    symbol : String,
    value : Double ) extends Ordered[DistanceHierarchyElem]{
  
  /**
   * Returns < 0 when this < that
   *         > 0 when this > that 
   *         = 0 when this == that
   */
  override def compare( that : DistanceHierarchyElem ) = {
    val x = this.value - that.value

    if (x == 0 && !this.symbol.equals(that.symbol)) {
      throw new UnsupportedOperationException("DistanceHierarchyElem")
    }
    else {
      x.asInstanceOf[Integer]
    }
  }
  
  def equals(that : DistanceHierarchyElem) : Boolean = {
    if (this.symbol.equals(that.symbol) && 
        this.value == that.value) 
      true
    else 
      false
  }
  
  override def toString : String = this.symbol + ":" + this.value
  
  def -( that : DistanceHierarchyElem ) : DistanceHierarchyElem = {
    /*
     *  When symbols are equal it is similar to vector subtraction, so do it normally
     *    (A, 2) - (A,1) = (A, 1)
     *  When symbols are not equal : 
     *    Vector of A - B => A + (-B) i.e. Reverse the direction of pull of B i.e. push it towards A 
     *    |A| + |B|
     *    (A,2) - (B,1) = (A, 3)
     */
    
    if (GHSomConfig.ignore_unknown) {
      if (this.symbol.equals("UNKNOWN") || this.symbol.equals("?") || this.symbol.equals("Not in universe"))
        return DistanceHierarchyElem(that.symbol, that.value)
      else if (that.symbol.equals("UNKNOWN") || that.symbol.equals("?"))
        return DistanceHierarchyElem(this.symbol, this.value)
    }
    
    if (this.symbol.equals(that.symbol))
      new DistanceHierarchyElem(this.symbol, abs(this.value - that.value))
    else
      new DistanceHierarchyElem(this.symbol, this.value + that.value)
  }
  
  def +( that : DistanceHierarchyElem ) : DistanceHierarchyElem = {
    /*
     * When symbols are equal it is similar to vector addition
     *  (A, 2) + (A, 1) = (A, 3)
     * When symbols are not equal, they try to cancel out each other. 
     * So the result is (symbol one with the bigger magnitude, abs( value difference ))
     *   (A,2) + (B,1) = (A, |2 - 1|)
     *   (A,2) + (B,3) = (B, |2 - 3|) since B has greater magnitude
     * If the resultant magnitude is 0, the symbol doesnt matter
     */
    if (GHSomConfig.ignore_unknown) {
      if (this.symbol.equals("UNKNOWN") || this.symbol.equals("?"))
        return DistanceHierarchyElem(that.symbol, that.value)
      else if (that.symbol.equals("UNKNOWN") || that.symbol.equals("?"))
        return DistanceHierarchyElem(this.symbol, this.value)
    }

    if (this.symbol.equals(that.symbol))
      new DistanceHierarchyElem(this.symbol, this.value + that.value)
    else {
      val valueDifference = this.value - that.value
      if(valueDifference < 0) 
        new DistanceHierarchyElem(that.symbol, valueDifference * -1)
      else
        new DistanceHierarchyElem(this.symbol, valueDifference)
    }
  }
  
  def /( num : Double) : DistanceHierarchyElem = {
    DistanceHierarchyElem(this.symbol, this.value / num)
  }
  
  def *( num : Double) : DistanceHierarchyElem = {
    if (GHSomConfig.ignore_unknown) {
      if (this.symbol.equals("UNKNOWN") || this.symbol.equals("?")) 
        DistanceHierarchyElem(this.symbol, 0)
    }
    DistanceHierarchyElem(this.symbol, this.value * num)
  }
  
  def getDissimilarityValue(that : DistanceHierarchyElem) : Double = {

    if (GHSomConfig.ignore_unknown) {
      if (this.symbol.equals("UNKNOWN") || this.symbol.equals("?"))
        return that.value + GHSomConfig.link_weight // UNKNOWN
      else if (that.symbol.equals("UNKNOWN") || that.symbol.equals("?"))
        return this.value + GHSomConfig.link_weight // UNKNOWN

    }
   /*
    * Ideally this should be :
    * this.value + that.value - 2 * distance_of_LCP(this, that)
    * For prototype, I am assuming a single level hierarchy, so the 
    * distance_of_LCP = 0
    */
    if (!this.symbol.equals(that.symbol))
      this.value + that.value 
    else
      abs(this.value - that.value)
  }
  
  def getUpdateValue(oldValue : DistanceHierarchyElem) : DistanceHierarchyElem = {
    var updateValue : DistanceHierarchyElem = DistanceHierarchyElem(this.symbol, this.value)
    
    if (GHSomConfig.ignore_unknown) {
      if (this.symbol.equals("UNKNOWN") || this.symbol.equals("?")) {
        updateValue = DistanceHierarchyElem(oldValue.symbol, oldValue.value)
      }
    }
    updateValue
  }
  
  def cloneMe : DistanceHierarchyElem = 
    DistanceHierarchyElem(this.symbol, this.value)
    
  def getOriginalValue( minValue : Double, maxValue : Double ) = {
    this.value * (maxValue - minValue) + minValue
  }
  
}

class DistanceHierarchyDimension (
  private val _attributeName : String,
  private val _attributeValue : DistanceHierarchyElem,
  private val _attributeType : DimensionTypeEnum.Value
) extends Dimension[DistanceHierarchyElem](_attributeValue) {
  
  def attributeName : String = _attributeName
  
  def attributeType : DimensionTypeEnum.Value = _attributeType 
  
  def attributeValue : DistanceHierarchyElem = _attributeValue
  
  private var _missingValue = true
  
  def missingValue : Boolean = _missingValue 
  def missingValue_=(value : Boolean) : Unit = { _missingValue = value }
  
  override def equals( d2 : DimensionType ) : Boolean = {
     d2 match {
       case other : DistanceHierarchyDimension => other.value.equals(this.value) 
       case _ => false 
     } 
  }
  
  override def compare( that : DimensionType ) : Int = {
    that match {
      case thatObj : DistanceHierarchyDimension => {
                                          this.value.compare(thatObj.value)
                                        }
      case _ => throw new IllegalClassException("Illegal class in DistanceHierarchyDimension")
    }
  }
  
  def !=( that : Double ) : Boolean = {
    throw new UnsupportedOperationException("DistanceHierarchyDimension")
  }
  
  def -( that : DistanceHierarchyDimension ) = {
    throw new UnsupportedOperationException("DistanceHierarchyDimension")
  }
  
  override def -( that : DimensionType ) : DimensionType = {
    that match {
      case d : DistanceHierarchyDimension => DistanceHierarchyDimension(this.attributeName, this.value - d.value, this.attributeType) 
      case _ => throw new IllegalClassException("Illegal class in DistanceHierarchyDimension")
    }
  }
  
  override def /( that : DimensionType ) = {
    throw new UnsupportedOperationException("DistanceHierarchyDimension")
  }
  
  override def /( num : Long ) : DimensionType = {
    DistanceHierarchyDimension(this.attributeName, this.value / num, this.attributeType)
  } 

  override def /( num : Double) : DimensionType = {
    DistanceHierarchyDimension(this.attributeName, this.value / num, this.attributeType)
  } 
  
  override def !=( that : DimensionType ) : Boolean = {
    that match {
      case d : DistanceHierarchyDimension => this.value != d.value
      case _ => false
    }
  }
  
  override def ==( that : DimensionType ) : Boolean = {
    that match {
      case d : DistanceHierarchyDimension => this.value == d.value
      case _ => false
    }
  }
  
  override def <( that : DimensionType ) : Boolean = {
    that match {
      case d : DistanceHierarchyDimension => this.value < d.value
      case _ => false
    }
  }
  
  override def <=( that : DimensionType ) : Boolean = {
    that match {
      case d : DistanceHierarchyDimension => this.value <= d.value
      case _ => false
    }
  }
  
  override def >=( that : DimensionType ) : Boolean = {
    that match {
      case d : DistanceHierarchyDimension => this.value >= d.value
      case _ => false
    }
  }
  
  override def >( that : DimensionType ) : Boolean = {
    that match {
      case d : DistanceHierarchyDimension => this.value > d.value
      case _ => false
    }
  }
  
  override def toString : String = {
    this.value.toString
  }
  
  override def + (that : DimensionType) : DimensionType = {
    that match {
      case d : DistanceHierarchyDimension => new DistanceHierarchyDimension(this.attributeName, this.value + d.value, this.attributeType)
      case _ => throw new IllegalClassException("Illegal class in DistanceHierarchyDimension")
    }
  }
  
  
  override def * (that : DimensionType) : DimensionType = {
    throw new UnsupportedOperationException("DistanceHierarchyDimension")
  }
  
  override def cloneMe : DimensionType = {
    new DistanceHierarchyDimension(this.attributeName, this.value.cloneMe, this.attributeType)
  }
  
  def isZero : Boolean = {
    this.value == 0
  }
  
  override def hashCode : Int = this.value.asInstanceOf[Int]

  override def getDissimilarityValue(that : DimensionType) : Double = {
    that match {
      case d : DistanceHierarchyDimension => this.value.getDissimilarityValue(d.value)
      case _ => throw new IllegalClassException("Illegal class in DistanceHierarchyDimension")
    }
  }
  override def applyNeighbourhoodFactor(factor : Double) : DimensionType = {
    new DistanceHierarchyDimension(this.attributeName, this.value * factor, this.attributeType)
  }
  
  override def divideByCumulativeNeighbourhoodFactor(factor : Double) : DimensionType = {
    new DistanceHierarchyDimension(this.attributeName, this.value / factor, this.attributeType)
  }
  
  override def getUpdateValue(oldValue : DimensionType) : DimensionType = {
    // old value not required and can be ignored
    
    oldValue match {
      case d : DistanceHierarchyDimension => DistanceHierarchyDimension(this.attributeName, this.value.getUpdateValue(d.value), this.attributeType) 
      case _ => throw new IllegalClassException("Illegal class in DistanceHierarchyDimension")
    }
    
    
  }
  
  override def getFreshDimension : DimensionType = {
    DistanceHierarchyDimension(this.attributeName, this.value.cloneMe, this.attributeType)
  }
  
  override def avgForGrowingLayerWith(that : DimensionType) : DimensionType = {
    that match {
      case other : DistanceHierarchyDimension => DistanceHierarchyDimension(this.attributeName, (this.value + other.value) / 2, this.attributeType)
      case _ => throw new IllegalClassException("Illegal class in DistanceHierarchyDimension")
    }
  }
  
  override def getImagePixelValue (domainValues : Array[_ <: Any] = null) : Double = {
    if (this.attributeType == DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC)
      this.value.getOriginalValue(
        domainValues(0).asInstanceOf[Double], 
        domainValues(1).asInstanceOf[Double]
      )
    else {
      val domainValuesStr = domainValues.map(_.asInstanceOf[String])
      domainValuesStr.indexOf(this.value.symbol)
    }
  }
}

object DistanceHierarchyDimension {
  
  //val MinValue = DoubleDimension(value = Double.MinValue)
  //val MaxValue = DoubleDimension(value = Double.MaxValue)
  
  val randomGenerator = new Random(Platform.currentTime)
  
  def apply(name : String = "", value : DistanceHierarchyElem, attributeType : DimensionTypeEnum.Value ) : DistanceHierarchyDimension = {
    new DistanceHierarchyDimension(name, value, attributeType)
  }
  
  def apply() : DistanceHierarchyDimension = {
    new DistanceHierarchyDimension("", DistanceHierarchyElem("",0.0),DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL)
  }
  
  def getRandomDimensionValue(attribute : Attribute) : DistanceHierarchyDimension = {
    attribute.dimensionType match {
      case DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC => 
        DistanceHierarchyDimension(
          attribute.name, 
          DistanceHierarchyElem("+", randomGenerator.nextDouble()),
          DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC
        )
      case DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL => 
        DistanceHierarchyDimension(
          attribute.name, 
          DistanceHierarchyElem(attribute.domainValues(randomGenerator.nextInt(attribute.domainValues.size)), randomGenerator.nextDouble * (GHSomConfig.link_weight/4)),
          DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL
        )
    }
  }
  
  def getMax( a : DistanceHierarchyDimension, b : DistanceHierarchyDimension) : DistanceHierarchyDimension = {
    if (a > b) a 
    else b 
  }
  
  def getMin( a : DistanceHierarchyDimension, b : DistanceHierarchyDimension) : DistanceHierarchyDimension = {
    if (a < b) a 
    else b 
  }
}

