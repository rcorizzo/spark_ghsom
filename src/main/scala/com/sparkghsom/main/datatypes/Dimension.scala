package com.sparkghsom.main.datatypes

abstract class Dimension[T](protected var _value : T) extends DimensionType { 
  def value : T = _value
  def value_=(value : T) : Unit = { _value = value  }
 
  override def toString : String = {
    value.toString()
  }
}