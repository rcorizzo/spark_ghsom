package com.sparkghsom.main.mr_ghsom


import com.sparkghsom.main.datatypes.DimensionTypeEnum
import com.sparkghsom.main.datatypes.DimensionType
import scala.collection.mutable

case class Attribute(
    name : String, 
    index : Int,
    dimensionType : DimensionTypeEnum.Value,
    var randomValueFunction : (Attribute) => DimensionType,
    var maxValue : Double = 0.0,
    var minValue : Double = 0.0,
    var domainValues : Array[String] = null
) {
  override def toString : String = {
    this.dimensionType.toString() + 
      "|" +
      this.name +
      "|" + 
      {
        if (this.dimensionType == DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC) "(" + this.minValue + "," + this.maxValue + ")"
        else "(" + this.domainValues.mkString(",") + ")"
      }
  }
}
