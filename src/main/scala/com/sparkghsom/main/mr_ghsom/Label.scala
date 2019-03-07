package com.sparkghsom.main.mr_ghsom

import com.sparkghsom.main.datatypes.DimensionType

case class Label(val name : String, val value : DimensionType) extends Serializable {
  override def equals (that : Any) : Boolean = {
    that match {
      case label2 : Label => this.name.equals(label2.name)
      case _ => false 
    }
  }
  
  override def toString : String = {
    "(" + this.name + ":" + this.value + ")"
  }
}