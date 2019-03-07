package com.sparkghsom.main.mr_ghsom

import scala.collection.mutable
import scala.collection.immutable
import scala.math.{exp,pow,abs,log,sqrt}
import com.sparkghsom.main.datatypes.DimensionType

class NeuronMT (
               private var _row : Int,
               private var _column : Int,
               private var _neuronInstanceMT : InstanceMT = null
             ) extends Serializable {
  private val _mappedInputs : mutable.ListBuffer[InstanceMT] = mutable.ListBuffer()

  private var _qe : Double = 0

  private var _mqe : Double = 0

  private var _mappedInstanceMTCount : Long = 0



  private var _id : String = _row.toString() + "," + _column.toString()

  private var _childLayerWeightVectors : Array[Array[DimensionType]] = Array.tabulate(4)(i => null)

  /* Getters and setters */
  def row : Int = _row

  def column : Int = _column

  def neuronInstanceMT : InstanceMT = _neuronInstanceMT

  def neuronInstanceMT_= (instance : InstanceMT) : Unit = _neuronInstanceMT = instance

  def id : String = _id

  def id_=(value : String) : Unit = _id = value

  def mappedInputs : mutable.ListBuffer[InstanceMT] = _mappedInputs

  def mqe : Double = _mqe

  def mqe_= (value : Double) : Unit = _mqe = value

  def qe : Double = _qe

  def qe_= (value : Double) : Unit = _qe = value

  def mappedInstanceMTCount : Long = _mappedInstanceMTCount

  def mappedInstanceMTCount_= (value : Long): Unit = _mappedInstanceMTCount = value


  def childLayerWeightVectors : Array[Array[DimensionType]] = _childLayerWeightVectors

  def childLayerWeightVectors_= (value : Array[Array[DimensionType]]) : Unit = {_childLayerWeightVectors = value}

  /* For Label SOM */
  private var _labels = scala.collection.Set[Label]()
  def labels_=(labelSet : scala.collection.Set[Label]) : Unit = _labels = labelSet
  def labels : scala.collection.Set[Label] = _labels
  /* End for label SOM */

  /* Class labels */
  private var _classLabels : scala.collection.Set[Array[String]] = scala.collection.Set[Array[String]]()
  def classLabels : scala.collection.Set[Array[String]] = _classLabels
  def classLabels_= (set : scala.collection.Set[Array[String]]): Unit = _classLabels = set
  /* End of class labels */

  def updateRowCol(i : Int, j : Int) : Unit = {
    _row = i ;
    _column = j;
    _id = _row.toString() + "," + _column.toString();
    qe = 0
    mqe = 0
    mappedInstanceMTCount = 0
  }

  /* Methods */
  def addToMappedInputs(instance : InstanceMT) {
    _mappedInputs += instance
  }

  def addToMappedInputs(instances : mutable.ListBuffer[InstanceMT]) {
    _mappedInputs ++= instances
  }

  def clearMappedInputs() {
    _mappedInputs.clear()
  }

  def getRowIndex() = { this._row }
  def getColumnIndex() = { this._column}

  def getNeighbourhoodFactor(neuron : NeuronMT, iteration : Long, maxIterations : Long, radius : Int) : Double = {

    /*
     * Neighbourhood factor = exp ( - (dist between neurons) / 2 x sigma(iteration)^2
     * 
     * sigma(iteration) = radius of the layer x exp ( - iteration / (maxIterations / log(radius of layer) ) )
     */
    val sigma = radius * (exp(( -1 * iteration) / (maxIterations / log(radius))))

    exp(-1 * (pow(abs(this.row - neuron.row) + abs(this.column - neuron.column),2)/(2*(pow(sigma,2)))))
    //exp(-(abs(this.row - neuron.row) + abs(this.column - neuron.column)) / iteration)
  }

  override def toString() : String = {
    var neuronString = "[" + row + "," + column + "]" + "(" + qe + ":" + mqe + ":" + mappedInstanceMTCount + ")" + ":" + "["

    neuronInstanceMT.attributeVector.foreach( attrib => neuronString += (attrib.toString() + ",") )

    neuronString += "]"

    neuronString
  }

  override def equals( obj : Any ) : Boolean = {
    obj match {
      case o : Neuron => o.id.equals(this.id)
      case _ => false
    }
  }

  override def hashCode : Int = _row + _column



}

object NeuronFunctionsMT {
  def getAttributeVectorWithNeighbourhoodFactor(neuron : NeuronMT, factor : Double) : Array[DimensionType] = {
    neuron.neuronInstanceMT.attributeVector.map { attrib => attrib.applyNeighbourhoodFactor(factor) }
  }

  def areNeighbours(one : NeuronMT, other : NeuronMT) : Boolean = {
    if ( (one.row == other.row) && (abs(one.column - other.column) == 1))
      true
    else if ( (one.column == other.column) && (abs(one.row - other.row) == 1))
      true
    else
      false
  }
}

object NeuronMT {
  def apply(row : Int, column: Int, neuronInstanceMT : InstanceMT) : NeuronMT = {
    new NeuronMT(row, column, neuronInstanceMT)
  }

  case class NeuronStatsMT(mqe : Double = 0, qe : Double = 0, instanceCount : Long = 0)
}