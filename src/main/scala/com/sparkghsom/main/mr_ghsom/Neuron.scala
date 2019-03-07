package com.sparkghsom.main.mr_ghsom

import scala.collection.mutable
import scala.collection.immutable
import scala.math.{exp,pow,abs,log,sqrt}
import com.sparkghsom.main.datatypes.DimensionType

class Neuron (
    private var _row : Int, 
    private var _column : Int, 
    private var _neuronInstance : Instance = null
  ) extends Serializable {
  private val _mappedInputs : mutable.ListBuffer[Instance] = mutable.ListBuffer()
  
  private var _qe : Double = 0
  
  private var _mqe : Double = 0
  
  private var _mappedInstanceCount : Long = 0
  
  
  
  private var _id : String = _row.toString() + "," + _column.toString() 
  
  private var _childLayerWeightVectors : Array[Array[DimensionType]] = Array.tabulate(4)(i => null)
  
  /* Getters and setters */
  def row : Int = _row 
  
  def column : Int = _column
  
  def neuronInstance : Instance = _neuronInstance 
  
  def neuronInstance_= (instance : Instance) : Unit = _neuronInstance = instance 
  
  def id : String = _id
  
  def id_=(value : String) : Unit = _id = value 
  
  def mappedInputs : mutable.ListBuffer[Instance] = _mappedInputs
  
  def mqe : Double = _mqe
  
  def mqe_= (value : Double) : Unit = _mqe = value
  
  def qe : Double = _qe
  
  def qe_= (value : Double) : Unit = _qe = value
  
  def mappedInstanceCount : Long = _mappedInstanceCount

  def mappedInstanceCount_= (value : Long): Unit = _mappedInstanceCount = value
  
    
  def childLayerWeightVectors : Array[Array[DimensionType]] = _childLayerWeightVectors
  
  def childLayerWeightVectors_= (value : Array[Array[DimensionType]]) : Unit = {_childLayerWeightVectors = value}
  
  /* For Label SOM */
  private var _labels = scala.collection.Set[Label]()
  def labels_=(labelSet : scala.collection.Set[Label]) : Unit = _labels = labelSet
  def labels : scala.collection.Set[Label] = _labels
  /* End for label SOM */
  
  /* Class labels */
  private var _classLabels : scala.collection.Set[String] = scala.collection.Set[String]()
  def classLabels : scala.collection.Set[String] = _classLabels
  def classLabels_= (set : scala.collection.Set[String]): Unit = _classLabels = set
  /* End of class labels */
  
  def updateRowCol(i : Int, j : Int) : Unit = { 
    _row = i ; 
    _column = j; 
    _id = _row.toString() + "," + _column.toString();  
    qe = 0
    mqe = 0
    mappedInstanceCount = 0
  }
  
  /* Methods */
  def addToMappedInputs(instance : Instance) {
    _mappedInputs += instance
  }
  
  def addToMappedInputs(instances : mutable.ListBuffer[Instance]) {
    _mappedInputs ++= instances
  }
  
  def clearMappedInputs() {
    _mappedInputs.clear()
  }

  def getRowIndex() = { this._row }
  def getColumnIndex() = { this._column}

  def getNeighbourhoodFactor(neuron : Neuron, iteration : Long, maxIterations : Long, radius : Int) : Double = {
    
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
    var neuronString = "[" + row + "," + column + "]" + "(" + qe + ":" + mqe + ":" + mappedInstanceCount + ")" + ":" + "["
    
    neuronInstance.attributeVector.foreach( attrib => neuronString += (attrib.toString() + ",") )
    
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

object NeuronFunctions {
  def getAttributeVectorWithNeighbourhoodFactor(neuron : Neuron, factor : Double) : Array[DimensionType] = {
    neuron.neuronInstance.attributeVector.map { attrib => attrib.applyNeighbourhoodFactor(factor) }  
  }
  
  def areNeighbours(one : Neuron, other : Neuron) : Boolean = {
    if ( (one.row == other.row) && (abs(one.column - other.column) == 1)) 
      true
    else if ( (one.column == other.column) && (abs(one.row - other.row) == 1))
      true
    else 
      false
  }
}

object Neuron {
  def apply(row : Int, column: Int, neuronInstance : Instance) : Neuron = {
    new Neuron(row, column, neuronInstance)
  }
  
  case class NeuronStats(mqe : Double = 0, qe : Double = 0, instanceCount : Long = 0)
}