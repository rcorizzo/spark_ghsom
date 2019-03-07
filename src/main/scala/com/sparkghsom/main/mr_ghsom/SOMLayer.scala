package com.sparkghsom.main.mr_ghsom

import com.sparkghsom.main.utils.Utils
import com.sparkghsom.main.globals.{GHSomConfig, SparkConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions

import scala.collection.{Map, Set, immutable, mutable}
import scala.math.{abs, ceil, exp, log, max, min, pow, sqrt}
import org.apache.commons
import org.apache.commons.io.FileUtils
import java.io.{File, FileInputStream}

import scala.compat.Platform
import com.sparkghsom.main.datatypes.{DimensionType, DimensionTypeEnum}
import com.sparkghsom.main.mr_ghsom.SOMLayer.NeuronUpdate

class SOMLayer private (
  private val _layerID : Int, 
  private var _rowDim : Int, 
  private var _colDim : Int,
  private val _parentNeuron : Neuron,
  //private val _parentNeuronID : String,
  private val _parentLayer : Int,
  //private val parentNeuronQE : Double, 
  //private val parentNeuronMQE : Double,  // mqe_change
  attributeVectorSize : Int,
  initializationInstances : Array[Instance] 
) extends Serializable {

  private var neurons : Array[Array[Neuron]] = {
    Array.tabulate(_rowDim, _colDim)(
      (rowParam,colParam) => {
        val neuron = Neuron(row = rowParam, 
          column = colParam, 
          neuronInstance = Instance(0,"neuron-[" + rowParam.toString() +","+ colParam.toString() + "]",
              initializationInstances(rowParam * _rowDim + colParam).attributeVector
            //Utils.generateArrayOfGivenValue(value, attributeVectorSize)
            )
        )
        neuron
      }
    )
  }

  def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }

  def layerID : Int = _layerID

  def parentLayer : Int = _parentLayer

  def parentNeuron : Neuron = _parentNeuron

  def gridSize() {
    println("LAYER SIZE: " + _rowDim + "x" + _colDim)
  }

  def totalNeurons : Long = _rowDim * _colDim
  
  def rowDim : Int = _rowDim
  
  def colDim : Int = _colDim
  
  def getNeuron(row : Int, col : Int) : Neuron = { this.neurons(row)(col) }

  def getNeuronMap() = { this.neurons }

  private case class NeuronPair ( var neuron1 : Neuron, var neuron2 : Neuron ) {

    if (isSameRow) {
      if (neuron1.column > neuron2.column) {
        val temp = neuron1
        neuron1 = neuron2
        neuron2 = temp
      }
    }
    else {
      if (neuron1.row > neuron2.row) {
        val temp = neuron1
        neuron1 = neuron2
        neuron2 = temp
      }
    }

    override def equals( obj : Any ) : Boolean = {
      obj match {
        case o : NeuronPair => {
          (this.neuron1.equals(o.neuron1) && this.neuron2.equals(o.neuron2)) || 
          (this.neuron1.equals(o.neuron2) && this.neuron2.equals(o.neuron1))  
          /*
          ((this.isSameRow) && (o.isSameRow) && (this.neuron1.column == o.neuron1.column) && (this.neuron2.column == o.neuron2.column)) || 
          ((this.isSameCol) && (o.isSameCol) && (this.neuron1.row == o.neuron1.row) && (this.neuron2.row == o.neuron2.row)) 
          */

        }
        case _ => false 
      }
    }

    override def hashCode : Int = neuron1.hashCode() + neuron2.hashCode()

    def isSameRow : Boolean = {
      if (neuron1.row == neuron2.row)
        true
      else 
        false
    }

    def isSameCol : Boolean = {
      if (neuron1.column == neuron2.column) 
        true
      else
        false
    }

    override def toString : String = {
      "Neuron1: " + neuron1.id + ", Neuron2: " + neuron2.id
    }
  }

  def display() {
    println("Display Layer")
    println("Layer Details -> id : " + 
      this._layerID + 
      ";parent Layer : " + 
      this.parentLayer + 
      ";parent Neuron" +
      this._parentNeuron.id)
    println("Layer:")
    neurons.foreach( neuronRow => neuronRow.foreach(neuron => println(neuron))) 
  }

  def clearMappedInputs {
    for (neuronRow <- neurons) {
      for (neuron <- neuronRow) {
        neuron.clearMappedInputs()
      }
    }  
  }

  
  def train(dataset : RDD[Instance], maxIterations : Long) {

    // TODO : neurons could be made broadcast variable
    val neurons = this.neurons

    var iteration = 0

    //val radius = (sqrt(pow(this._rowDim, 2) + pow(this._colDim,2)).asInstanceOf[Int])  
    val radius = (sqrt(pow(this._rowDim, 2) + pow(this._colDim,2)).asInstanceOf[Int] / 2)  
    //max(this._rowDim, this._colDim)
    //val radius = min(this._rowDim, this._colDim)
    
    while( iteration < maxIterations ) {
      //println("Sigma = " + (radius * (exp(( -1 * iteration) / (maxIterations / log(radius)))))) 
      /**** MapReduce Begins ****/
      // neuronUpdatesRDD is a RDD of (numerator, denominator) for the update of neurons at the end of epoch
      // runs on workers
      val neuronUpdatesRDD = {
        dataset.mapPartitions(
          partition => {
            partition.flatMap {
              instance => {
                val bmu : Neuron = SOMLayerFunctions.findBMU(neurons, instance)
                neurons.flatten.map { neuron =>
                  val neighbourhoodFactor = neuron.getNeighbourhoodFactor(bmu, iteration, maxIterations, radius)
                  val neuronUpdateNumNDen: NeuronUpdate = SOMLayer.NeuronUpdate (
                    InstanceFunctions.getInstanceWithNeighbourhoodFactor(instance, neighbourhoodFactor), 
                    neighbourhoodFactor
                  )
                  ( neuron.id, neuronUpdateNumNDen )
                }
              }
            }
          }
        )
      }
      /*
      val neuronUpdatesRDD = 
      dataset.flatMap { instance => 
        val bmu : Neuron = SOMLayerFunctions.findBMU(neurons, instance)

        val temp = neurons.flatten
        temp.map { neuron => 
          val neighbourhoodFactor = neuron.getNeighbourhoodFactor(bmu, iteration, maxIterations, radius)
          (
            neuron.id, 
            SOMLayer.NeuronUpdate (
              InstanceFunctions.getAttributeVectorWithNeighbourhoodFactor(instance, neighbourhoodFactor), 
              neighbourhoodFactor
            )
          )
        }
      }
      * 
      */

      val updatedModel: Map[String, Instance] = new PairRDDFunctions[String, SOMLayer.NeuronUpdate](neuronUpdatesRDD) // create a pairrdd
        .reduceByKey(SOMLayerFunctions.combineNeuronUpdates) // combines/shuffles updates from all workers
        .mapValues(SOMLayerFunctions.computeUpdatedNeuronVector) // updates all values (neuron ID -> wt. vector)
        .collectAsMap()  // converts to map of neuronID -> wt. vector // returns to driver

        /**** MapReduce Ends ****/

        // Running on driver                                   
        /* 
         * At the driver:
         * Perform update to neurons 
         */

        for (i <- 0 until neurons.size) {
          for (j <- 0 until neurons(0).size) {
            if(updatedModel.keySet.contains(i.toString() + "," + j.toString())) {
              neurons(i)(j).neuronInstance =
                InstanceFunctions.computeUpdateInstance(
                  neurons(i)(j).neuronInstance,
                  updatedModel(i.toString() + "," + j.toString())
                )
            }
          }
        } 

        iteration += 1
    }
  }

  /**
   * Computes the statistics for the layer such as 
   *  - number of mapped instances
   *  - mqe
   *  - qe
   */
  def computeStatsForLayer(dataset : RDD[Instance]) {

    val neurons = this.neurons

    /***** MapReduce Begins *****/
    // runs on workers 
    // creates a map of (neuron id -> (quantization error, instance count = 1)) 
    
    val neuronsQE = new PairRDDFunctions[String, Neuron.NeuronStats]( 
    //val neuronsQE = new PairRDDFunctions[String, (Double, Long, Set[String])]( // classlabel_map
      dataset.map { instance => 
        val bmu = SOMLayerFunctions.findBMU(neurons, instance)
        val qe = bmu.neuronInstance.getDistanceFrom(instance)
        (bmu.id, Neuron.NeuronStats(qe = qe, instanceCount = 1L))
      }
    )

    // combines / reduces (adds) all quantization errors and instance counts from each mapper
      val neuronMQEs = neuronsQE.reduceByKey(SOMLayerFunctions.combineNeuronsQE)
                              .mapValues(SOMLayerFunctions.computeMQEForNeuron)

    neuronMQEs.collectAsMap
              .map(updateNeuronStats) // return to driver
    /***** MapReduce Ends *****/
  }

  def checkMQE(tau1 : Double): (Boolean, Double, Neuron) = {
    var sum_mqe_m : Double = 0 // mqe_change
    var mappedNeuronsCnt : Int = 0 
    var maxMqeNeuron = neurons(0)(0) //mqe_change
    var maxMqe : Double = 0 //mqe_change

    for (neuronRow <- neurons) {
      for (neuron <- neuronRow) {
        if (neuron.mappedInstanceCount != 0) {
          sum_mqe_m += neuron.mqe
          mappedNeuronsCnt += 1
          if (neuron.mqe > maxMqe) {
            maxMqeNeuron = neuron
            maxMqe = neuron.mqe
          }
        }     
      }
    }

    val MQE_m = sum_mqe_m / mappedNeuronsCnt // mqe_change
    println("Criterion : ")
    println("sum_mqe_m / mappedNeuronsCnt : " + sum_mqe_m + "/" + mappedNeuronsCnt + "=" + MQE_m) //mqe_change
    println("tau1 x parentNeuronMQE : " + tau1 + "x" + this._parentNeuron.mqe + "=" + tau1 * this._parentNeuron.mqe)

    if (MQE_m > tau1 * this._parentNeuron.mqe) {  
      (true, MQE_m , maxMqeNeuron)
    }
    else {
      (false, MQE_m , maxMqeNeuron)
    }

  } 

  def checkQE(tau1 : Double): (Boolean, Double, Neuron) = {
    //var sum_mqe_m : Double = 0 // mqe_change
    var sum_qe_m : Double = 0
    var mappedNeuronsCnt : Int = 0 
    //var maxMqeNeuron = neurons(0)(0) //mqe_change
    var maxQeNeuron = neurons(0)(0)
    //var maxMqe : Double = 0 //mqe_change
    var maxQe : Double = 0

    for (neuronRow <- neurons) {
      for (neuron <- neuronRow) {
        if (neuron.mappedInstanceCount != 0) {
          sum_qe_m += neuron.qe
          mappedNeuronsCnt += 1
          if (neuron.qe > maxQe) {
            maxQeNeuron = neuron
            maxQe = neuron.qe
          }
          /*
           * mqe_change
           sum_mqe_m += neuron.mqe
           mappedNeuronsCnt += 1

           if (neuron.mqe > maxMqe) {
             maxMqeNeuron = neuron
             maxMqe = neuron.mqe
           }
           */
        }     
      }
    }

    //val MQE_m = sum_mqe_m / mappedNeuronsCnt // mqe_change
    val MQE_m = sum_qe_m / mappedNeuronsCnt
    println("Criterion : ")
    //println("sum_qe_m / mappedNeuronsCnt : " + sum_mqe_m + "/" + mappedNeuronsCnt + "=" + MQE_m) //mqe_change
    println("sum_qe_m / mappedNeuronsCnt : " + sum_qe_m + "/" + mappedNeuronsCnt + "=" + MQE_m)
    println("tau1 x parentNeuronQE : " + tau1 + "x" + this._parentNeuron.qe + "=" + tau1 * this._parentNeuron.qe)
    //println("tau1 x parentNeuronMQE : " + tau1 + "x" + this.parentNeuronMQE) //mqe_change

    if (MQE_m > tau1 * this._parentNeuron.qe) {  
      (true, MQE_m , maxQeNeuron)
    }
    else {
      (false, MQE_m , maxQeNeuron)
    }

  }

  def growSingleRowColumn(errorNeuron : Neuron) {

    val dissimilarNeighbour = getMostDissimilarNeighbour(errorNeuron)

    neurons = getGrownLayer(NeuronPair(errorNeuron, dissimilarNeighbour))

  }

  /**
   * Grows the layer adding rows/columns. 
   * This method runs on the driver completely
   * @param tau1 parameter controlling the horizontal growth of a layer
   * @param mqe_u mean quantization error of the parent neuron of the layer 
   */
  def growMultipleCells(tau1 : Double) {
    var neuronPairSet : Set[NeuronPair] = null
    if (GHSomConfig.mqe_criterion)
      neuronPairSet = getNeuronAndNeighbourSetForGrowing(tau1 * this._parentNeuron.mqe)// mqe_change
    else 
      neuronPairSet = getNeuronAndNeighbourSetForGrowing(tau1 * this._parentNeuron.qe)
    if (GHSomConfig.debug) {
      println("Neurons to Expand")  
      for (pair <- neuronPairSet) {
        println(pair)
      }
    }

    neurons = getGrownLayer(neuronPairSet)
  }

  def getNeuronsForHierarchicalExpansion(criterion : Double, instanceCount : Long) : mutable.Set[Neuron] = {
    val neuronSet = new mutable.HashSet[Neuron]()
    
    if (GHSomConfig.mqe_criterion) {
      neurons.foreach { 
        neuronRow => 
          neuronRow.foreach { 
            neuron => 
              if (neuron.mappedInstanceCount > GHSomConfig.hierarchical_count_factor * instanceCount &&
                neuron.mqe > criterion ) { 
                  neuronSet += neuron
                  neuron.childLayerWeightVectors = getWeightVectorsForChildLayer(neuron)
                }
          }   
      }
    }
    else {
      neurons.foreach { 
        neuronRow => 
          neuronRow.foreach { 
            neuron => 
              if (neuron.mappedInstanceCount > GHSomConfig.hierarchical_count_factor * instanceCount &&
                neuron.qe > criterion /* mqe_change neuron.mqe > tau2 * parentNeuronMQE */) { 
                  neuronSet += neuron
                  neuron.childLayerWeightVectors = getWeightVectorsForChildLayer(neuron)
                }
          }   
      }
    }
    neuronSet
  }

  def getRDDForHierarchicalExpansion(
    //addToDataset : RDD[(Int, String, Instance)], 
    dataset : RDD[Instance], 
    neuronsToExpand : mutable.Set[Neuron]
  ) : RDD[GHSom.LayerNeuronRDDRecord] = {

    val context = dataset.context

    val neurons = this.neurons

    val layerID = this.layerID

    val neuronIdsToExpand = neuronsToExpand.map(neuron => neuron.id)
    //var origDataset = addToDataset
    
    dataset.map(
      instance => {
        val bmu = SOMLayerFunctions.findBMU(neurons, instance)
        if (neuronIdsToExpand.contains(bmu.id)) {
          GHSom.LayerNeuronRDDRecord(layerID, bmu.id, instance)
        }
        else 
          null
      }
    )
    .filter(record => record != null)
  }

  def initializeLayerWithParentNeuronWeightVectors {
    if (this.rowDim != 2 || this.colDim != 2)
      return
      
    for (i <- 0 until this.rowDim) {
      for (j <- 0 until this.colDim) {
        neurons(i)(j).neuronInstance = Instance(0,neurons(i)(j).neuronInstance.label,
                                                this._parentNeuron.childLayerWeightVectors(i * colDim + j))
      }
    }
  }
  
  /* LABEL_SOM
  def computeLabels(dataset : RDD[Instance], headers : Array[String]) {
    
    var attributeHeaders = headers 
    
    if (attributeHeaders == null) {
      attributeHeaders = Array.tabulate(this.attributeVectorSize)(index => index.toString())
    }
    
    val neurons = this.neurons

    /***** MapReduce Begins *****/
    // runs on workers 
    // creates a map of (neuron id -> (sum of instances,  instance count = 1)) 
    
    val neuronLabels = new PairRDDFunctions[String, (Instance, Instance, Long)]( 
      dataset.map { instance => 
        val bmu = SOMLayerFunctions.findBMU(neurons, instance)
        val sumInstance = Instance("sumInstance", instance.attributeVector)
        val qeInstance = InstanceFunctions.getQEInstance(bmu.neuronInstance, instance)
        (bmu.id,(sumInstance, qeInstance, 1))
      }
    )
    
    // combines / reduces (adds) all quantization errors and instance counts from each mapper
    val neuronMeanNQEs = neuronLabels.reduceByKey(SOMLayerFunctions.combineNeuronsMeanAndQEForLabels)
                                     .mapValues(SOMLayerFunctions.computeMeanAndQEForLabels)
    
    val neuronUpdatesMap = neuronMeanNQEs.collectAsMap
    
    val neuronLabelMap = neuronUpdatesMap.mapValues( tup => getNeuronLabelSet(tup._1, tup._2, attributeHeaders) )
    
    neuronLabelMap.map(updateNeuronLabels) // return to driver
              
    /***** MapReduce Ends *****/
  }
  * 
  */


  def computeClassLabelsRegression(dataset : RDD[Instance]): Map[(Int, Int), String] = {

    val neurons = this.neurons

    val neuronsClassLabels = new PairRDDFunctions[String, Set[String]](
      dataset.map { instance =>
        val bmu = SOMLayerFunctions.findBMU(neurons, instance)
        (bmu.id,Set(instance.label)) // classlabel_map
      }
    )

    // combines / reduces (adds) all quantization errors and instance counts from each mapper
    val neuronMQEs: RDD[(String, Set[String])] = neuronsClassLabels.reduceByKey(SOMLayerFunctions.computeNeuronClassLabels)

    // NEW START *****************************************************************************

    val neuronsClassLabelsNew = new PairRDDFunctions[(Int,Int),(String,Int)](
      dataset.map { instance =>
        val bmu: Neuron = SOMLayerFunctions.findBMU(neurons, instance)
        ((bmu.row,bmu.column),(instance.label,1)) // classlabel_map
      }
    )

    val sumClasses: RDD[((Int, Int), (String, Int))] = neuronsClassLabelsNew.reduceByKey((x, y) => {
      val doubleTargetX = x._1.toDouble
      val doubleTargetY = y._1.toDouble
      val combinedString = (doubleTargetX + doubleTargetY).toString
      (combinedString, x._2+y._2)
    })

    val avgClasses: RDD[((Int, Int), String)] = sumClasses.map({
      case((row, column),(sumLabel,count)) => {
        val avgLabel = (sumLabel.toDouble/count).toString
      ((row,column),avgLabel)
      }
    })

    // NEW END *****************************************************************************

    // combines / reduces (adds) all quantization errors and instance counts from each mapper

    neuronMQEs.collectAsMap
      .map(updateNeuronClassLabels) // return to driver
    /***** MapReduce Ends *****/


    avgClasses.collectAsMap()
  }

  
  def computeClassLabels(dataset : RDD[Instance]): Map[(Int, Int), String] = {

    val neurons = this.neurons

    /***** MapReduce Begins *****/
    // runs on workers 
    // creates a map of (neuron id -> (quantization error, instance count = 1)) 
    
    val neuronsClassLabels = new PairRDDFunctions[String, Set[String]](
      dataset.map { instance => 
        val bmu = SOMLayerFunctions.findBMU(neurons, instance)
        (bmu.id,Set(instance.label)) // classlabel_map
      }
    )

        // combines / reduces (adds) all quantization errors and instance counts from each mapper
    val neuronMQEs = neuronsClassLabels.reduceByKey(SOMLayerFunctions.computeNeuronClassLabels)

    // NEW START *****************************************************************************

    val neuronsClassLabelsNew = new PairRDDFunctions[(Int,Int,String),Int](
      dataset.map { instance =>
        val bmu: Neuron = SOMLayerFunctions.findBMU(neurons, instance)
        ((bmu.row,bmu.column,instance.label),1) // classlabel_map
      }
    )

    val sumClasses: RDD[((Int,Int,String), Int)] = neuronsClassLabelsNew.reduceByKey((x, y) => x+y)

    val reMapped: RDD[((Int,Int),(String, Int))] = sumClasses.map {
      case((bmuRow,bmuCol,className),count) => {
        ((bmuRow,bmuCol),(className,count))
      }
    }

    val finalClasses: RDD[((Int,Int),(String, Int))] = reMapped.reduceByKey((x, y) => {
      if (x._2 > y._2)
        (x._1,x._2)
      else
        (y._1,y._2)
    })

    val updatedMap: RDD[((Int, Int), String)] = finalClasses.map({
      case((row,col),(classAttribute,count)) => {
        println(row + " " + col + " " + " " + classAttribute + " " + count)
        ((row,col),classAttribute)
      }
    })

    //val finalClassesRemap = finalClasses.map(x => (x._1,x._2._1))

    // NEW END *****************************************************************************

    // combines / reduces (adds) all quantization errors and instance counts from each mapper

    neuronMQEs.collectAsMap
              .map(updateNeuronClassLabels) // return to driver
    /***** MapReduce Ends *****/


    updatedMap.collectAsMap()
  }

  def computeTopographicalError(dataset : RDD[Instance]) : Double = {

    val neurons = this.neurons

    /***** MapReduce Begins *****/
    // runs on workers 
    // creates a map of (neuron id -> (quantization error, instance count = 1)) 

    val neuronTopographicErrorMap: RDD[Double] =

      dataset.map { instance =>
        val (bmu, nextBmu) = SOMLayerFunctions.findBMUAndNextBMU(neurons, instance)
        var error = 0.0
        if (!NeuronFunctions.areNeighbours(bmu,nextBmu)) {
          error = 1
        }
        error
      }

    neuronTopographicErrorMap.reduce(_ + _)

  }

  
  def dumpToFile( attributes : Array[Attribute] ) {

      /* Codebook vectors */
      val strNeurons = 
        neurons.map(row => {
                    row.map(neuron => neuron)
                       .mkString("\n")
                    }
             )
             .mkString("\n")

      val filename = "output/SOM/SOMLayer_CodebookVectors" + this.layerID + "_" + this._parentLayer + "_" + this._parentNeuron.id + ".data"

      val encoding : String = null

      FileUtils.writeStringToFile(new File(filename), strNeurons, encoding)
      
      /* Dump UMatrix and component planes */
      
      dumpUMatrix(attributes)
      
      dumpComponentPlanes(attributes)
      
      /* Mapped Instance Count */
      val mappedInstances = 
            neurons.map(row => 
                       row.map(neuron => neuron.mappedInstanceCount.toString())
                          .mkString("|")
                   )
                   .mkString("\n")

      val mappedInstanceFileName = "output/SOM/SOMLayer_MappedInstance_" + this.layerID + "_" + this._parentLayer + "_" + this._parentNeuron.id + ".data"

      FileUtils.writeStringToFile(new File(mappedInstanceFileName), mappedInstances, encoding)
      
      if (GHSomConfig.class_labels) {
        val classLabels: String = neurons.map(row =>
            row.map(neuron => neuron.classLabels.mkString(","))
               .mkString("|")
            )
            .mkString("\n")

        val classLabelsFileName = "output/SOM/SOMLayer_ClassLabels_" +
                                          this.layerID + "_" + 
                                          this._parentLayer + "_" + this._parentNeuron.id + ".data"

        FileUtils.writeStringToFile(new File(classLabelsFileName), classLabels, encoding)
      }
    
      /* LABEL_SOM
      if (GHSomConfig.LABEL_SOM) {
        
        // convert attribute array to a map of attribute name and min, max value
        
        val attributeMap = attributes.map ( attrib => (attrib.name, (attrib.minValue, attrib.maxValue) ) )
                                     .toMap
        val mappedLabelsOfNeurons = neurons.map(row =>
          row.map(neuron => 
            neuron.labels.map( label => {
                // find the label in attribute map and deduce its unnormalized value
                val (minVal, maxVal) = attributeMap(label.name)
                Label(label.name, ((label.value * (maxVal - minVal)) + minVal))
              }
            )
          )
        )
        
        val mappedLabels = mappedLabelsOfNeurons.map(row => 
            row.map(labelSet => labelSet.mkString(","))
               .mkString("|")
            )
            .mkString("\n")

        val mappedLabelsFileName = "SOMLayer_Labels_" + 
                                          this.layerID + "_" + 
                                          this._parentLayer + "_" + this._parentNeuron.id + ".data"

        FileUtils.writeStringToFile(new File(mappedLabelsFileName), mappedLabels, encoding)
        
      }
      * 
      */
  }

  private def updateNeuronStats(
    tuple : (String, Neuron.NeuronStats)) : Unit = {

      val neuronRowCol = tuple._1.split(",")

      val (neuronRow, neuronCol) = (neuronRowCol(0).toInt, neuronRowCol(1).toInt)

      //println("Neuron - " + tuple._1 + "; MQE : " + tuple._2._1 + ";mappedInstance Count : " + tuple._2._3) 

      val neuronStats = tuple._2
      neurons(neuronRow)(neuronCol).mqe = neuronStats.mqe
      neurons(neuronRow)(neuronCol).qe = neuronStats.qe
      neurons(neuronRow)(neuronCol).mappedInstanceCount = neuronStats.instanceCount
      neurons(neuronRow)(neuronCol).clearMappedInputs()
  }

  /* LABEL_SOM
  private def getNeuronLabelSet(
    meanInstance : Instance, 
    qeInstance : Instance,
    header : Array[String]
  ) : scala.collection.Set[Label] = {
    
    /* 
     * Reference from SOM Tool Box : http://www.ifs.tuwien.ac.at/dm/download/somtoolbox+src.tar.gz
     */
    val labelMeanVector = header.zip(meanInstance.attributeVector)
                                .map(tup => new Label(name = tup._1, value = tup._2))
                                .filter( label => label.value.getValue >= 0.02 )
                                .sortWith( _.value < _.value )
    val labelQEVector = header.zip(qeInstance.attributeVector)
                              .map(tup => new Label(name = tup._1, value = tup._2))
                              //.filter(label => label.value.getValue >= 0.001)
                              .sortWith(_.value < _.value)
                              
    val numOfLabels = GHSomConfig.NUM_LABELS
    
    val labels = scala.collection.mutable.Set[Label]()
    
    var attributeIterator = 0 
    
    while( labels.size < numOfLabels && attributeIterator < labelQEVector.length) {
      var labelFound = false 
      var attributeIterator2 = 0
      while( !labelFound && attributeIterator2 < labelMeanVector.length ) {
        if (labelMeanVector(attributeIterator2).equals(labelQEVector(attributeIterator))) {
          labelFound = true
          labels.add(labelMeanVector(attributeIterator2))
        }
        attributeIterator2 += 1
      }
      attributeIterator += 1
    } 
    
    labels
  }   
  *   
  */
  
  /* LABEL_SOM
  private def updateNeuronLabels(
       tuple : (String, Set[Label])
  ) : Unit = {
      val neuronRowCol = tuple._1.split(",")

      val (neuronRow, neuronCol) = (neuronRowCol(0).toInt, neuronRowCol(1).toInt)

      //println("Neuron - " + tuple._1 + "; MQE : " + tuple._2._1 + ";mappedInstance Count : " + tuple._2._3) 

      neurons(neuronRow)(neuronCol).labels = tuple._2
  }
  * 
  */
  
  private def updateNeuronClassLabels(
    tuple : (String, Set[String])
  ) : Unit = {
      val neuronRowCol = tuple._1.split(",")

      val (neuronRow, neuronCol) = (neuronRowCol(0).toInt, neuronRowCol(1).toInt)

      //println("Neuron - " + tuple._1 + "; MQE : " + tuple._2._1 + ";mappedInstance Count : " + tuple._2._3) 

      neurons(neuronRow)(neuronCol).classLabels = tuple._2
  }
  
  private def getNeuronAndNeighbourSetForGrowing(
    criterion : Double
  ) : Set[NeuronPair] = {

    // find neurons which have high qe
    var neuronsToExpandList = new mutable.ListBuffer[Neuron]()

    if (GHSomConfig.mqe_criterion) {
      for(neuronRow <- neurons) {
        for (neuron <- neuronRow) {
          if (neuron.mqe > criterion) { //mqe_change
            neuronsToExpandList += neuron
          }
        }
      }
    }
    else {
      for(neuronRow <- neurons) {
        for (neuron <- neuronRow) {
          //if (neuron.mqe > criterion)  //mqe_change
          if (neuron.qe > criterion) {
            neuronsToExpandList += neuron
          }
        }
      }
    }

    if (GHSomConfig.mqe_criterion)
      neuronsToExpandList = neuronsToExpandList.sortWith(_.mqe > _.mqe) //mqe_change
    else
      neuronsToExpandList = neuronsToExpandList.sortWith(_.qe > _.qe)

    val neuronNeighbourSet = mutable.Set[NeuronPair]()

    for (neuron <- neuronsToExpandList) {
      //println("getNeuronAndNeighbourSetForGrowing Neuron : " + neuron)
      val dissimilarNeighbour = getMostDissimilarNeighbour(neuron)
      val neuronPair = NeuronPair(neuron, dissimilarNeighbour)

      val rowColNotExistsInSet = neuronNeighbourSet.forall { 
        setPair =>
            (setPair.isSameCol && neuronPair.isSameCol && 
              setPair.neuron1.row != neuronPair.neuron1.row) || 
            //setPair.neuron2.row != neuronPair.neuron2.row)) ||
            (setPair.isSameRow && neuronPair.isSameRow && 
              setPair.neuron1.column != neuronPair.neuron1.column)  
            //setPair.neuron2.column != neuronPair.neuron2.column))
      }

      if (rowColNotExistsInSet || neuronNeighbourSet.isEmpty ) {
        neuronNeighbourSet += neuronPair
      }
    }

    neuronNeighbourSet
  }

  private def getMostDissimilarNeighbour(refNeuron : Neuron) = {
    val neighbours = getNeighbourNeurons(refNeuron)

    var dissimilarNeighbour : Neuron = null
    var maxDist = 0.0
    // find the dissimilar neighbour
    for (neighbour <- neighbours) {
      val dist = refNeuron.neuronInstance.getDistanceFrom(neighbour.neuronInstance)
      if (dist > maxDist) {
        dissimilarNeighbour = neighbour
        maxDist = dist
      }
    }

    dissimilarNeighbour
  }

  private def getNeuronAndNeighbourForGrowing(errorNeuron : Neuron) : NeuronPair = {  
    var neuronPair : NeuronPair = null 
    val neighbours = getNeighbourNeurons(errorNeuron)
    var dissimilarNeighbour : Neuron = null
    var maxDist = 0.0

    // find the dissimilar neighbour
    for (neighbour <- neighbours) {
      val dist = errorNeuron.neuronInstance.getDistanceFrom(neighbour.neuronInstance)
      if (dist > maxDist) {
        dissimilarNeighbour = neighbour
        maxDist = dist
      }
    }

    NeuronPair(errorNeuron, dissimilarNeighbour)

  }

  private def getGrownLayer(neuronPair : NeuronPair) : Array[Array[Neuron]] = {

    var newNeurons : Array[Array[Neuron]] = null
    // add a row
    if (neuronPair.neuron1.row != neuronPair.neuron2.row) { 
      newNeurons = getRowAddedLayer(neuronPair)
      _rowDim += 1
    }
    else { // add a column
      newNeurons = getColumnAddedLayer(neuronPair)
      _colDim += 1
    }

    newNeurons
  }

  private def getGrownLayer(neuronNeighbourSet : Set[NeuronPair]) 
  : Array[Array[Neuron]] = {

    // get count of rows, columns to be added

    val currentNeuronLayer = neurons 

    var rowsToAdd = 0 
    var colsToAdd = 0 

    for (neuronPair <- neuronNeighbourSet) {
      if (neuronPair.isSameRow) 
        colsToAdd += 1
      else if (neuronPair.isSameCol)
        rowsToAdd += 1
      else {
        throw new IllegalArgumentException("neighbour set contains improper neighbour pair")
      }
    }

    val newNeurons = Array.ofDim[Neuron](_rowDim + rowsToAdd, _colDim + colsToAdd)

    // copy original array as it is
    for (i <- 0 until neurons.size) {
      for ( j <- 0 until neurons(0).size) {
        newNeurons(i)(j) = neurons(i)(j)
      }
    }

    _rowDim += rowsToAdd
    _colDim += colsToAdd

    // update the new array for each neuron pair
    for (neuronPair <- neuronNeighbourSet) {
      val (rowIdxNeuron1, colIdxNeuron1) : (Int, Int)= getNeuronRowColIdxInLayer(neuronPair.neuron1, newNeurons)

      if (neuronPair.isSameRow) {
        if (neuronPair.neuron1.column < neuronPair.neuron2.column) {
          // update and shift the values after this column
          insertInNextCol(newNeurons, colIdxNeuron1)
        }
        else 
          // update and shift the values after previous column
          insertInNextCol(newNeurons, colIdxNeuron1 - 1)
      }
      else {
        if (neuronPair.neuron1.row < neuronPair.neuron2.row) {
          // update and shift the values after this row 
          insertInNextRow(newNeurons, rowIdxNeuron1)
        }
        else 
          // update and shift the values after previous row 
          insertInNextRow(newNeurons, rowIdxNeuron1 - 1)
      }
    }

    for (i <- 0 until newNeurons.size) {
      for (j <- 0 until newNeurons(0).size) {
        newNeurons(i)(j).updateRowCol(i, j)
      }
    }

    newNeurons
  }

  private def insertInNextRow(neuronArray : Array[Array[Neuron]], row : Int) {

    if (row + 1 >= neuronArray.size || row + 2 >= neuronArray.size)
      throw new IllegalArgumentException("row value improper")

    for ( i <- neuronArray.size - 1 until row + 1 by -1 ) {
      for (j <- 0 until neuronArray(0).size) {
        neuronArray(i)(j) = neuronArray(i-1)(j)
      }
    } 

    // update with the average instance 
    for (j <- 0 until neuronArray(0).size ) {
      if (neuronArray(row)(j) != null && neuronArray(row + 2)(j) != null) {
        neuronArray(row + 1)(j) = Neuron(
          row + 1, 
          j, 
          Instance.averageInstance(
            neuronArray(row)(j).neuronInstance, 
            neuronArray(row + 2)(j).neuronInstance 
          )
        )
        neuronArray(row + 1)(j).id = neuronArray(row)(j).id + "+" + neuronArray(row + 2)(j).id
      }
    }
  }

  private def insertInNextCol(neuronArray : Array[Array[Neuron]], col : Int) {

    if (col + 1 >= neuronArray(0).size || col + 2 >= neuronArray(0).size)
      throw new IllegalArgumentException("row value improper")

    for (j <- neuronArray(0).size - 1 until col + 1 by -1 ) {
      for ( i <- 0 until neuronArray.size ) {
        neuronArray(i)(j) = neuronArray(i)(j - 1)
      }
    } 

    // update with the average instance 
    for (i <- 0 until neuronArray.size ) {
      if (neuronArray(i)(col) != null && neuronArray(i)(col + 2) != null) {
        neuronArray(i)(col + 1) = Neuron(
          i, 
          col + 1, 
          Instance.averageInstance(
            neuronArray(i)(col).neuronInstance, 
            neuronArray(i)(col + 2).neuronInstance 
          )
        )
        neuronArray(i)(col + 1).id = neuronArray(i)(col).id + "+" + neuronArray(i)(col + 2).id
      }
    }
  }

  private def getNeuronRowColIdxInLayer(neuron : Neuron, neuronArray : Array[Array[Neuron]]) : (Int, Int) = {

    var rowColTuple : (Int, Int) = (0,0)

    var found = false 
    var i = 0
    while ( i < neuronArray.size && !found ) {
      var j = 0
      while ( j < neuronArray(0).size && !found ) {
        if ( neuronArray(i)(j) != null && neuron.id.equals(neuronArray(i)(j).id) ) {
          rowColTuple = (i,j)
          found = true
        }
        j += 1
      }
      i += 1
    }

    if (found == false)
      throw new IllegalStateException("Improper state of neuron Array")

    rowColTuple
  }

  private def getRowAddedLayer(neuronPair : NeuronPair) : Array[Array[Neuron]] = {
    var newNeurons = Array.ofDim[Neuron](_rowDim + 1, _colDim) // add a row

    val minRow = (neuronPair.neuron1.row < neuronPair.neuron2.row) match { 
      case true => neuronPair.neuron1.row 
      case false => neuronPair.neuron2.row
    }

    for ( i <- 0 to minRow ) { // 0 to minRow included
      for ( j <- 0 until _colDim) {
        newNeurons(i)(j) = neurons(i)(j)
      }
    }

    // average values for new row
    for ( j <- 0 until _colDim ) {
      newNeurons(minRow + 1)(j) = 
        Neuron(minRow + 1, 
          j, 
          Instance.averageInstance(neurons(minRow)(j).neuronInstance, 
            neurons(minRow+1)(j).neuronInstance))
    }

    // copy remaining
    for (i <- minRow + 1 until _rowDim) {
      for ( j <- 0 until _colDim ) {
        newNeurons(i + 1)(j) = Neuron(i+1, j, neurons(i)(j).neuronInstance)
      }
    }

    newNeurons
  }

  private def getColumnAddedLayer(neuronPair : NeuronPair) : Array[Array[Neuron]] = {
    var newNeurons = Array.ofDim[Neuron](_rowDim, _colDim + 1) // add a column

    val minCol = (neuronPair.neuron1.column < neuronPair.neuron2.column) match { 
      case true => neuronPair.neuron1.column 
      case false => neuronPair.neuron2.column
    }

    for ( j <- 0 to minCol ) { // 0 to minCol included
      for ( i <- 0 until _rowDim) {
        newNeurons(i)(j) = neurons(i)(j)
      }
    }

    // average values for new row
    for ( i <- 0 until _rowDim ) {
      newNeurons(i)(minCol + 1) = 
        Neuron(i, 
          minCol + 1, 
          Instance.averageInstance(neurons(i)(minCol).neuronInstance, 
            neurons(i)(minCol+1).neuronInstance))
    }

    // copy remaining
    for (j <- minCol + 1 until _colDim) {
      for ( i <- 0 until _rowDim ) {
        newNeurons(i)(j + 1) = Neuron(i, j+1, neurons(i)(j).neuronInstance)
      }
    }

    newNeurons
  }

  private def getNeighbourNeurons(neuron : Neuron) : Iterable[Neuron] = {

    val row = neuron.row

    val col = neuron.column

    val neighbours : mutable.ListBuffer[Neuron] = new mutable.ListBuffer[Neuron]()

    if (row - 1 >= 0)
      neighbours += neurons(row - 1)(col)

    if (row + 1 < _rowDim)
      neighbours += neurons(row + 1)(col)

    if (col - 1 >= 0)
      neighbours += neurons(row)(col - 1)

    if (col + 1 < _colDim)
      neighbours += neurons(row)(col + 1)

    neighbours
  }

  // TODO : change for categorical
  private def getWeightVectorsForChildLayer(neuron : Neuron) : Array[Array[DimensionType]]  = {
    
    var instanceTop : Instance = null
    var instanceLeft : Instance = null
    var instanceRight : Instance = null
    var instanceBottom : Instance = null
    
    if (neuron.row == 0 && neuron.column == 0) {
      // top-left
      
      // param neuron is neuron[0,0]
      // surrounded by neuron[0,1], neuron[1,0], 
      // output neuron[-1,-1], neuorn[-1,0], neuron[0,-1], neuron[0,0]
      
      instanceTop = neuron.neuronInstance + (neuron.neuronInstance - this.getNeuron(neuron.row + 1,neuron.column).neuronInstance)
      instanceLeft = neuron.neuronInstance + (neuron.neuronInstance - this.getNeuron(neuron.row,neuron.column + 1).neuronInstance)
      instanceRight = this.getNeuron(neuron.row,neuron.column + 1).neuronInstance
      instanceBottom = this.getNeuron(neuron.row + 1,neuron.column).neuronInstance
    }
    
    else if (neuron.row == 0 && neuron.column == this.colDim - 1) {
      // top-right
      instanceTop = neuron.neuronInstance + (neuron.neuronInstance - this.getNeuron(neuron.row + 1,neuron.column).neuronInstance)
      instanceLeft = this.getNeuron(neuron.row, neuron.column - 1).neuronInstance
      instanceRight = neuron.neuronInstance + (neuron.neuronInstance - this.getNeuron(neuron.row, neuron.column - 1).neuronInstance)
      instanceBottom = this.getNeuron(neuron.row + 1, neuron.column).neuronInstance
    }
    else if (neuron.row == this.rowDim - 1 && neuron.column == 0) {
      // bottom left
      instanceTop = this.getNeuron(neuron.row - 1, neuron.column).neuronInstance
      instanceLeft = neuron.neuronInstance + (neuron.neuronInstance - this.getNeuron(neuron.row, neuron.column + 1).neuronInstance)
      instanceRight = this.getNeuron(neuron.row, neuron.column + 1).neuronInstance
      instanceBottom = neuron.neuronInstance + (neuron.neuronInstance - this.getNeuron(neuron.row - 1, neuron.column).neuronInstance)
    }
    else if (neuron.row == this.rowDim - 1 && neuron.column == this.colDim - 1) {
      // bottom right
      instanceTop = this.getNeuron(neuron.row - 1, neuron.column).neuronInstance
      instanceLeft = this.getNeuron(neuron.row, neuron.column - 1).neuronInstance
      instanceRight = neuron.neuronInstance + (neuron.neuronInstance - this.getNeuron(neuron.row, neuron.column - 1).neuronInstance)
      instanceBottom = neuron.neuronInstance + (neuron.neuronInstance - this.getNeuron(neuron.row - 1, neuron.column).neuronInstance)
    }
    else if (neuron.row == 0) {
      // top row
      instanceTop = neuron.neuronInstance + (neuron.neuronInstance - this.getNeuron(neuron.row + 1, neuron.column).neuronInstance)
      instanceLeft = this.getNeuron(neuron.row, neuron.column - 1).neuronInstance
      instanceRight = this.getNeuron(neuron.row, neuron.column + 1).neuronInstance
      instanceBottom = this.getNeuron(neuron.row + 1, neuron.column).neuronInstance
    }
    else if (neuron.column == 0) {
      // left column
      instanceTop = this.getNeuron(neuron.row - 1, neuron.column).neuronInstance
      instanceLeft = neuron.neuronInstance + (neuron.neuronInstance - this.getNeuron(neuron.row, neuron.column + 1).neuronInstance)
      instanceRight = this.getNeuron(neuron.row, neuron.column + 1).neuronInstance
      instanceBottom = this.getNeuron(neuron.row + 1, neuron.column).neuronInstance
    }
    else if (neuron.row == this.rowDim - 1) {
      // bottom row
      instanceTop = this.getNeuron(neuron.row - 1, neuron.column).neuronInstance
      instanceLeft = this.getNeuron(neuron.row, neuron.column - 1).neuronInstance
      instanceRight = this.getNeuron(neuron.row, neuron.column + 1).neuronInstance
      instanceBottom = neuron.neuronInstance + (neuron.neuronInstance - this.getNeuron(neuron.row - 1, neuron.column).neuronInstance)
    }
    else if (neuron.column == this.colDim - 1) {
      // right column
      instanceTop = this.getNeuron(neuron.row - 1, neuron.column).neuronInstance
      instanceLeft = this.getNeuron(neuron.row, neuron.column - 1).neuronInstance
      instanceRight = neuron.neuronInstance + (neuron.neuronInstance - this.getNeuron(neuron.row, neuron.column - 1).neuronInstance)
      instanceBottom = this.getNeuron(neuron.row + 1, neuron.column).neuronInstance
    }
    else {
      // middle cells
      instanceTop = this.getNeuron(neuron.row - 1, neuron.column).neuronInstance
      instanceLeft = this.getNeuron(neuron.row, neuron.column - 1).neuronInstance
      instanceRight = this.getNeuron(neuron.row, neuron.column + 1).neuronInstance
      instanceBottom = this.getNeuron(neuron.row + 1, neuron.column).neuronInstance
    }
    
    val weightVector00 = InstanceFunctions.getAverageInstance(neuron.neuronInstance, 
                                                                instanceTop, 
                                                                instanceLeft).attributeVector
    val weightVector01 = InstanceFunctions.getAverageInstance(neuron.neuronInstance, 
                                                                instanceTop,
                                                                instanceRight).attributeVector
    val weightVector10 = InstanceFunctions.getAverageInstance(neuron.neuronInstance, 
                                                                instanceLeft, 
                                                                instanceBottom).attributeVector                                                        
    val weightVector11 = InstanceFunctions.getAverageInstance(neuron.neuronInstance, 
                                                                instanceRight, 
                                                                instanceBottom).attributeVector
                                                                
    Array(weightVector00, weightVector01, weightVector10, weightVector11)
  }
  
  def dumpUMatrix(attributes : Array[Attribute]) : Unit = {
    
    /*
     * Fill the cells between the neuron cells
     * Reference from SOM Tool Box : http://www.ifs.tuwien.ac.at/dm/download/somtoolbox+src.tar.gz
     */
    val umatrixRows = (this._rowDim * 2) - 1
    val umatrixCols = (this._colDim * 2) - 1
    
    val umatrix = Array.tabulate(umatrixRows, umatrixCols)((row, col) => {
      0.0  
    })
    
    for( row <- 0 until umatrixRows) {
      for (col <- 0 until umatrixCols) {
        var instance1 : Instance = null
        var instance2 : Instance = null
        if (row % 2 != 0 && col %2 == 0) {
          instance1 = this.neurons((row - 1) / 2)(col / 2).neuronInstance
          instance2 = this.neurons((row - 1) / 2 + 1)(col / 2).neuronInstance
          umatrix(row)(col) = instance1.getDistanceFrom(instance2) 
        }
        else if (row % 2 == 0 && col %2 != 0) {
          instance1 = this.neurons(row / 2)((col - 1)/2).neuronInstance
          instance2 = this.neurons(row / 2)((col - 1)/2 + 1).neuronInstance
          umatrix(row)(col) = instance1.getDistanceFrom(instance2)
        }
        else if (row % 2 != 0 && col % 2 != 0) {
          instance1 = this.neurons((row - 1) / 2)((col - 1) / 2).neuronInstance
          instance2 = this.neurons((row - 1) / 2 + 1)((col - 1)/2 + 1).neuronInstance
          val dist1 = instance1.getDistanceFrom(instance2)
          
          instance1 = this.neurons((row - 1)/2 + 1)((col - 1)/2).neuronInstance 
          instance2 = this.neurons((row - 1)/2)((col - 1)/2 + 1).neuronInstance
          val dist2 = instance1.getDistanceFrom(instance2)

          umatrix(row)(col) = (dist1 + dist2) / (2 * sqrt(2))
        }
      }
    }
    
    // interpolate the remaining values of umatrix
    for (row <- 0 until umatrixRows by 2) {
      for (col <- 0 until umatrixCols by 2) {
          var lst : Array[Double] = null
          if (row == 0 && col == 0)
              lst = Array(umatrix(row+1)(col),umatrix(row)(col + 1))
          else if (row == 0 && col == umatrixCols - 1)
              lst = Array(umatrix(row+1)(col), umatrix(row)(col-1))
          else if (row == umatrixRows - 1 && col == 0)
              lst = Array(umatrix(row - 1)(col), umatrix(row)(col + 1))
          else if (row == umatrixRows - 1 && col == umatrixCols - 1)
              lst = Array(umatrix(row-1)(col), umatrix(row)(col-1))
          else if (col == 0)
              lst = Array(umatrix(row - 1)(col), umatrix(row + 1)(col), umatrix(row)(col + 1))
          else if (col == umatrixCols - 1)
              lst = Array(umatrix(row - 1)(col), umatrix(row + 1)(col), umatrix(row)(col - 1))
          else if (row == 0) 
              lst = Array(umatrix(row)(col - 1), umatrix(row)(col + 1), umatrix(row + 1)(col))
          else if (row == umatrixRows - 1)
              lst = Array(umatrix(row)(col - 1), umatrix(row)(col + 1), umatrix(row - 1)(col))
          else 
              lst = Array(umatrix(row)(col - 1), umatrix(row)(col + 1), 
                     umatrix(row - 1)(col), umatrix(row + 1)(col))
          umatrix(row)(col) = lst.sum / lst.size
      }
    }
    
    val umatrixStr = umatrix.map(row => row.mkString(","))
                            .mkString("\n")
    
    val filename = "output/SOM/UMatrix_" + this.layerID + "_" + this._parentLayer + "_" + this._parentNeuron.id + ".mat"

    val encoding : String = null

    FileUtils.writeStringToFile(new File(filename), umatrixStr, encoding)
  }
  
  def dumpComponentPlanes(attributes : Array[Attribute]) : Unit = { 
    
    for (attrib <- attributes) {
      val name = attrib.name
      val idx = attrib.index
      var domainValues : Array[_ <: Any] = null
      
      if (attrib.dimensionType == DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL)
        domainValues = attrib.domainValues
      else 
        domainValues = Array(attrib.minValue, attrib.maxValue)
        
      // Returns an 2-d array of the attribute only
      val attribComponentPlanePixelValues = 
        this.neurons.map{
          row => row.map{
            neuron => neuron.neuronInstance.attributeVector(idx).getImagePixelValue(domainValues)
          }
          .mkString(",")
        }
        .mkString("\n")
      
        /*
      val attribComponentPlaneStr = 
        if (attrib.dimensionType == DimensionTypeEnum.NUMERIC) {
          attribComponentPlaneDimensionType.map{ 
            row => 
              row.map{ dimension => 
                dimension * (attrib.maxValue - attrib.minValue) + attrib.minValue
              }
              .mkString(",")
          }
          .mkString("\n")
        }
        else if (attrib.dimensionType == DimensionTypeEnum.NOMINAL) {
          attribComponentPlaneDimensionType.map{
            row => 
              row.mkString(",")
          }
          .mkString("\n")
        }
        else {
          null
        }
        * 
        */
    
      val filename = "output/SOM/ComponentPlane_" + name + "_" + this.layerID + "_" + this._parentLayer + "_" + this._parentNeuron.id + ".mat"

      val encoding : String = null

      FileUtils.writeStringToFile(new File(filename), attribComponentPlanePixelValues, encoding)
    }
  }
}

object SOMLayerFunctions {

  def testModel(neurons : Array[Array[Neuron]],
                classAssignments: Map[(Int, Int), String],
                testData: RDD[Instance]): RDD[(Instance, Double, String)] = {

    val classified: RDD[(Instance, Double, String)] = testData.map(instance => {

      var responseFlag = false
      var distances: Map[(Int, Int), Double] = findAllDistances(neurons, instance)
      var dist = 0.0
      var instanceClass = "init"

      if(!distances.isEmpty) {

        while(!responseFlag) {

          if(!distances.isEmpty) {
            val minEntry: ((Int, Int), Double) = distances.minBy(x => x._2)
            dist = minEntry._2

            if(classAssignments.keySet.contains(minEntry._1)) {
              instanceClass = classAssignments.apply(minEntry._1)
              responseFlag=true
            }
            else {
              println("Key not found for entry " + minEntry._1 + ", looking for another neuron...")
              distances = distances - minEntry._1
            }
          }

          else
          {
            dist = 0.0
            instanceClass = "none"
          }

        }
        (instance,dist,instanceClass)

      }

      else
        (instance,0.0,"none")

    })

    classified
  }
  //********************************************************************************************
  def getNeuronUpdatesTuples(instancePartition : Iterator[Instance], neurons: Array[Array[Neuron]]) = {
    
  }

  def findAllDistances(neurons : Array[Array[Neuron]], instance : Instance): Map[(Int, Int), Double] = {

    var distances : Map[(Int,Int),Double] = Map[(Int,Int),Double]()

    for (i <- 0 until neurons.size) {
      for (j <- 0 until neurons(0).size) {
        val dist = neurons(i)(j).neuronInstance.getDistanceFrom(instance)
        distances += ((i,j) -> dist)
      }
    }
    distances
  }

  def findBMU(neurons : Array[Array[Neuron]], instance : Instance) : Neuron = {
    var bmu : Neuron = neurons(0)(0)

    var minDist = instance.getDistanceFrom(bmu.neuronInstance)

    for (i <- 0 until neurons.size) {
      for (j <- 0 until neurons(0).size) {
        val dist = neurons(i)(j).neuronInstance.getDistanceFrom(instance)
        if (dist < minDist) {
          minDist = dist
          bmu = neurons(i)(j)
        }
      }
    } 
    bmu
  }
  //********************************************************************************************
  def findBMUMT(neurons : Array[Array[NeuronMT]], instance : InstanceMT) : NeuronMT = {
    var bmu : NeuronMT = neurons(0)(0)

    var minDist = instance.getDistanceFrom(bmu.neuronInstanceMT)

    for (i <- 0 until neurons.size) {
      for (j <- 0 until neurons(0).size) {
        val dist = neurons(i)(j).neuronInstanceMT.getDistanceFrom(instance)
        if (dist < minDist) {
          minDist = dist
          bmu = neurons(i)(j)
        }
      }
    }
    bmu
  }
  //********************************************************************************************
  def findBMUAndNextBMU(neurons : Array[Array[Neuron]], instance : Instance) : (Neuron, Neuron) = {
    var bmu : Neuron = neurons(0)(0)
    var nextBmu : Neuron = null

    var minDist = instance.getDistanceFrom(bmu.neuronInstance)
    var nextMinDist = Double.MaxValue

    for (i <- 0 until neurons.size) {
      for (j <- 0 until neurons(0).size) {
        val dist = neurons(i)(j).neuronInstance.getDistanceFrom(instance)
        if (dist <= minDist) {
          nextBmu = bmu
          nextMinDist = minDist
          minDist = dist
          bmu = neurons(i)(j)
        }
        else if (dist < nextMinDist) {
          nextBmu = neurons(i)(j)
          nextMinDist = dist
        }
      }
    } 
    (bmu, nextBmu)
  }

  def combineNeuronUpdates(
    a : SOMLayer.NeuronUpdate,
    b : SOMLayer.NeuronUpdate
  ) : SOMLayer.NeuronUpdate = {
    // for both elements of tuples,
    // zip up the corresponding arrays of a & b and add them
    // TODO : combineWithInstance(b.numerator) 
    SOMLayer.NeuronUpdate(InstanceFunctions.combineNeighbourhoodFactorUpdates(a.numerator,b.numerator), a.denominator + b.denominator)
  }

  def computeUpdatedNeuronVector(
    neuronUpdate : SOMLayer.NeuronUpdate
  ) : Instance = {
    InstanceFunctions.divideInstanceByCumulativeNeighbourhoodFactor(neuronUpdate.numerator, neuronUpdate.denominator)
  }

  /* Input : (QE , Count)
   * Output : (QE_sum, Count_sum)
   */
  def combineNeuronsQE( 
    neuron1 : Neuron.NeuronStats, 
    neuron2 : Neuron.NeuronStats 
  ) : Neuron.NeuronStats = {
      Neuron.NeuronStats(qe = neuron1.qe + neuron2.qe, instanceCount = neuron1.instanceCount + neuron2.instanceCount)
  }

  def combineNeuronsQEMT(
                        neuron1 : NeuronMT.NeuronStatsMT,
                        neuron2 : NeuronMT.NeuronStatsMT
                      ) : NeuronMT.NeuronStatsMT = {
    NeuronMT.NeuronStatsMT(qe = neuron1.qe + neuron2.qe, instanceCount = neuron1.instanceCount + neuron2.instanceCount)
  }

  /**
   * input : tuple of (qe, num_of_instances)
   * output : tuple of (mqe , qe, num_of_instances)
   */
  def computeMQEForNeuron(
    neuron : Neuron.NeuronStats
  ) : Neuron.NeuronStats = {
    Neuron.NeuronStats(mqe = neuron.qe / neuron.instanceCount, qe = neuron.qe, instanceCount = neuron.instanceCount)
  }

  def computeMQEForNeuronMT(
                           neuron : NeuronMT.NeuronStatsMT
                         ) : NeuronMT.NeuronStatsMT = {
    NeuronMT.NeuronStatsMT(mqe = neuron.qe / neuron.instanceCount, qe = neuron.qe, instanceCount = neuron.instanceCount)
  }

  def mergeDatasetsForHierarchicalExpansion(
    rec1 : GHSom.LayerNeuronRDDRecord, 
    rec2 : GHSom.LayerNeuronRDDRecord
  ) : List[GHSom.LayerNeuronRDDRecord] = {
    List(rec1) ++ List(rec2) 
  }
  
 /** 
  * Input : (QE , Count)
  * Output : (QE_sum, Count_sum)
  */
  def combineNeuronsMeanAndQEForLabels( 
      t1: (Instance, Instance, Long), 
      t2: (Instance, Instance, Long) 
  ) : (Instance, Instance, Long) = {
    (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
  }

  /**
   * input : tuple of (qe, num_of_instances)
   * output : tuple of (mqe , qe, num_of_instances)
   */
  def computeMeanAndQEForLabels(
    tup : (Instance, Instance, Long) 
  ) : (Instance, Instance, Long) = {
    (tup._1 / tup._3, tup._2 / tup._3, tup._3)
  }
  
  def computeNeuronClassLabels(
    labelSet1 : Set[String], labelSet2 : Set[String]    
  ) : Set[String] = {
    labelSet1.union(labelSet2)
  }

  def computeNeuronClassLabelsList(
                                labelSet1 : List[String], labelSet2 : List[String]
                              ) : List[String] = {
    labelSet1.union(labelSet2)
  }
  
}

object SOMLayer {

	private var layerId = 0

  def apply(
      parentNeuron : Neuron, 
      parentLayer : Int, 
      rowDim : Int, 
      colDim : Int, 
      vectorSize : Int, 
      initializationInstances : Array[Instance]) = {
		layerId += 1
				//new SOMLayer(layerId, rowDim, colDim, parentNeuronID, parentLayer, parentNeuronMQE /*mqe_change*/ , vectorSize )
		new SOMLayer(
        layerId, 
        rowDim, 
        colDim, 
        parentNeuron, 
        parentLayer,  
        vectorSize, 
        initializationInstances 
    )
  }

  // TODO : NeuronUpdate(numerator : Instance, denominator : Double)
  case class NeuronUpdate(numerator : Instance, denominator : Double)

  def main(args : Array[String]) {
  }

}

