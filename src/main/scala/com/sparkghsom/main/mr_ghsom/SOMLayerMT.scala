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
import com.sparkghsom.main.mr_ghsom.NeuronMT.NeuronStatsMT
import com.sparkghsom.main.mr_ghsom.SOMLayerMT.NeuronMTUpdate

class SOMLayerMT private (
                         private val _layerID : Int,
                         private var _rowDim : Int,
                         private var _colDim : Int,
                         private val _parentNeuronMT : NeuronMT,
                         //private val _parentNeuronMTID : String,
                         private val _parentLayer : Int,
                         //private val parentNeuronMTQE : Double, 
                         //private val parentNeuronMTMQE : Double,  // mqe_change
                         attributeVectorSize : Int,
                         initializationInstanceMTs : Array[InstanceMT]
                       ) extends Serializable {

  private var neurons : Array[Array[NeuronMT]] = {
    Array.tabulate(_rowDim, _colDim)(
      (rowParam,colParam) => {
        val neuron = NeuronMT(row = rowParam,
          column = colParam,
          neuronInstanceMT = InstanceMT(0,Array("neuron-[" + rowParam.toString() +","+ colParam.toString() + "]"),
            initializationInstanceMTs(rowParam * _rowDim + colParam).attributeVector
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

  def parentNeuronMT : NeuronMT = _parentNeuronMT

  def gridSize() {
    println("LAYER SIZE: " + _rowDim + "x" + _colDim)
  }

  def totalNeuronMTs : Long = _rowDim * _colDim

  def rowDim : Int = _rowDim

  def colDim : Int = _colDim

  def getNeuronMT(row : Int, col : Int) : NeuronMT = { this.neurons(row)(col) }

  def getNeuronMTMap() = { this.neurons }

  private case class NeuronMTPair ( var neuron1 : NeuronMT, var neuron2 : NeuronMT ) {

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
        case o : NeuronMTPair => {
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
      "NeuronMT1: " + neuron1.id + ", NeuronMT2: " + neuron2.id
    }
  }

  def display() {
    println("Display Layer")
    println("Layer Details -> id : " +
      this._layerID +
      ";parent Layer : " +
      this.parentLayer +
      ";parent NeuronMT" +
      this._parentNeuronMT.id)
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


  def train(dataset : RDD[InstanceMT], maxIterations : Long) {

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
                val bmu : NeuronMT = SOMLayerFunctions.findBMUMT(neurons, instance)
                neurons.flatten.map { neuron =>
                  val neighbourhoodFactor = neuron.getNeighbourhoodFactor(bmu, iteration, maxIterations, radius)
                  val neuronUpdateNumNDen: NeuronMTUpdate = SOMLayerMT.NeuronMTUpdate (
                    InstanceMTFunctions.getInstanceMTWithNeighbourhoodFactor(instance, neighbourhoodFactor),
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
        val bmu : NeuronMT = SOMLayerFunctions.findBMUMT(neurons, instance)

        val temp = neurons.flatten
        temp.map { neuron => 
          val neighbourhoodFactor = neuron.getNeighbourhoodFactor(bmu, iteration, maxIterations, radius)
          (
            neuron.id, 
            SOMLayerMT.NeuronMTUpdate (
              InstanceMTFunctions.getAttributeVectorWithNeighbourhoodFactor(instance, neighbourhoodFactor), 
              neighbourhoodFactor
            )
          )
        }
      }
      * 
      */

      val updatedModel: Map[String, InstanceMT] = new PairRDDFunctions[String, SOMLayerMT.NeuronMTUpdate](neuronUpdatesRDD) // create a pairrdd
        .reduceByKey(SOMLayerFunctionsMT.combineNeuronMTUpdates) // combines/shuffles updates from all workers
        .mapValues(SOMLayerFunctionsMT.computeUpdatedNeuronMTVector) // updates all values (neuron ID -> wt. vector)
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
            neurons(i)(j).neuronInstanceMT =
              InstanceMTFunctions.computeUpdateInstanceMT(
                neurons(i)(j).neuronInstanceMT,
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
  def computeStatsForLayer(dataset : RDD[InstanceMT]) {

    val neurons = this.neurons

    /***** MapReduce Begins *****/
    // runs on workers 
    // creates a map of (neuron id -> (quantization error, instance count = 1)) 


    val neuronsQE = new PairRDDFunctions[String, NeuronMT.NeuronStatsMT](
      //val neuronsQE = new PairRDDFunctions[String, (Double, Long, Set[String])]( // classlabel_map
      dataset.map { instance =>
        val bmu = SOMLayerFunctionsMT.findBMUMT(neurons, instance)
        val qe = bmu.neuronInstanceMT.getDistanceFrom(instance)
        (bmu.id, NeuronMT.NeuronStatsMT(qe = qe, instanceCount = 1L))
      }
    )

    val neuronMQEs = neuronsQE.reduceByKey(SOMLayerFunctionsMT.combineNeuronMTsQE)
      .mapValues(SOMLayerFunctionsMT.computeMQEForNeuronMT)

    neuronMQEs.collectAsMap
      .map(updateNeuronStats) // return to driver
    /***** MapReduce Ends *****/
  }

  def checkMQE(tau1 : Double): (Boolean, Double, NeuronMT) = {
    var sum_mqe_m : Double = 0 // mqe_change
    var mappedNeuronMTsCnt : Int = 0
    var maxMqeNeuronMT = neurons(0)(0) //mqe_change
    var maxMqe : Double = 0 //mqe_change

    for (neuronRow <- neurons) {
      for (neuron <- neuronRow) {
        if (neuron.mappedInstanceMTCount != 0) {
          sum_mqe_m += neuron.mqe
          mappedNeuronMTsCnt += 1
          if (neuron.mqe > maxMqe) {
            maxMqeNeuronMT = neuron
            maxMqe = neuron.mqe
          }
        }
      }
    }

    val MQE_m = sum_mqe_m / mappedNeuronMTsCnt // mqe_change
    println("Criterion : ")
    println("sum_mqe_m / mappedNeuronMTsCnt : " + sum_mqe_m + "/" + mappedNeuronMTsCnt + "=" + MQE_m) //mqe_change
    println("tau1 x parentNeuronMTMQE : " + tau1 + "x" + this._parentNeuronMT.mqe + "=" + tau1 * this._parentNeuronMT.mqe)

    if (MQE_m > tau1 * this._parentNeuronMT.mqe) {
      (true, MQE_m , maxMqeNeuronMT)
    }
    else {
      (false, MQE_m , maxMqeNeuronMT)
    }

  }

  def checkQE(tau1 : Double): (Boolean, Double, NeuronMT) = {
    //var sum_mqe_m : Double = 0 // mqe_change
    var sum_qe_m : Double = 0
    var mappedNeuronMTsCnt : Int = 0
    //var maxMqeNeuronMT = neurons(0)(0) //mqe_change
    var maxQeNeuronMT = neurons(0)(0)
    //var maxMqe : Double = 0 //mqe_change
    var maxQe : Double = 0

    for (neuronRow <- neurons) {
      for (neuron <- neuronRow) {
        if (neuron.mappedInstanceMTCount != 0) {
          sum_qe_m += neuron.qe
          mappedNeuronMTsCnt += 1
          if (neuron.qe > maxQe) {
            maxQeNeuronMT = neuron
            maxQe = neuron.qe
          }
          /*
           * mqe_change
           sum_mqe_m += neuron.mqe
           mappedNeuronMTsCnt += 1

           if (neuron.mqe > maxMqe) {
             maxMqeNeuronMT = neuron
             maxMqe = neuron.mqe
           }
           */
        }
      }
    }

    //val MQE_m = sum_mqe_m / mappedNeuronMTsCnt // mqe_change
    val MQE_m = sum_qe_m / mappedNeuronMTsCnt
    println("Criterion : ")
    //println("sum_qe_m / mappedNeuronMTsCnt : " + sum_mqe_m + "/" + mappedNeuronMTsCnt + "=" + MQE_m) //mqe_change
    println("sum_qe_m / mappedNeuronMTsCnt : " + sum_qe_m + "/" + mappedNeuronMTsCnt + "=" + MQE_m)
    println("tau1 x parentNeuronMTQE : " + tau1 + "x" + this._parentNeuronMT.qe + "=" + tau1 * this._parentNeuronMT.qe)
    //println("tau1 x parentNeuronMTMQE : " + tau1 + "x" + this.parentNeuronMTMQE) //mqe_change

    if (MQE_m > tau1 * this._parentNeuronMT.qe) {
      (true, MQE_m , maxQeNeuronMT)
    }
    else {
      (false, MQE_m , maxQeNeuronMT)
    }

  }

  def growSingleRowColumn(errorNeuronMT : NeuronMT) {

    val dissimilarNeighbour = getMostDissimilarNeighbour(errorNeuronMT)

    neurons = getGrownLayer(NeuronMTPair(errorNeuronMT, dissimilarNeighbour))

  }

  /**
    * Grows the layer adding rows/columns. 
    * This method runs on the driver completely
    * @param tau1 parameter controlling the horizontal growth of a layer
    * @param mqe_u mean quantization error of the parent neuron of the layer 
    */
  def growMultipleCells(tau1 : Double) {
    var neuronPairSet : Set[NeuronMTPair] = null
    if (GHSomConfig.mqe_criterion)
      neuronPairSet = getNeuronMTAndNeighbourSetForGrowing(tau1 * this._parentNeuronMT.mqe)// mqe_change
    else
      neuronPairSet = getNeuronMTAndNeighbourSetForGrowing(tau1 * this._parentNeuronMT.qe)
    if (GHSomConfig.debug) {
      println("NeuronMTs to Expand")
      for (pair <- neuronPairSet) {
        println(pair)
      }
    }

    neurons = getGrownLayer(neuronPairSet)
  }

  def getNeuronMTsForHierarchicalExpansion(criterion : Double, instanceCount : Long) : mutable.Set[NeuronMT] = {
    val neuronSet = new mutable.HashSet[NeuronMT]()

    if (GHSomConfig.mqe_criterion) {
      neurons.foreach {
        neuronRow =>
          neuronRow.foreach {
            neuron =>
              if (neuron.mappedInstanceMTCount > GHSomConfig.hierarchical_count_factor * instanceCount &&
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
              if (neuron.mappedInstanceMTCount > GHSomConfig.hierarchical_count_factor * instanceCount &&
                neuron.qe > criterion /* mqe_change neuron.mqe > tau2 * parentNeuronMTMQE */) {
                neuronSet += neuron
                neuron.childLayerWeightVectors = getWeightVectorsForChildLayer(neuron)
              }
          }
      }
    }
    neuronSet
  }

  def getRDDForHierarchicalExpansion(
                                      //addToDataset : RDD[(Int, String, InstanceMT)], 
                                      dataset : RDD[InstanceMT],
                                      neuronsToExpand : mutable.Set[NeuronMT]
                                    ) : RDD[GHSom.LayerNeuronRDDRecordMT] = {

    val context = dataset.context

    val neurons = this.neurons

    val layerID = this.layerID

    val neuronIdsToExpand = neuronsToExpand.map(neuron => neuron.id)
    //var origDataset = addToDataset

    dataset.map(
      instance => {
        val bmu = SOMLayerFunctions.findBMUMT(neurons, instance)
        if (neuronIdsToExpand.contains(bmu.id)) {
          GHSom.LayerNeuronRDDRecordMT(layerID, bmu.id, instance)
        }
        else
          null
      }
    )
      .filter(record => record != null)
  }

  def initializeLayerWithParentNeuronMTWeightVectors {
    if (this.rowDim != 2 || this.colDim != 2)
      return

    for (i <- 0 until this.rowDim) {
      for (j <- 0 until this.colDim) {
        neurons(i)(j).neuronInstanceMT = InstanceMT(0,neurons(i)(j).neuronInstanceMT.label,
          this._parentNeuronMT.childLayerWeightVectors(i * colDim + j))
      }
    }
  }

  /* LABEL_SOM
  def computeLabels(dataset : RDD[InstanceMT], headers : Array[String]) {

    var attributeHeaders = headers

    if (attributeHeaders == null) {
      attributeHeaders = Array.tabulate(this.attributeVectorSize)(index => index.toString())
    }

    val neurons = this.neurons

    /***** MapReduce Begins *****/
    // runs on workers
    // creates a map of (neuron id -> (sum of instances,  instance count = 1))

    val neuronLabels = new PairRDDFunctions[String, (InstanceMT, InstanceMT, Long)](
      dataset.map { instance =>
        val bmu = SOMLayerFunctions.findBMUMT(neurons, instance)
        val sumInstanceMT = InstanceMT("sumInstanceMT", instance.attributeVector)
        val qeInstanceMT = InstanceMTFunctions.getQEInstanceMT(bmu.neuronInstanceMT, instance)
        (bmu.id,(sumInstanceMT, qeInstanceMT, 1))
      }
    )

    // combines / reduces (adds) all quantization errors and instance counts from each mapper
    val neuronMeanNQEs = neuronLabels.reduceByKey(SOMLayerFunctions.combineNeuronMTsMeanAndQEForLabels)
                                     .mapValues(SOMLayerFunctions.computeMeanAndQEForLabels)

    val neuronUpdatesMap = neuronMeanNQEs.collectAsMap

    val neuronLabelMap = neuronUpdatesMap.mapValues( tup => getNeuronMTLabelSet(tup._1, tup._2, attributeHeaders) )

    neuronLabelMap.map(updateNeuronMTLabels) // return to driver

    /***** MapReduce Ends *****/
  }
  *
  */


  def computeClassLabelsRegression(dataset : RDD[InstanceMT]): Map[(Int, Int), Array[String]] = {

    val neurons = this.neurons

    val neuronsClassLabels = new PairRDDFunctions[String, Set[Array[String]]](
      dataset.map { instance =>
        val bmu = SOMLayerFunctionsMT.findBMUMT(neurons, instance)
        (bmu.id,Set(instance.label)) // classlabel_map
      }
    )

    // combines / reduces (adds) all quantization errors and instance counts from each mapper
    val neuronMQEs: RDD[(String, Set[Array[String]])] = neuronsClassLabels.reduceByKey(SOMLayerFunctionsMT.computeNeuronMTClassLabels)

    // NEW START *****************************************************************************

    val neuronsClassLabelsNew = new PairRDDFunctions[(Int,Int),(Array[String],Int)](
      dataset.map { instance =>
        val bmu: NeuronMT = SOMLayerFunctions.findBMUMT(neurons, instance)
        ((bmu.row,bmu.column),(instance.label,1)) // classlabel_map
      }
    )

    val sumClasses: RDD[((Int, Int), (Array[String], Int))] = neuronsClassLabelsNew.reduceByKey((x, y) => {
      val targetX = x._1
      val targetY = y._1

      val sumV: Array[String] = (for((a, b) <- targetX zip targetY) yield (a.toDouble + b.toDouble).toString)

      (sumV, x._2+y._2)
    })

    val avgClasses: RDD[((Int, Int), Array[String])] = sumClasses.map({
      case((row, column),(sumLabel,count)) => {
        val avgLabel = (sumLabel.map(e => (e.toDouble / sumLabel.size).toString))
        ((row,column),avgLabel)
      }
    })

    // NEW END *****************************************************************************

    // combines / reduces (adds) all quantization errors and instance counts from each mapper

    neuronMQEs.collectAsMap
      .map(updateNeuronMTClassLabels) // return to driver
    /***** MapReduce Ends *****/

    avgClasses.collectAsMap()
  }


  def computeTopographicalError(dataset : RDD[InstanceMT]) : Double = {

    val neurons = this.neurons

    /***** MapReduce Begins *****/
    // runs on workers
    // creates a map of (neuron id -> (quantization error, instance count = 1))

    val neuronTopographicErrorMap: RDD[Double] =

    dataset.map { instance =>
      val (bmu, nextBmu) = SOMLayerFunctionsMT.findBMUMTAndNextBMU(neurons, instance)
      var error = 0.0
      if (!NeuronFunctionsMT.areNeighbours(bmu,nextBmu)) {
        error = 1
      }
      error
    }

    neuronTopographicErrorMap.reduce(_ + _)

  }


  def dumpToFile( attributes : Array[Attribute] ) {

    /* Codebook vectors */
    val strNeuronMTs =
      neurons.map(row => {
        row.map(neuron => neuron)
          .mkString("\n")
      }
      )
        .mkString("\n")

    val filename = "output/SOM/SOMLayer_CodebookVectors" + this.layerID + "_" + this._parentLayer + "_" + this._parentNeuronMT.id + ".data"

    val encoding : String = null

    FileUtils.writeStringToFile(new File(filename), strNeuronMTs, encoding)

    /* Dump UMatrix and component planes */

    dumpUMatrix(attributes)

    dumpComponentPlanes(attributes)

    /* Mapped InstanceMT Count */
    val mappedInstanceMTs =
      neurons.map(row =>
        row.map(neuron => neuron.mappedInstanceMTCount.toString())
          .mkString("|")
      )
        .mkString("\n")

    val mappedInstanceMTFileName = "output/SOM/SOMLayer_MappedInstanceMT_" + this.layerID + "_" + this._parentLayer + "_" + this._parentNeuronMT.id + ".data"

    FileUtils.writeStringToFile(new File(mappedInstanceMTFileName), mappedInstanceMTs, encoding)

    if (GHSomConfig.class_labels) {
      val classLabels: String = neurons.map(row =>
        row.map(neuron => neuron.classLabels.mkString(","))
          .mkString("|")
      )
        .mkString("\n")

      val classLabelsFileName = "output/SOM/SOMLayer_ClassLabels_" +
        this.layerID + "_" +
        this._parentLayer + "_" + this._parentNeuronMT.id + ".data"

      FileUtils.writeStringToFile(new File(classLabelsFileName), classLabels, encoding)
    }

    /* LABEL_SOM
    if (GHSomConfig.LABEL_SOM) {

      // convert attribute array to a map of attribute name and min, max value

      val attributeMap = attributes.map ( attrib => (attrib.name, (attrib.minValue, attrib.maxValue) ) )
                                   .toMap
      val mappedLabelsOfNeuronMTs = neurons.map(row =>
        row.map(neuron =>
          neuron.labels.map( label => {
              // find the label in attribute map and deduce its unnormalized value
              val (minVal, maxVal) = attributeMap(label.name)
              Label(label.name, ((label.value * (maxVal - minVal)) + minVal))
            }
          )
        )
      )

      val mappedLabels = mappedLabelsOfNeuronMTs.map(row =>
          row.map(labelSet => labelSet.mkString(","))
             .mkString("|")
          )
          .mkString("\n")

      val mappedLabelsFileName = "SOMLayer_Labels_" +
                                        this.layerID + "_" +
                                        this._parentLayer + "_" + this._parentNeuronMT.id + ".data"

      FileUtils.writeStringToFile(new File(mappedLabelsFileName), mappedLabels, encoding)

    }
    *
    */
  }

  private def updateNeuronStats(
                                 tuple : (String, NeuronMT.NeuronStatsMT)) : Unit = {

    val neuronRowCol = tuple._1.split(",")

    val (neuronRow, neuronCol) = (neuronRowCol(0).toInt, neuronRowCol(1).toInt)

    //println("NeuronMT - " + tuple._1 + "; MQE : " + tuple._2._1 + ";mappedInstanceMT Count : " + tuple._2._3)

    val neuronStats = tuple._2
    neurons(neuronRow)(neuronCol).mqe = neuronStats.mqe
    neurons(neuronRow)(neuronCol).qe = neuronStats.qe
    neurons(neuronRow)(neuronCol).mappedInstanceMTCount = neuronStats.instanceCount
    neurons(neuronRow)(neuronCol).clearMappedInputs()
  }

  /* LABEL_SOM
  private def getNeuronMTLabelSet(
    meanInstanceMT : InstanceMT,
    qeInstanceMT : InstanceMT,
    header : Array[String]
  ) : scala.collection.Set[Label] = {

    /*
     * Reference from SOM Tool Box : http://www.ifs.tuwien.ac.at/dm/download/somtoolbox+src.tar.gz
     */
    val labelMeanVector = header.zip(meanInstanceMT.attributeVector)
                                .map(tup => new Label(name = tup._1, value = tup._2))
                                .filter( label => label.value.getValue >= 0.02 )
                                .sortWith( _.value < _.value )
    val labelQEVector = header.zip(qeInstanceMT.attributeVector)
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
  private def updateNeuronMTLabels(
       tuple : (String, Set[Label])
  ) : Unit = {
      val neuronRowCol = tuple._1.split(",")

      val (neuronRow, neuronCol) = (neuronRowCol(0).toInt, neuronRowCol(1).toInt)

      //println("NeuronMT - " + tuple._1 + "; MQE : " + tuple._2._1 + ";mappedInstanceMT Count : " + tuple._2._3)

      neurons(neuronRow)(neuronCol).labels = tuple._2
  }
  *
  */

  private def updateNeuronMTClassLabels(
                                       tuple : (String, Set[Array[String]])
                                     ) : Unit = {
    val neuronRowCol = tuple._1.split(",")

    val (neuronRow, neuronCol) = (neuronRowCol(0).toInt, neuronRowCol(1).toInt)

    //println("NeuronMT - " + tuple._1 + "; MQE : " + tuple._2._1 + ";mappedInstanceMT Count : " + tuple._2._3)

    neurons(neuronRow)(neuronCol).classLabels = tuple._2
  }

  private def getNeuronMTAndNeighbourSetForGrowing(
                                                  criterion : Double
                                                ) : Set[NeuronMTPair] = {

    // find neurons which have high qe
    var neuronsToExpandList = new mutable.ListBuffer[NeuronMT]()

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

    val neuronNeighbourSet = mutable.Set[NeuronMTPair]()

    for (neuron <- neuronsToExpandList) {
      //println("getNeuronMTAndNeighbourSetForGrowing NeuronMT : " + neuron)
      val dissimilarNeighbour = getMostDissimilarNeighbour(neuron)
      val neuronPair = NeuronMTPair(neuron, dissimilarNeighbour)

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

  private def getMostDissimilarNeighbour(refNeuronMT : NeuronMT) = {
    val neighbours = getNeighbourNeuronMTs(refNeuronMT)

    var dissimilarNeighbour : NeuronMT = null
    var maxDist = 0.0
    // find the dissimilar neighbour
    for (neighbour <- neighbours) {
      val dist = refNeuronMT.neuronInstanceMT.getDistanceFrom(neighbour.neuronInstanceMT)
      if (dist > maxDist) {
        dissimilarNeighbour = neighbour
        maxDist = dist
      }
    }

    dissimilarNeighbour
  }

  private def getNeuronMTAndNeighbourForGrowing(errorNeuronMT : NeuronMT) : NeuronMTPair = {
    var neuronPair : NeuronMTPair = null
    val neighbours = getNeighbourNeuronMTs(errorNeuronMT)
    var dissimilarNeighbour : NeuronMT = null
    var maxDist = 0.0

    // find the dissimilar neighbour
    for (neighbour <- neighbours) {
      val dist = errorNeuronMT.neuronInstanceMT.getDistanceFrom(neighbour.neuronInstanceMT)
      if (dist > maxDist) {
        dissimilarNeighbour = neighbour
        maxDist = dist
      }
    }

    NeuronMTPair(errorNeuronMT, dissimilarNeighbour)

  }

  private def getGrownLayer(neuronPair : NeuronMTPair) : Array[Array[NeuronMT]] = {

    var newNeuronMTs : Array[Array[NeuronMT]] = null
    // add a row
    if (neuronPair.neuron1.row != neuronPair.neuron2.row) {
      newNeuronMTs = getRowAddedLayer(neuronPair)
      _rowDim += 1
    }
    else { // add a column
      newNeuronMTs = getColumnAddedLayer(neuronPair)
      _colDim += 1
    }

    newNeuronMTs
  }

  private def getGrownLayer(neuronNeighbourSet : Set[NeuronMTPair])
  : Array[Array[NeuronMT]] = {

    // get count of rows, columns to be added

    val currentNeuronMTLayer = neurons

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

    val newNeuronMTs = Array.ofDim[NeuronMT](_rowDim + rowsToAdd, _colDim + colsToAdd)

    // copy original array as it is
    for (i <- 0 until neurons.size) {
      for ( j <- 0 until neurons(0).size) {
        newNeuronMTs(i)(j) = neurons(i)(j)
      }
    }

    _rowDim += rowsToAdd
    _colDim += colsToAdd

    // update the new array for each neuron pair
    for (neuronPair <- neuronNeighbourSet) {
      val (rowIdxNeuronMT1, colIdxNeuronMT1) : (Int, Int)= getNeuronMTRowColIdxInLayer(neuronPair.neuron1, newNeuronMTs)

      if (neuronPair.isSameRow) {
        if (neuronPair.neuron1.column < neuronPair.neuron2.column) {
          // update and shift the values after this column
          insertInNextCol(newNeuronMTs, colIdxNeuronMT1)
        }
        else
        // update and shift the values after previous column
          insertInNextCol(newNeuronMTs, colIdxNeuronMT1 - 1)
      }
      else {
        if (neuronPair.neuron1.row < neuronPair.neuron2.row) {
          // update and shift the values after this row
          insertInNextRow(newNeuronMTs, rowIdxNeuronMT1)
        }
        else
        // update and shift the values after previous row
          insertInNextRow(newNeuronMTs, rowIdxNeuronMT1 - 1)
      }
    }

    for (i <- 0 until newNeuronMTs.size) {
      for (j <- 0 until newNeuronMTs(0).size) {
        newNeuronMTs(i)(j).updateRowCol(i, j)
      }
    }

    newNeuronMTs
  }

  private def insertInNextRow(neuronArray : Array[Array[NeuronMT]], row : Int) {

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
        neuronArray(row + 1)(j) = NeuronMT(
          row + 1,
          j,
          InstanceMT.averageInstanceMT(
            neuronArray(row)(j).neuronInstanceMT,
            neuronArray(row + 2)(j).neuronInstanceMT
          )
        )
        neuronArray(row + 1)(j).id = neuronArray(row)(j).id + "+" + neuronArray(row + 2)(j).id
      }
    }
  }

  private def insertInNextCol(neuronArray : Array[Array[NeuronMT]], col : Int) {

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
        neuronArray(i)(col + 1) = NeuronMT(
          i,
          col + 1,
          InstanceMT.averageInstanceMT(
            neuronArray(i)(col).neuronInstanceMT,
            neuronArray(i)(col + 2).neuronInstanceMT
          )
        )
        neuronArray(i)(col + 1).id = neuronArray(i)(col).id + "+" + neuronArray(i)(col + 2).id
      }
    }
  }

  private def getNeuronMTRowColIdxInLayer(neuron : NeuronMT, neuronArray : Array[Array[NeuronMT]]) : (Int, Int) = {

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

  private def getRowAddedLayer(neuronPair : NeuronMTPair) : Array[Array[NeuronMT]] = {
    var newNeuronMTs = Array.ofDim[NeuronMT](_rowDim + 1, _colDim) // add a row

    val minRow = (neuronPair.neuron1.row < neuronPair.neuron2.row) match {
      case true => neuronPair.neuron1.row
      case false => neuronPair.neuron2.row
    }

    for ( i <- 0 to minRow ) { // 0 to minRow included
      for ( j <- 0 until _colDim) {
        newNeuronMTs(i)(j) = neurons(i)(j)
      }
    }

    // average values for new row
    for ( j <- 0 until _colDim ) {
      newNeuronMTs(minRow + 1)(j) =
        NeuronMT(minRow + 1,
          j,
          InstanceMT.averageInstanceMT(neurons(minRow)(j).neuronInstanceMT,
            neurons(minRow+1)(j).neuronInstanceMT))
    }

    // copy remaining
    for (i <- minRow + 1 until _rowDim) {
      for ( j <- 0 until _colDim ) {
        newNeuronMTs(i + 1)(j) = NeuronMT(i+1, j, neurons(i)(j).neuronInstanceMT)
      }
    }

    newNeuronMTs
  }

  private def getColumnAddedLayer(neuronPair : NeuronMTPair) : Array[Array[NeuronMT]] = {
    var newNeuronMTs = Array.ofDim[NeuronMT](_rowDim, _colDim + 1) // add a column

    val minCol = (neuronPair.neuron1.column < neuronPair.neuron2.column) match {
      case true => neuronPair.neuron1.column
      case false => neuronPair.neuron2.column
    }

    for ( j <- 0 to minCol ) { // 0 to minCol included
      for ( i <- 0 until _rowDim) {
        newNeuronMTs(i)(j) = neurons(i)(j)
      }
    }

    // average values for new row
    for ( i <- 0 until _rowDim ) {
      newNeuronMTs(i)(minCol + 1) =
        NeuronMT(i,
          minCol + 1,
          InstanceMT.averageInstanceMT(neurons(i)(minCol).neuronInstanceMT,
            neurons(i)(minCol+1).neuronInstanceMT))
    }

    // copy remaining
    for (j <- minCol + 1 until _colDim) {
      for ( i <- 0 until _rowDim ) {
        newNeuronMTs(i)(j + 1) = NeuronMT(i, j+1, neurons(i)(j).neuronInstanceMT)
      }
    }

    newNeuronMTs
  }

  private def getNeighbourNeuronMTs(neuron : NeuronMT) : Iterable[NeuronMT] = {

    val row = neuron.row

    val col = neuron.column

    val neighbours : mutable.ListBuffer[NeuronMT] = new mutable.ListBuffer[NeuronMT]()

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
  private def getWeightVectorsForChildLayer(neuron : NeuronMT) : Array[Array[DimensionType]]  = {

    var instanceTop : InstanceMT = null
    var instanceLeft : InstanceMT = null
    var instanceRight : InstanceMT = null
    var instanceBottom : InstanceMT = null

    if (neuron.row == 0 && neuron.column == 0) {
      // top-left

      // param neuron is neuron[0,0]
      // surrounded by neuron[0,1], neuron[1,0],
      // output neuron[-1,-1], neuorn[-1,0], neuron[0,-1], neuron[0,0]

      instanceTop = neuron.neuronInstanceMT + (neuron.neuronInstanceMT - this.getNeuronMT(neuron.row + 1,neuron.column).neuronInstanceMT)
      instanceLeft = neuron.neuronInstanceMT + (neuron.neuronInstanceMT - this.getNeuronMT(neuron.row,neuron.column + 1).neuronInstanceMT)
      instanceRight = this.getNeuronMT(neuron.row,neuron.column + 1).neuronInstanceMT
      instanceBottom = this.getNeuronMT(neuron.row + 1,neuron.column).neuronInstanceMT
    }

    else if (neuron.row == 0 && neuron.column == this.colDim - 1) {
      // top-right
      instanceTop = neuron.neuronInstanceMT + (neuron.neuronInstanceMT - this.getNeuronMT(neuron.row + 1,neuron.column).neuronInstanceMT)
      instanceLeft = this.getNeuronMT(neuron.row, neuron.column - 1).neuronInstanceMT
      instanceRight = neuron.neuronInstanceMT + (neuron.neuronInstanceMT - this.getNeuronMT(neuron.row, neuron.column - 1).neuronInstanceMT)
      instanceBottom = this.getNeuronMT(neuron.row + 1, neuron.column).neuronInstanceMT
    }
    else if (neuron.row == this.rowDim - 1 && neuron.column == 0) {
      // bottom left
      instanceTop = this.getNeuronMT(neuron.row - 1, neuron.column).neuronInstanceMT
      instanceLeft = neuron.neuronInstanceMT + (neuron.neuronInstanceMT - this.getNeuronMT(neuron.row, neuron.column + 1).neuronInstanceMT)
      instanceRight = this.getNeuronMT(neuron.row, neuron.column + 1).neuronInstanceMT
      instanceBottom = neuron.neuronInstanceMT + (neuron.neuronInstanceMT - this.getNeuronMT(neuron.row - 1, neuron.column).neuronInstanceMT)
    }
    else if (neuron.row == this.rowDim - 1 && neuron.column == this.colDim - 1) {
      // bottom right
      instanceTop = this.getNeuronMT(neuron.row - 1, neuron.column).neuronInstanceMT
      instanceLeft = this.getNeuronMT(neuron.row, neuron.column - 1).neuronInstanceMT
      instanceRight = neuron.neuronInstanceMT + (neuron.neuronInstanceMT - this.getNeuronMT(neuron.row, neuron.column - 1).neuronInstanceMT)
      instanceBottom = neuron.neuronInstanceMT + (neuron.neuronInstanceMT - this.getNeuronMT(neuron.row - 1, neuron.column).neuronInstanceMT)
    }
    else if (neuron.row == 0) {
      // top row
      instanceTop = neuron.neuronInstanceMT + (neuron.neuronInstanceMT - this.getNeuronMT(neuron.row + 1, neuron.column).neuronInstanceMT)
      instanceLeft = this.getNeuronMT(neuron.row, neuron.column - 1).neuronInstanceMT
      instanceRight = this.getNeuronMT(neuron.row, neuron.column + 1).neuronInstanceMT
      instanceBottom = this.getNeuronMT(neuron.row + 1, neuron.column).neuronInstanceMT
    }
    else if (neuron.column == 0) {
      // left column
      instanceTop = this.getNeuronMT(neuron.row - 1, neuron.column).neuronInstanceMT
      instanceLeft = neuron.neuronInstanceMT + (neuron.neuronInstanceMT - this.getNeuronMT(neuron.row, neuron.column + 1).neuronInstanceMT)
      instanceRight = this.getNeuronMT(neuron.row, neuron.column + 1).neuronInstanceMT
      instanceBottom = this.getNeuronMT(neuron.row + 1, neuron.column).neuronInstanceMT
    }
    else if (neuron.row == this.rowDim - 1) {
      // bottom row
      instanceTop = this.getNeuronMT(neuron.row - 1, neuron.column).neuronInstanceMT
      instanceLeft = this.getNeuronMT(neuron.row, neuron.column - 1).neuronInstanceMT
      instanceRight = this.getNeuronMT(neuron.row, neuron.column + 1).neuronInstanceMT
      instanceBottom = neuron.neuronInstanceMT + (neuron.neuronInstanceMT - this.getNeuronMT(neuron.row - 1, neuron.column).neuronInstanceMT)
    }
    else if (neuron.column == this.colDim - 1) {
      // right column
      instanceTop = this.getNeuronMT(neuron.row - 1, neuron.column).neuronInstanceMT
      instanceLeft = this.getNeuronMT(neuron.row, neuron.column - 1).neuronInstanceMT
      instanceRight = neuron.neuronInstanceMT + (neuron.neuronInstanceMT - this.getNeuronMT(neuron.row, neuron.column - 1).neuronInstanceMT)
      instanceBottom = this.getNeuronMT(neuron.row + 1, neuron.column).neuronInstanceMT
    }
    else {
      // middle cells
      instanceTop = this.getNeuronMT(neuron.row - 1, neuron.column).neuronInstanceMT
      instanceLeft = this.getNeuronMT(neuron.row, neuron.column - 1).neuronInstanceMT
      instanceRight = this.getNeuronMT(neuron.row, neuron.column + 1).neuronInstanceMT
      instanceBottom = this.getNeuronMT(neuron.row + 1, neuron.column).neuronInstanceMT
    }

    val weightVector00 = InstanceMTFunctions.getAverageInstanceMT(neuron.neuronInstanceMT,
      instanceTop,
      instanceLeft).attributeVector
    val weightVector01 = InstanceMTFunctions.getAverageInstanceMT(neuron.neuronInstanceMT,
      instanceTop,
      instanceRight).attributeVector
    val weightVector10 = InstanceMTFunctions.getAverageInstanceMT(neuron.neuronInstanceMT,
      instanceLeft,
      instanceBottom).attributeVector
    val weightVector11 = InstanceMTFunctions.getAverageInstanceMT(neuron.neuronInstanceMT,
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
        var instance1 : InstanceMT = null
        var instance2 : InstanceMT = null
        if (row % 2 != 0 && col %2 == 0) {
          instance1 = this.neurons((row - 1) / 2)(col / 2).neuronInstanceMT
          instance2 = this.neurons((row - 1) / 2 + 1)(col / 2).neuronInstanceMT
          umatrix(row)(col) = instance1.getDistanceFrom(instance2)
        }
        else if (row % 2 == 0 && col %2 != 0) {
          instance1 = this.neurons(row / 2)((col - 1)/2).neuronInstanceMT
          instance2 = this.neurons(row / 2)((col - 1)/2 + 1).neuronInstanceMT
          umatrix(row)(col) = instance1.getDistanceFrom(instance2)
        }
        else if (row % 2 != 0 && col % 2 != 0) {
          instance1 = this.neurons((row - 1) / 2)((col - 1) / 2).neuronInstanceMT
          instance2 = this.neurons((row - 1) / 2 + 1)((col - 1)/2 + 1).neuronInstanceMT
          val dist1 = instance1.getDistanceFrom(instance2)

          instance1 = this.neurons((row - 1)/2 + 1)((col - 1)/2).neuronInstanceMT
          instance2 = this.neurons((row - 1)/2)((col - 1)/2 + 1).neuronInstanceMT
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

    val filename = "output/SOM/UMatrix_" + this.layerID + "_" + this._parentLayer + "_" + this._parentNeuronMT.id + ".mat"

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
            neuron => neuron.neuronInstanceMT.attributeVector(idx).getImagePixelValue(domainValues)
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

      val filename = "output/SOM/ComponentPlane_" + name + "_" + this.layerID + "_" + this._parentLayer + "_" + this._parentNeuronMT.id + ".mat"

      val encoding : String = null

      FileUtils.writeStringToFile(new File(filename), attribComponentPlanePixelValues, encoding)
    }
  }
}

object SOMLayerFunctionsMT {


  def testModel(neurons : Array[Array[NeuronMT]],
                classAssignments: Map[(Int, Int), Array[String]],
                testData: RDD[InstanceMT]): RDD[(InstanceMT, Double, Array[String])] = {

    val classified: RDD[(InstanceMT, Double, Array[String])] = testData.map(instance => {

      var responseFlag = false
      var distances: Map[(Int, Int), Double] = findAllDistances(neurons, instance)
      var dist = 0.0
      var instanceClass = Array("init")

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
            instanceClass = Array("none")
          }

        }
        (instance,dist,instanceClass)

      }

      else
        (instance,0.0,Array("none"))

    })

    classified
  }
  //********************************************************************************************
  def getNeuronMTUpdatesTuples(instancePartition : Iterator[InstanceMT], neurons: Array[Array[NeuronMT]]) = {

  }

  def findAllDistances(neurons : Array[Array[NeuronMT]], instance : InstanceMT): Map[(Int, Int), Double] = {

    var distances : Map[(Int,Int),Double] = Map[(Int,Int),Double]()

    for (i <- 0 until neurons.size) {
      for (j <- 0 until neurons(0).size) {
        val dist = neurons(i)(j).neuronInstanceMT.getDistanceFrom(instance)
        distances += ((i,j) -> dist)
      }
    }
    distances
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
  def findBMUMTAndNextBMU(neurons : Array[Array[NeuronMT]], instance : InstanceMT) : (NeuronMT, NeuronMT) = {
    var bmu : NeuronMT = neurons(0)(0)
    var nextBmu : NeuronMT = null

    var minDist = instance.getDistanceFrom(bmu.neuronInstanceMT)
    var nextMinDist = Double.MaxValue

    for (i <- 0 until neurons.size) {
      for (j <- 0 until neurons(0).size) {
        val dist = neurons(i)(j).neuronInstanceMT.getDistanceFrom(instance)
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

  def combineNeuronMTUpdates(
                            a : SOMLayerMT.NeuronMTUpdate,
                            b : SOMLayerMT.NeuronMTUpdate
                          ) : SOMLayerMT.NeuronMTUpdate = {
    // for both elements of tuples,
    // zip up the corresponding arrays of a & b and add them
    // TODO : combineWithInstanceMT(b.numerator)
    SOMLayerMT.NeuronMTUpdate(InstanceMTFunctions.combineNeighbourhoodFactorUpdates(a.numerator,b.numerator), a.denominator + b.denominator)
  }

  def computeUpdatedNeuronMTVector(
                                  neuronUpdate : SOMLayerMT.NeuronMTUpdate
                                ) : InstanceMT = {
    InstanceMTFunctions.divideInstanceMTByCumulativeNeighbourhoodFactor(neuronUpdate.numerator, neuronUpdate.denominator)
  }

  /* Input : (QE , Count)
   * Output : (QE_sum, Count_sum)
   */
  def combineNeuronMTsQE(
                        neuron1 : NeuronMT.NeuronStatsMT,
                        neuron2 : NeuronMT.NeuronStatsMT
                      ) : NeuronMT.NeuronStatsMT = {
    NeuronMT.NeuronStatsMT(qe = neuron1.qe + neuron2.qe, instanceCount = neuron1.instanceCount + neuron2.instanceCount)
  }

  /**
    * input : tuple of (qe, num_of_instances)
    * output : tuple of (mqe , qe, num_of_instances)
    */
  def computeMQEForNeuronMT(
                           neuron : NeuronMT.NeuronStatsMT
                         ) : NeuronMT.NeuronStatsMT = {
    NeuronMT.NeuronStatsMT(mqe = neuron.qe / neuron.instanceCount, qe = neuron.qe, instanceCount = neuron.instanceCount)
  }

  def mergeDatasetsForHierarchicalExpansion(
                                             rec1 : GHSom.LayerNeuronRDDRecordMT,
                                             rec2 : GHSom.LayerNeuronRDDRecordMT
                                           ) : List[GHSom.LayerNeuronRDDRecordMT] = {
    List(rec1) ++ List(rec2)
  }

  /**
    * Input : (QE , Count)
    * Output : (QE_sum, Count_sum)
    */
  def combineNeuronMTsMeanAndQEForLabels(
                                        t1: (InstanceMT, InstanceMT, Long),
                                        t2: (InstanceMT, InstanceMT, Long)
                                      ) : (InstanceMT, InstanceMT, Long) = {
    (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
  }

  /**
    * input : tuple of (qe, num_of_instances)
    * output : tuple of (mqe , qe, num_of_instances)
    */
  def computeMeanAndQEForLabels(
                                 tup : (InstanceMT, InstanceMT, Long)
                               ) : (InstanceMT, InstanceMT, Long) = {
    (tup._1 / tup._3, tup._2 / tup._3, tup._3)
  }

  def computeNeuronMTClassLabels(
                                labelSet1 : Set[Array[String]], labelSet2 : Set[Array[String]]
                              ) : Set[Array[String]] = {
    labelSet1.union(labelSet2)
  }

  def computeNeuronMTClassLabelsList(
                                    labelSet1 : List[String], labelSet2 : List[String]
                                  ) : List[String] = {
    labelSet1.union(labelSet2)
  }

}

object SOMLayerMT {

  private var layerId = 0

  def apply(
             parentNeuronMT : NeuronMT,
             parentLayer : Int,
             rowDim : Int,
             colDim : Int,
             vectorSize : Int,
             initializationInstanceMTs : Array[InstanceMT]) = {
    layerId += 1
    //new SOMLayer(layerId, rowDim, colDim, parentNeuronMTID, parentLayer, parentNeuronMTMQE /*mqe_change*/ , vectorSize )
    new SOMLayerMT(
      layerId,
      rowDim,
      colDim,
      parentNeuronMT,
      parentLayer,
      vectorSize,
      initializationInstanceMTs
    )
  }

  // TODO : NeuronMTUpdate(numerator : InstanceMT, denominator : Double)
  case class NeuronMTUpdate(numerator : InstanceMT, denominator : Double)

  def main(args : Array[String]) {
  }

}

