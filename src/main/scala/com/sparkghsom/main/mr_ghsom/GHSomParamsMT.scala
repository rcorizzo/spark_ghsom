package com.sparkghsom.main.mr_ghsom

import com.sparkghsom.main.datatypes.{Dimension, DimensionType, DimensionTypeEnum, DistanceHierarchyDimension, DistanceHierarchyElem}

import scala.collection.{Map, immutable, mutable}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import com.sparkghsom.main.globals.{GHSomConfig, SparkConfig}

import scala.math.{abs, pow, sqrt}
import scala.concurrent.duration.{Duration, FiniteDuration}
import java.util.concurrent.TimeUnit._
import java.io.{File, FileInputStream}

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.IllegalClassException
import com.sparkghsom.main.datatypes.DistanceHierarchyDimension
import com.sparkghsom.main.globals.GHSomConfig.VarianceType
import com.sparkghsom.main.mr_ghsom.GHSom.{LayerNeuron, LayerNeuronMT, LayerNeuronRDDRecordMT}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}

import scala.collection.mutable.ArrayBuffer

class GHSomParamsMT() extends Serializable {
  def vecToDouble(v: Array[String]): Array[Double] = {
    v.map(_.toDouble)
  }

  def trainAndTestRegression(dataset : RDD[InstanceMT],
                             testDataset: RDD[InstanceMT],
                             groundTruth: RDD[(Int, Array[String])],
                             attributes : Array[Attribute] = null,
                             tau1: Double,
                             tau2: Double,
                             epochsValue: Int): (RDD[(Array[Double], Array[Double])],Double,Double) = {

    val startLearningTime = System.currentTimeMillis()

    dataset.persist(StorageLevel.MEMORY_ONLY)
    val layer0Neuron : NeuronMT = NeuronMT(0,0, null)
    // Compute m0 - Mean of all the input

    var criterion = computeCriterion(dataset, layer0Neuron) //0.0

    println("total Instances : " +  layer0Neuron.mappedInstanceMTCount)
    if (GHSomConfig.mqe_criterion)
      println("mqe0 : " + criterion)
    else
      println("qe0 : " + criterion)

    // ID of a particular SOMLayer. there can be many maps in the same layer. this is like a PK
    val layer = 0

    var layerNeuronRDD: RDD[LayerNeuronRDDRecordMT] = dataset.map(instance => GHSom.LayerNeuronRDDRecordMT(layer, layer0Neuron.id, instance))
    dataset.unpersist()

    val layerQueue = new mutable.Queue[GHSom.LayerNeuronMT]()

    //layerQueue.enqueue(LayerNeuron(layer, layer0Neuron.id, mqe0)) //mqe_change
    layerQueue.enqueue(GHSom.LayerNeuronMT(layer, layer0Neuron))

    var hierarchicalGrowth = true

    // Create first som layer of 2 x 2

    val attribVectorSize = attributes.size

    dumpAttributes(attributes)

    val randomInstances = generateRandomInstances(attributes, 4)

    var multiLayerPredictions : Array[((Int, Double), Array[String])] = Array.empty[((Int, Double), Array[String])]

    while(!layerQueue.isEmpty) {
      val layerLearningStartTime = System.currentTimeMillis()
      val currentLayerNeuron = layerQueue.dequeue

      println("Processing for parentLayer :" + currentLayerNeuron.parentLayer + ", parent neuron : " + currentLayerNeuron.parentNeuron.id)
      // make dataset for this layer

      val currentDataset: RDD[InstanceMT] = layerNeuronRDD.filter(obj =>
        obj.parentLayerID.equals(currentLayerNeuron.parentLayer) &&
          obj.parentNeuronID.equals(currentLayerNeuron.parentNeuron.id)
      )
        .map(obj => obj.instance)

      currentDataset.persist(StorageLevel.MEMORY_AND_DISK_SER)

      if(currentDataset.count()>0) {

        val instanceCount = currentLayerNeuron.parentNeuron.mappedInstanceMTCount

        println("Instance count in dataset for current layer " + instanceCount)

        var continueTraining = false

        val currentLayer = SOMLayerMT(
          rowDim = GHSomConfig.init_layer_size,
          colDim = GHSomConfig.init_layer_size,
          parentNeuronMT = currentLayerNeuron.parentNeuron,
          parentLayer = currentLayerNeuron.parentLayer,
          vectorSize = attribVectorSize,
          initializationInstanceMTs = randomInstances
        )

        if (currentLayerNeuron.parentLayer != 0) {
          currentLayer.initializeLayerWithParentNeuronMTWeightVectors
        }

        var epochs = epochsValue
        var prevMQE_m = 0.0
        do {
          val growthIterationStartTime = System.currentTimeMillis()
          //var epochs = currentLayer.totalNeurons * 2
          // runs on driver
          currentLayer.clearMappedInputs

          println("epochs : " + epochs)

          // MapReduce : Uses driver and workers returning the updated values to the driver
          currentLayer.train(currentDataset, epochs)

          // MapReduce : Uses driver and workers, updating the neurons at the driver
          // computes the layer's MQE_m and updates the mqe for individual neurons in the layer
          currentLayer.computeStatsForLayer(currentDataset)

          if (GHSomConfig.debug) {
            println("After Training")
            currentLayer.display()
          }
          //currentLayer.dumpToFile(attributes)
          var needsTraining = false
          var mqe_m = 0.0
          var errorNeuron : NeuronMT = null

          if (GHSomConfig.mqe_criterion) {
            val tup = currentLayer.checkMQE(tau1) //mqe_change
            needsTraining = tup._1
            mqe_m = tup._2
            errorNeuron = tup._3
          }
          else {
            val tup = currentLayer.checkQE(tau1)
            needsTraining = tup._1
            mqe_m = tup._2
            errorNeuron = tup._3
          }

          if (needsTraining && currentLayer.totalNeuronMTs < instanceCount/2) {
            var fastGrowth = true
            println("Diff from criterion: " + abs(currentLayer.parentNeuronMT.mqe  * tau1 - mqe_m))
            if (GHSomConfig.mqe_criterion) {
              if (abs(currentLayer.parentNeuronMT.mqe * tau1 - mqe_m) < 0.05)
                fastGrowth = false
            }
            else {
              if (abs(currentLayer.parentNeuronMT.qe * tau1 - mqe_m) < 0.05)
                fastGrowth = false
            }

            if (GHSomConfig.growth_multiple && fastGrowth) {
              println("Growing fast...")
              currentLayer.growMultipleCells(tau1)
            }
            else {
              println("Growing Slow...")
              currentLayer.growSingleRowColumn(errorNeuron)
            }
            continueTraining = true
            prevMQE_m = mqe_m
            println("Growing")
            currentLayer.gridSize
          }
          else if (needsTraining && prevMQE_m - mqe_m > 0.1) {
            epochs = epochs * 2
            continueTraining = true
            prevMQE_m = mqe_m
            println("Increasing epochs " + epochs )
          }
          else {
            continueTraining = false
            println("Done training")
          }
          prevMQE_m = mqe_m
          println("Growth Training time : " + Duration.create(System.currentTimeMillis() - growthIterationStartTime, MILLISECONDS))
          if (GHSomConfig.compute_topographical_error) {
            println("Topographic Error: " + currentLayer.computeTopographicalError(currentDataset) / instanceCount)
          }

        } while(continueTraining)

        println("Layer " + currentLayer.layerID + " Training time : " + Duration.create(System.currentTimeMillis() - layerLearningStartTime, MILLISECONDS))
        //currentLayer.train(currentDataset, epochs)

        // ********************************************************************************************************
        // ***  Predictive stage  *********************************************************************************
        // ********************************************************************************************************

        if (GHSomConfig.class_labels) {

          val neuronsMap: Array[Array[NeuronMT]] = currentLayer.getNeuronMTMap()

          val classAssignments: Map[(Int, Int), Array[String]] = {    // map neuron -> class
            currentLayer.computeClassLabelsRegression(currentDataset)
          }

          println("class assignments for neurons")
          classAssignments.take(5).foreach(x => println(x._1 + " " + x._2.mkString(",")))

          val predictions: RDD[(InstanceMT, Double, Array[String])] = SOMLayerFunctionsMT.testModel(neuronsMap, classAssignments, testDataset)
          val currentLayerPredictions: Array[((Int, Double), Array[String])] = predictions.map(e => ((e._1.id,e._2),e._3)).collect()

          multiLayerPredictions = multiLayerPredictions ++ currentLayerPredictions

          println("Making predictions for current layer...")
          //predictions.take(10).foreach(x => {
           // val actualClass: Array[String] = groundTruth.filter(g => g._1 == x._1.id).map(e => e._2).take(1).apply(0)
            //println("actual: " + actualClass.mkString(","))
            //println("predicted: " + x._3.mkString(","))
            //println("distance: " +  x._2)
          //})
          println("...")

        }

        currentLayer.dumpToFile(attributes)

        println("Hierarchical criterion: " + GHSomConfig.tau2 + "x" + criterion + "=" + (criterion * GHSomConfig.tau2))

        val neuronsToExpand : mutable.Set[NeuronMT] = currentLayer.getNeuronMTsForHierarchicalExpansion(criterion * GHSomConfig.tau2, layer0Neuron.mappedInstanceMTCount)

        neuronsToExpand.foreach { neuron =>
          if (GHSomConfig.debug) {
            if (GHSomConfig.mqe_criterion)
              println("Expand neuron: " + currentLayer.layerID + " : " + neuron.id + " : " + neuron.mqe)
            else
              println("Expand neuron: " + currentLayer.layerID + " : " + neuron.id + " : " + neuron.qe)
          }
          layerQueue.enqueue(GHSom.LayerNeuronMT(currentLayer.layerID, neuron))
        }

        layerNeuronRDD = layerNeuronRDD ++ currentLayer.getRDDForHierarchicalExpansion(currentDataset, neuronsToExpand)

        currentDataset.unpersist()

        layerNeuronRDD = layerNeuronRDD.filter( record => !(record.parentLayerID.equals(currentLayerNeuron.parentLayer) &&
          record.parentNeuronID.equals(currentLayerNeuron.parentNeuron.id))
        )
      }

    }

    // multi layer model built
    // compare predictions at for each layer and choose best

    val sc = SparkConfig.getSparkContext
    val MLP: RDD[((Int, Double), Array[String])] = sc.parallelize(multiLayerPredictions)

    val bestPreds: RDD[(Int, (Array[String], Double))] = MLP.map({
      case((id, distance), classAssigned) => {
        ((id),(classAssigned,distance))
      }
    }).reduceByKey((x,y) => {
      if(x._2 < y._2)
        x
      else
        y
    })

    // create (prediction, groundTruth) RDD

    val pairRddPredictions: RDD[(Int, Array[String])] = bestPreds.map({
      case(id,(prediction, distance)) => {
        (id, prediction)
      }
    })
    /*
        println("pairRddPredictions")
        pairRddPredictions.take(10).foreach(println)
        println()
    */
    val joinedRddPredictions: RDD[(Array[String], Array[String])] = pairRddPredictions.join(groundTruth).map({
      case(id, (prediction, actual)) => (prediction, actual)
    })

    /*
    println("joinedRddPredictions")
    joinedRddPredictions.take(10).foreach(e => println(e._1.mkString(",") + " +++ " + e._2.mkString(",")))
    println()
*/

    // re-map string to double and calculate errors

    val pred_actual_numeric: RDD[(Array[Double], Array[Double])] = joinedRddPredictions.map(e => (vecToDouble(e._1), vecToDouble(e._2)))

    val predicted: Array[Array[Double]] = pred_actual_numeric.map(x => x._1).collect()
    val actual: Array[Array[Double]] = pred_actual_numeric.map(x => x._2).collect()

    println("Training time : " + Duration.create(System.currentTimeMillis() - startLearningTime, MILLISECONDS))

    var errorList = Array.empty[(Double,Double)]

    predicted.zip(actual).map(
      e => {
        val (rmse,mae) = calculateErrors(e._1,e._2)
        //println("Single error: " + rmse + " " + mae)
        errorList :+= (rmse,mae)
      }
    )

    val rmseTot = errorList.map(e => e._1).sum  / errorList.size
    val maeTot = errorList.map(e => e._2).sum  / errorList.size

    (pred_actual_numeric, rmseTot, maeTot)
  }
  // ****************************************************************************************************
  def calculateErrors(predicted: Array[Double], expected: Array[Double]): (Double, Double) = {

    // RMSE
    var sum = 0.0
    var sumExp = 0.0
    val temp: Double = Math.pow(10, 3)

    for(i <- 0 to predicted.length-1) {
      val diffsquare_norm = Math.pow(predicted.apply(i) - expected.apply(i), 2)
      sum = sum + diffsquare_norm

      var diffExp = Math.pow(((predicted.apply(i)-expected.apply(i))/(expected.apply(i))), 2)

      if(java.lang.Double.isNaN(diffExp)||java.lang.Double.isInfinite(diffExp))
        diffExp=0.0

      sumExp = sumExp + diffExp
    }

    var rmse = Math.sqrt(sum/expected.length)
    rmse = Math.round(rmse * temp) / temp

    // MAE (Mean Absolute Error)
    sum = 0.0

    for(i <- 0 to predicted.length-1) {
      var diff = 0.0

      diff = Math.abs(predicted.apply(i)-expected.apply(i))
      sum = sum + diff
    }

    var mae = sum / expected.length
    mae = Math.round(mae * temp) / temp

    //println("RMSE   " + rmse + " | MAE   " + mae)
    (rmse,mae)
  }

  private def computeCriterion(dataset : RDD[InstanceMT], layer0Neuron : NeuronMT) : Double = {
    var criterion = 0.0
    var instanceCount = 0L
    if(GHSomConfig.variance_method == VarianceType.COEFF_UNALIKELIHOOD) {
      //
      val (instanceSumForVariance,totalInstances) = dataset.map{instance =>
        val newInstance = InstanceMT(0,
          instance.label,
          instance.attributeVector.map(
            dimension => dimension.attributeType match {
              case DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL =>
                new CategoricalVarianceDimension(
                  dimension.attributeName,
                  dimension.asInstanceOf[DistanceHierarchyDimension].attributeValue.symbol,
                  dimension.attributeType
                )
              case _ => dimension
            }
          )
        )
        (newInstance,1L)
      }
        .reduce(GHSomFunctions.computeSumAndNumOfInstancesMT)

      // runs on driver
      val m0 = instanceSumForVariance.attributeVector.map {
        attribute => attribute / totalInstances
      }

      val categoricalVariance = m0.map{
        dimension => dimension.attributeType match {
          case DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL => {
            dimension.asInstanceOf[CategoricalVarianceDimension].getVariance
          }
          case _ => 0.0
        }
      }.reduce(_ + _)

      var numericalVariance = dataset.map{instance =>
        var distance = 0.0

        instance.attributeVector.zip(m0).foreach{
          dimensionPair => dimensionPair._1.attributeType match {
            case DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC => {
              distance += pow(dimensionPair._1.getDissimilarityValue(dimensionPair._2),2)
            }
            case _ => 0.0
          }
        }
        distance
        //sqrt(distance) variane instead of deviation
      }
        .reduce(_ + _) / totalInstances
      println("Variance (C,N):" + (categoricalVariance/2) + ","+ numericalVariance)
      criterion = (categoricalVariance/2) + numericalVariance
      instanceCount = totalInstances
    }
    else {
      // executes on workers
      val (sumOfInstances, totalInstances) = dataset.map(instance =>
        (instance, 1L))
        .reduce(GHSomFunctions.computeSumAndNumOfInstancesMT) // returns to the driver (Instance[attribSum], total count)

      // runs on driver
      val m0 = sumOfInstances.attributeVector.map {
        attribute => attribute / totalInstances
      }

      //val m0 = sumOfInstances.attributeVector.map { attribute => attribute }
      val meanInstance = InstanceMT(0,Array("0thlayer"), m0)

      println("Mean Instance : " + meanInstance)

      //val layer0Neuron = Neuron(0,0,meanInstance)
      // Compute mqe0 - mean quantization error for 0th layer


      // map runs on workers, computing distance value of each instance from the meanInstance
      if (GHSomConfig.mqe_criterion)
        criterion  = dataset.map(instance => meanInstance.getDistanceFrom(instance)).reduce(_ + _) / totalInstances //mqe_change
      else
        criterion = dataset.map(instance => meanInstance.getDistanceFrom(instance)).reduce(_ + _)

      instanceCount = totalInstances
    }

    if (GHSomConfig.mqe_criterion)
      layer0Neuron.mqe = criterion//mqe_change
    else
      layer0Neuron.qe = criterion

    layer0Neuron.mappedInstanceMTCount = instanceCount
    criterion
  }

  /**
    * Class for categorical variance calculation
    */
  private class CategoricalVarianceDimension (
                                               val _attributeName : String,
                                               val _attributeValue : String,
                                               val _attributeType : DimensionTypeEnum.Value,
                                               private val _valueFreqMap : mutable.Map[String, Double] = mutable.Map[String, Double]()
                                             )
    extends Dimension(_attributeValue){
    def attributeName : String = _attributeName
    def attributeType : DimensionTypeEnum.Value = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL

    def getVariance : Double = {
      _valueFreqMap.mapValues(value => value * (1-value)).values.reduce(_ + _)
    }

    override def +(that : DimensionType) : DimensionType = {
      that match {
        case other : CategoricalVarianceDimension => {
          val newValueFreqMap = mutable.Map[String,Double]()

          /*
           * Combine the values and frequencies from both NominalDimensions.
           * * CASE 1 : If both have empty valueFreqMaps, then both are fresh (no
           *   operations performed after creation) NominalDimensions,
           *   hence, add both the values to the new map with freq 1
           * * CASE 2 : Else If "this" has empty valueFreqMap and "other" has elements in the
           *   frequency map,
           *   copy other's freq map to new map and add "this" value to
           *   the new map (update/add)
           * * CASE 3 : Else If "other" has empty valueFreqMap and "this" has elements in the
           *   frequency map,
           *   copy this's freq map to new map and add "other" value to
           *   the new map (update/add)
           * * CASE 4 : Else both "this" and "other" have updates made on them, so
           *   combine value Freq Maps of them
           *
           */
          /* CASE 1 */
          if (this._valueFreqMap.isEmpty && other._valueFreqMap.isEmpty) {
            if (this._value.equals(other._value))
              newValueFreqMap.put(this._value, 2)
            else {
              newValueFreqMap.put(this._value, 1)
              newValueFreqMap.put(other._value, 1)
            }
          }
          /* CASE 2 */
          else if (this._valueFreqMap.isEmpty) {
            newValueFreqMap.put(this._value, 1)
            other._valueFreqMap.foreach{
              case (value, freq) =>
                if (newValueFreqMap.contains(value))
                  newValueFreqMap.put(value, freq + newValueFreqMap(value))
                else
                  newValueFreqMap.put(value, freq)
            }
          }
          /* CASE 3 */
          else if (other._valueFreqMap.isEmpty) {
            newValueFreqMap.put(other._value, 1)
            this._valueFreqMap.foreach{
              case (value, freq) =>
                if (newValueFreqMap.contains(value))
                  newValueFreqMap.put(value, freq + newValueFreqMap(value))
                else
                  newValueFreqMap.put(value, freq)
            }
          }
          /* CASE 4 */
          else {
            // copy values from this value-freq map and common ones
            this._valueFreqMap.foreach{
              case (value, freq) => {
                if (other._valueFreqMap.contains(value)) {
                  val newFreq = other._valueFreqMap(value) + freq
                  newValueFreqMap.put(value, newFreq)
                }
                else
                  newValueFreqMap.put(value, freq)
              }
            }
          }
          // copy the remaining ones from other
          other._valueFreqMap.foreach {
            case (value, freq) => {
              if (!newValueFreqMap.contains(value)) {
                newValueFreqMap.put(value,freq)
              }
            }
          }
          if (GHSomConfig.ignore_unknown)
            newValueFreqMap.remove("UNKNOWN")
          new CategoricalVarianceDimension(this.attributeName, this.value, this.attributeType, newValueFreqMap)
        }

        case _ => throw new IllegalClassException("Illegal class in CategoricalVarianceDimension")
      }
    }

    override def compare( that : DimensionType ) : Int = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def equals( d2 : DimensionType ) : Boolean = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def toString() : String = {
      _valueFreqMap.mkString(",")
    }
    override def !=(d2 : DimensionType) : Boolean = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def ==(d2 : DimensionType) : Boolean = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def <(d2 : DimensionType) : Boolean = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def <=(d2 : DimensionType) : Boolean = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def >(d2 : DimensionType) : Boolean = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def >=(d2 : DimensionType) : Boolean = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }

    override def -(d2 : DimensionType) : DimensionType = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def getDissimilarityValue(d2 : DimensionType) : Double = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def /(num : Long) : DimensionType = {
      val newValueFreqMap : mutable.Map[String, Double] = _valueFreqMap.map{ case (value,freq) => (value,freq / num) }
      new CategoricalVarianceDimension(this.attributeName, this._value, this._attributeType, newValueFreqMap)
    }
    override def /(num : Double) : DimensionType = {
      val newValueFreqMap : mutable.Map[String, Double] = _valueFreqMap.map{ case (value,freq) => (value,freq / num) }
      new CategoricalVarianceDimension(this.attributeName, this._value, this._attributeType, newValueFreqMap)
    }
    override def /(d2 : DimensionType) : DimensionType = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def *(d2 : DimensionType) : DimensionType  = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def hashCode : Int = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def applyNeighbourhoodFactor(factor : Double) : DimensionType = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def divideByCumulativeNeighbourhoodFactor(factor : Double) : DimensionType = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def getUpdateValue(oldValue : DimensionType) : DimensionType = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }

    override def getFreshDimension : DimensionType = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }

    override def avgForGrowingLayerWith(that : DimensionType) : DimensionType = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }

    override def cloneMe : DimensionType  = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
    override def getImagePixelValue (domainValues : Array[_ <: Any] = null) : Double = {
      throw new UnsupportedOperationException("CategoricalVarianceDimension")
    }
  }

  private def dumpAttributes(attributes : Array[Attribute]) {
    val encoding : String = null
    val attributeFileName = "attributes.txt"

    val attributeString = attributes.mkString("\n")

    FileUtils.writeStringToFile(new File(attributeFileName), attributeString, encoding)
  }

  private def generateRandomInstances(attributes : Array[Attribute], size : Int) : Array[InstanceMT] = {
    Array.tabulate(size)(
      idx => {
        InstanceMT(
          0,
          Array(idx.toString()),
          attributes.map(
            attribute => attribute.randomValueFunction(attribute)
          )
        )
      }
    )
  }
}

object GHSomParamsMTFunctions {
  def computeSumAndNumOfInstances( a: (Instance, Long), b : (Instance, Long) ) : (Instance,Long) = {
    (a._1 + b._1, a._2 + b._2)
  }
}

object GHSomParamsMT {

  def apply() : GHSomParamsMT = {
    new GHSomParamsMT()
  }

  // layer and neuron is the pared layer and neuron
  case class LayerNeuron(parentLayer : Int, parentNeuron : Neuron) {
    override def equals( obj : Any ) : Boolean = {
      obj match {
        case o : LayerNeuron => {
          (this.parentLayer.equals(o.parentLayer) && this.parentNeuron.id.equals(o.parentNeuron.id))
        }
        case _ => false
      }
    }

    override def hashCode : Int = parentLayer.hashCode() + parentNeuron.id.hashCode()
  }

  case class LayerNeuronRDDRecord(parentLayerID : Int, parentNeuronID : String, instance : Instance) {
    override def equals( obj : Any ) : Boolean = {
      obj match {
        case o : LayerNeuronRDDRecord => {
          (this.parentLayerID.equals(o.parentLayerID) &&
            this.parentNeuronID.equals(o.parentNeuronID) &&
            this.instance.equals(o.instance))
        }
        case _ => false
      }
    }

    override def hashCode : Int = parentLayerID.hashCode() + parentNeuronID.hashCode() + instance.hashCode()
  }



}
