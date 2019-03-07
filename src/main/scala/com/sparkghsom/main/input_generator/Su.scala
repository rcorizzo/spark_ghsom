package com.sparkghsom.main.input_generator

import java.io.FileInputStream

import com.sparkghsom.main.datatypes.{DimensionType, DimensionTypeEnum, DistanceHierarchyDimension, DistanceHierarchyElem}
import com.sparkghsom.main.globals.{GHSomConfig, SparkConfig}
import com.sparkghsom.main.mr_ghsom.{Attribute, GHSom, Instance}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.{max, min}

class Su(val dataset : RDD[String]) extends Serializable{

  val maxF = 5565
  val attributes = new Array[Attribute](maxF+1)

  for (i <- 0 to maxF-1) {
    val elem = Attribute(name = "V" + (i),
      index = i,
      dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
      randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue)

    attributes(i) = elem
  }

  val classAttr = Attribute(name = "class" ,
    index = maxF,
    dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
    randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue)

  attributes(maxF) = classAttr

  private val datasetOfInstanceObjs = instanizeDataset(dataset)
  
  case class Data ( id: Int, className : String, attributeVector : Array[DimensionType] ) {
    def getInstanceObj = {
      Instance(id, className, attributeVector)
    }
  }
  
  def printDataset() {
    // println(datasetRDD.count)
  }
  
  def getDataset : RDD[Instance] = datasetOfInstanceObjs
  
  private def instanizeDataset(dataset : RDD[String]) : RDD[Instance] = {

    val pureDataRDD: RDD[PureData] = getPureDataRDD(dataset)
    val attributeMap = getIndexAttributeTypeValueRDD( pureDataRDD )                                              
    val (minAttributeMap, maxAttributeMap) = getAttributeMinMaxValuesMap(attributeMap)
    val domainAttributeMap = getAttributeDomainValuesMap(attributeMap)

    for (i <- 0 until attributes.size) {
      attributes(i).dimensionType match {
        case DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL =>
          attributes(i).domainValues = domainAttributeMap(i).filter { value => !value.equals("UNKNOWN") }
                                                            .toArray
                                                            .sorted
        case DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC =>
          attributes(i).maxValue = maxAttributeMap(i)
          attributes(i).minValue = minAttributeMap(i)
        case _ => throw new UnsupportedOperationException("Unsupported DimensionType")
      }
    }
    
    getInstanceRDD(pureDataRDD)
  }    

  // Pure record from the file (just split the class-name and attribute vector)
  private case class PureData(id: Int, name : String, attributeVector : Array[String])
  // Class for holding just a attribute type and the original string value
  private case class AttributeTypeValue (attributeType : DimensionTypeEnum.Value, value : String)
  
  private def getPureDataRDD(dataset : RDD[String]) = {
    dataset.map{
      record => {
        val array = record.split(',')
        val arrayWithoutId = array.drop(1)
        PureData(
            array.head.toInt,
            array.last, // class label
            Array.tabulate(attributes.size)(i => arrayWithoutId(i))
        )
      }
    }
  }
  
  private def getIndexAttributeTypeValueRDD ( pureDataRDD : RDD[PureData]) = {

    def convertToIndexAttributeTypeValueTuple(index : Int, value : String) : (Int, AttributeTypeValue)= {
      attributes(index).dimensionType match {
         case DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC =>
           (index, AttributeTypeValue(DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC, value))
         case DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL =>
           (index, AttributeTypeValue(DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL, value))
         case _ => throw new UnsupportedOperationException("Unsupported DimensionType")
       }
    }

    pureDataRDD.flatMap { 
      pureData => 
        pureData.attributeVector.zipWithIndex
                                .map(valueIndex => convertToIndexAttributeTypeValueTuple(valueIndex._2, valueIndex._1)) 
    }
  }
  
  private def getAttributeMinMaxValuesMap( inputRDD : RDD[(Int, AttributeTypeValue)]) = {
    val numericAttributesRDD = inputRDD.filter(tuple => tuple._2.attributeType == DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC)
                                       .map(tuple => (tuple._1, tuple._2.value.toDouble))
    numericAttributesRDD.persist()
    
    val maxAttributeMap = numericAttributesRDD.reduceByKey( max(_,_) ).collectAsMap()

    val minAttributeMap = numericAttributesRDD.reduceByKey( min(_,_) ).collectAsMap()
    
    numericAttributesRDD.unpersist(false)

    (minAttributeMap, maxAttributeMap)
  }
  
  private def getAttributeDomainValuesMap( inputRDD : RDD[(Int, AttributeTypeValue)]) = {
    val nominalAttributesRDD = inputRDD.filter(tuple => tuple._2.attributeType == DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL)
                                           .map(tuple => (tuple._1, Set(tuple._2.value)))
    nominalAttributesRDD.reduceByKey( _.union(_) ).collectAsMap()
  }
    
  private def getInstanceRDD( inputRDD : RDD[PureData] ) : RDD[Instance] = {

    def getDistanceHierarchyDimension(index : Int, value : String) = {
      
      attributes(index).dimensionType match {

        case DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL => {
          val distanceHierarchyElemObj = DistanceHierarchyElem(value, 0.5)  
          DistanceHierarchyDimension(attributes(index).name, distanceHierarchyElemObj, DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL)
        }

        case DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC => {
          var normalizedValue = 0.0
          val den = (attributes(index).maxValue - attributes(index).minValue)

          if (den>0.0)
            normalizedValue = (value.toDouble - attributes(index).minValue) / den

          val distanceHierarchyElemObj = DistanceHierarchyElem("+", normalizedValue)

          DistanceHierarchyDimension(attributes(index).name, distanceHierarchyElemObj, DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC)
        }

        case _ => throw new UnsupportedOperationException("Unsupported DimensionType")
      }
    }
    
    inputRDD.map{ 
      record => {
        val attributeVectorIndex = record.attributeVector.zipWithIndex  
        Data(
          record.id,
          record.name,
          attributeVectorIndex.map{ 
            valueIndex => getDistanceHierarchyDimension(valueIndex._2, valueIndex._1)
          }
        )
        .getInstanceObj
      }
    } 
  }
}

object Su {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val (appname, master, datasetPath, testPath) =
    try {
      val prop = new java.util.Properties()
      prop.load(new FileInputStream("config.properties"))
      (
        prop.getProperty("appname"),
        prop.getProperty("master"),
        prop.getProperty("datasetPath"),
        prop.getProperty("testPath")
      )
    } catch { case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
    }


  def main(args : Array[String]) {

   val logger = LogManager.getLogger(appname)

    val sc = SparkConfig.getSparkContext
    
    var epochs = GHSomConfig.epochs
    
    println("EPOCHS: " + epochs)
    println("TAU1: " + GHSomConfig.tau1)
    println("TAU2: " + GHSomConfig.tau2)
    /*
    val maxVector = Array.fill(10)(DoubleDimension.MinValue)
    val attribVector = Array.fill(10)(DoubleDimension.getRandomDimensionValue)
    println(maxVector.mkString)
    println(attribVector.mkString)

    for ( i <- 0 until attribVector.size ) { 
        maxVector(i) = if (attribVector(i) > maxVector(i)) attribVector(i) else maxVector(i)
    } 
    println(maxVector.mkString)
    */
    val dataset = sc.textFile(datasetPath)    // id + label
    var datasetReader = new Su(dataset)
    val processedDataset = datasetReader.getDataset

    val testDataset: RDD[String] = sc.textFile(testPath)
    val testDatasetDummy: RDD[String] = testDataset.map(x => x.split(",").toList.drop(1).dropRight(1).mkString(",") + ",dummy")
    val testDatasetDummyWithID: RDD[String] = testDataset.map(x => x.split(",").toList.dropRight(1).mkString(",") + ",dummy")

    val groundTruth: RDD[(Int, String)] = testDataset.map(x => {
      val row = x.split(",").toList
      (row.take(1).apply(0).toInt , row.takeRight(1).apply(0))
    })

    datasetReader = new Su(testDatasetDummyWithID)
    val processedTestDataset: RDD[Instance] = datasetReader.getDataset

    val ghsom = GHSom()
    ghsom.trainAndTest(processedDataset, processedTestDataset, groundTruth, datasetReader.attributes, epochs)
  }
}
