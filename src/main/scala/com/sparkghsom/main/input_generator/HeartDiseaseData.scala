package com.sparkghsom.main.input_generator

import com.sparkghsom.main.mr_ghsom.Instance
import com.sparkghsom.main.datatypes.{DimensionTypeEnum,DistanceHierarchyDimension, DistanceHierarchyElem}
import com.sparkghsom.main.datatypes.DimensionType
import com.sparkghsom.main.mr_ghsom.GHSom
import com.sparkghsom.main.globals.GHSomConfig
import com.sparkghsom.main.mr_ghsom.Attribute
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.LogManager
import org.apache.commons
import org.apache.commons.io.FileUtils
import java.io.File
import scala.math.{max,min}

class HeartDiseaseDataset (val dataset : RDD[String]) extends Serializable{
  
  val attributes = Array(
                        Attribute(name = "age", 
                                  index = 0, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "sex", 
                                  index = 1, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "cp", 
                                  index = 2, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "trestbps", 
                                  index = 3, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "chol", 
                                  index = 4, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "fbs", 
                                  index = 5, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "restecg", 
                                  index = 6, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "thalach", 
                                  index = 7, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "exang", 
                                  index = 8, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "oldpeak", 
                                  index = 9, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "slope", 
                                  index = 10, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "ca", 
                                  index = 11, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "thal", 
                                  index = 12, 
                                  dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                                  randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue)
                   )
  
    private val datasetOfInstanceObjs = instanizeDataset(dataset)
  
  case class Data ( className : String, attributeVector : Array[DimensionType] ) {
    def getInstanceObj = {
      Instance(0,className, attributeVector)
    }
  }
  
  def printDataset() {
    // println(datasetRDD.count)
  }
  
  def getDataset : RDD[Instance] = datasetOfInstanceObjs
  
  private def instanizeDataset(dataset : RDD[String]) : RDD[Instance] = {

    // RDD[PureData] i.e. RDD[class-name, <attribute-vector>]
    val pureDataRDD = getPureDataRDD(dataset) 

    // RDD[PureData] => RDD[(index, AttributeTypeValue[AttributeType, Value])]
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
  private case class PureData(name : String, attributeVector : Array[String])
  // Class for holding just a attribute type and the original string value
  private case class AttributeTypeValue (attributeType : DimensionTypeEnum.Value, value : String)
  
  private def getPureDataRDD(dataset : RDD[String]) = {
    dataset.map{
      record => {
        val array = record.split(',')
        PureData(
            array(array.length - 1),
            Array.tabulate(attributes.size)(i => array(i))
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
          val normalizedValue = (value.toDouble - attributes(index).minValue) / (attributes(index).maxValue - attributes(index).minValue)
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

object HeartDiseaseDataset {
  
  val logger = LogManager.getLogger("Heart Disease")
  
  def main(args : Array[String]) {
    val conf = new SparkConf(true)
               .setAppName("HeartDisease")
               //.set("spark.storage.memoryFraction","0")
               .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               .set("spark.default.parallelism","8")
               .set("spark.kryoserializer.buffer.max","400")
               .setMaster("spark://192.168.101.13:7077")

    val sc = new SparkContext(conf) 
    
    var epochs = GHSomConfig.epochs
    if(args.length >= 1) {
      epochs = args(0).toInt  
    }
    
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
    val dataset = sc.textFile("hdfs://192.168.101.13:9000/user/ameya/datasets/heartdisease/heartdisease.data")
    val datasetReader = new HeartDiseaseDataset(dataset) 
    datasetReader.printDataset()
    val processedDataset = datasetReader.getDataset
    
    val ghsom = GHSom()
    
    ghsom.train(processedDataset, datasetReader.attributes, epochs)
  }
}
