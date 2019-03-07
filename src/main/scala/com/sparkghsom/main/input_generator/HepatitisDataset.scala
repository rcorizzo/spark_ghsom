package com.sparkghsom.main.input_generator

import com.sparkghsom.main.mr_ghsom.Instance
import com.sparkghsom.main.datatypes.{DimensionTypeEnum, DistanceHierarchyDimension, DistanceHierarchyElem}
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
import java.io.{File, FileInputStream}

import scala.math.{max, min}

class HepatitisDataset (val dataset : RDD[String]) extends Serializable{
  
  val attributes = Array(
                        Attribute(name = "Age", 
                            index = 0, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "sex", 
                            index = 1, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "steroid", 
                            index = 2, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "antivirals", 
                            index = 3, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "fatigue", 
                            index = 4, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "malaise", 
                            index = 5, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "anorexia", 
                            index = 6, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "liver_big", 
                            index = 7, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "liver_firm", 
                            index = 8, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "spleen_palpable", 
                            index = 9, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "spiders", 
                            index = 10, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "ascites", 
                            index = 11, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "varices", 
                            index = 12, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "bilirubin", 
                            index = 13, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "alk_phosphate", 
                            index = 14, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "sgot", 
                            index = 15, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "albumin", 
                            index = 16, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "protime", 
                            index = 17, 
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NUMERIC,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "histology",
                            index = 18,
                            dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                            randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue),
                        Attribute(name = "class",
                          index = 19,
                          dimensionType = DimensionTypeEnum.DISTANCE_HIERARCHY_NOMINAL,
                          randomValueFunction = DistanceHierarchyDimension.getRandomDimensionValue)
                   )
  
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

    // RDD[PureData] i.e. RDD[class-name, <attribute-vector>]
    val pureDataRDD: RDD[PureData] = getPureDataRDD(dataset)

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

object HepatitisDataset {

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
}