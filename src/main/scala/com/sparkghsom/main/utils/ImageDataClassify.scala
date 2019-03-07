package com.sparkghsom.main.utils

import org.apache.log4j.{Level, LogManager, Logger}
import java.io.{File, FileInputStream, FileWriter, PrintWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.sparkghsom.main.input_generator._
import com.sparkghsom.main.mr_ghsom.{GHSomParams, Instance}
import com.sparkghsom.main.globals.SparkConfig

// Used for time-series data with dates splits

object ImageDataClassify {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkSession: SparkSession = SparkConfig.getSparkSession
  import sparkSession.implicits._

  def main(args: Array[String]): Unit = {

    val datasetFile = args(0)
    val testFile = args(1)
    val datasetName = args(2)
    val tau1 = args(3).toDouble
    val tau2 = args(4).toDouble
    val epochs = args(5).toInt
    val targetField = args(6)
    val path = args(7)

    /*
    val path = "/Users/hydergine/git/spark_ghsom_src/"
    val datasetFile = "/Users/hydergine/git/spark_ghsom_src/datasets/imagedata_reduced_nohead.csv"
    val testFile = "/Users/hydergine/git/spark_ghsom_src/datasets/imagedata_reduced_nohead.csv"
    val datasetName = "IMAGEDATA"
    val targetField = "class"

    val tau1 = 0.95
    val tau2 = 0.95
    val epochs = 5
    */

    var executionString = ""

    executionString = datasetName + "_tau1_" + tau1 + "_tau2_" + tau2 + "_epochs_" + epochs

    writeFile(path + executionString + ".errors", "rmse,mae")
    writeFile(path + executionString + ".predictions", "prediction,actual")

    println("EPOCHS: " + epochs)
    println("TAU1: " + tau1)
    println("TAU2: " + tau2)

    //val trainTblDF = selectDataFrameFromTxt(datasetFile)
    //val testTblDF = selectDataFrameFromTxt(testFile)

    val trainTblDF: RDD[String] = sparkSession.sparkContext.textFile(datasetFile, 3)    // id + label
    var errorList: Array[(Double, Double)] = Array[(Double,Double)]()

    println("Train dataset: " + trainTblDF.count())
    //println("Test dataset: " + testTblDF.count())

     val stringRDDdataset: RDD[String] = trainTblDF.map(_.toString.replace("[","").replace("]",""))
     val testDatasetDummyWithID: RDD[String] = stringRDDdataset.map(x => x.split(",").toList.dropRight(1).mkString(",") + ",0.0")

    val groundTruthRDD: RDD[(Int, String)] = stringRDDdataset.map(x => {
       val vec = x.split(",")
       val id = vec.apply(0).toInt
       val target = vec.takeRight(1).apply(0)
       (id,target)
     })

   // val stringRDDdataset: RDD[String] = trainTblDF.rdd.map(_.toString.replace("[","").replace("]",""))
   // val stringRDDtest = testTblDF.rdd.map(_.toString.replace("[","").replace("]",""))
   // val testDatasetDummyWithID: RDD[String] = stringRDDtest.map(x => x.split(",").toList.dropRight(1).mkString(",") + ",0.0")

   // val groundTruth: DataFrame =  selectGroundTruth(testTblDF, targetField)
   // val groundTruthParsed = groundTruth.map(_.toString.replace("[","").replace("]",""))
/*
    val groundTruthRDD: RDD[(Int, String)] = groundTruthParsed.map(e => {
      val split = e.split(",")
      val id = split.apply(0).toInt
      val actual = split.apply(1)
      (id,actual)
    }).rdd */

    var (processedDataset, processedTestDataset, attributes) = {
      //if(datasetName.equals("ALON")) {
        var datasetReader = new ImageData(stringRDDdataset)
        val processedDataset: RDD[Instance] = datasetReader.getDataset
        datasetReader = new ImageData(testDatasetDummyWithID)
        val processedTestDataset: RDD[Instance] = datasetReader.getDataset
        (processedDataset, processedTestDataset, datasetReader.attributes)
      //}
    }

    val ghsom = GHSomParams()

    val (predictions, rmse, mae) =
      ghsom.trainAndTestRegression(processedDataset, processedTestDataset, groundTruthRDD, attributes, tau1, tau2, epochs)

    val classPreds = predictions.map {
      case (prediction, actual) => {
        if (prediction > 0.5)
          (1.0, actual)
        else
          (0.0, actual)
      }
    }

    classPreds.foreach(x => println(x))

    classPreds.foreach(p => {
      writeFile("output/" + executionString + ".predictions", p._1.toString + "," + p._2.toString)
    })

    //writeFile("output/" + executionString + ".errors", rmse.toString + "," + mae.toString)

    //println("Daily RMSE: " + rmse)
    //println("Daily MAE: " + mae)

    //errorList :+= (rmse.toDouble,mae.toDouble)

    //val rmseAVG = errorList.map(e => e._1).sum/errorList.size
    //val maeAVG = errorList.map(e => e._2).sum/errorList.size

    //println()
    //println("Average error on dates: RMSE " + rmseAVG + " MAE " + maeAVG)

    //writeFile("output/" + executionString + ".average", rmseAVG.toString + "," + maeAVG.toString)
  }
  //************************************************************************************************
  def truncateAt(n: Double, p: Int): Double = { val s = math pow (10, p); (math floor n * s) / s }
  //************************************************************************************************
  def selectGroundTruth(df: DataFrame, targetField: String): DataFrame = {

    val groundTruth: DataFrame = df.selectExpr("id",targetField)
    (groundTruth)
  }
  //************************************************************************************************
  def selectDataFrameFromTxt(fileName: String): DataFrame = {

    val df: DataFrame = SparkConfig.getSqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")       // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter",",")
      .option("maxColumns","60000")
      .load(fileName)

    df
  }
  //************************************************************************************************
  def writeFile(fileName: String, content: String): Unit = {
    val fw = new FileWriter(fileName, true)
    try {
      fw.write(content + "\r\n")
    }
    fw.close()
  }

}