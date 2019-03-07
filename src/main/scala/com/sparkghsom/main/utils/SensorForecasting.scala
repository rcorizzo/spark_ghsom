package com.sparkghsom.main.utils

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Date

import com.sparkghsom.main.globals.SparkConfig
import com.sparkghsom.main.input_generator._
import com.sparkghsom.main.mr_ghsom.{GHSomParams, Instance}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDate

object SensorForecasting {

  val sqlC = SparkConfig.getSqlContext
  import sqlC.implicits._

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val path = args(0)
    val datasetName = args(1)
    val tau1 = args(2).toDouble
    val tau2 = args(3).toDouble
    val epochs = args(4).toInt

    execute(path, datasetName, tau1, tau2, epochs)

  }

  def execute(path: String,
              datasetName: String,
              tau1: Double,
              tau2: Double,
              epochs: Int): Unit = {

    val (datasetFile,testFile,dateFile,dateField,targetField) = {
      if(datasetName.equals("PV_NREL")) {
        (path + "datasets/pv_nrel/pv_nrel_hourly.csv",
          path + "datasets/pv_nrel/pv_nrel_hourly.csv",
          path + "datasets/pv_nrel/test_dates.txt",
          "day",
          "power"
        )
      }
      else if (datasetName.equals("PV_ITALY")) {
        (path + "datasets/pv_italy/pv_italy_training.csv",
          path + "datasets/pv_italy/pv_italy_test.csv",
          path + "datasets/pv_italy/test_dates.txt",
          "data",
          "power")
      }
      else if (datasetName.equals("BURLINGTON")) {
        (path + "datasets/burlington/burlington_train.csv",
          path + "datasets/burlington/burlington_test.csv",
          path + "dates/burlington_1.txt",
          "data",
          "power")
      }
      else
        ("","","","","")
    }

    val windowSize = 60
    var executionString = ""
    executionString = datasetName + "_tau1_" + tau1 + "_tau2_" + tau2 + "_epochs_" + epochs

    writeFile("/mrghsom/output/" + executionString + ".errors", "rmse,mae")
    writeFile("/mrghsom/output/" + executionString + ".predictions", "prediction,actual")

    println("EPOCHS: " + epochs)
    println("TAU1: " + tau1)
    println("TAU2: " + tau2)

    val source = scala.io.Source.fromFile(dateFile)
    val lines = try source.mkString finally source.close()
    val testDates = lines.split(" ").filter(_ != ("", " ")).toList

    val trainTblDF = selectDataFrameFromTxt(datasetFile)
    val testTblDF = selectDataFrameFromTxt(testFile)

    var errorList: Array[(Double, Double)] = Array[(Double, Double)]()

    for (date <- testDates) {

      val (trainDF, testDF, datesToTest) = {
        if (datasetName == "PV_NREL")
          getTrainTestDay(trainTblDF, testTblDF, date, windowSize, dateField)
        else
          getTrainTest(trainTblDF, testTblDF, date, windowSize, dateField)
      }

      println("Train dataset: " + trainDF.count())
      println("Test dataset: " + testDF.count())

      val stringRDDdataset: RDD[String] = trainDF.rdd.map(_.toString.replace("[", "").replace("]", ""))
      val stringRDDtest = testDF.rdd.map(_.toString.replace("[", "").replace("]", ""))
      val testDatasetDummyWithID: RDD[String] = stringRDDtest.map(x => x.split(",").toList.dropRight(1).mkString(",") + ",0.0")

      val sqlC = SparkConfig.getSqlContext
      import sqlC.implicits._

      val groundTruth: DataFrame = {
        if (datasetName == "PV_NREL")
          selectGroundTruthDay(testTblDF, date, dateField)
        else
          selectGroundTruth(testTblDF, date, dateField, targetField)
      }

      val groundTruthParsed = groundTruth.map(_.toString.replace("[", "").replace("]", ""))

      val groundTruthRDD: RDD[(Int, String)] = groundTruthParsed.map(e => {
        val split = e.split(",")
        val id = split.apply(0).toInt
        val actual = split.apply(1)
        (id, actual)
      }).rdd

      var (processedDataset, processedTestDataset, attributes) = {
        if (datasetName.equals("PV_NREL")) {
          var datasetReader = new PVNREL(stringRDDdataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new PVNREL(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else if (datasetName.equals("PV_ITALY")) {
          var datasetReader = new PVITALY(stringRDDdataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new PVITALY(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else // (datasetName.equals("BURLINGTON"))
         {
          var datasetReader = new Burlington(stringRDDdataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new Burlington(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
         }
      }

      val ghsom = GHSomParams()

      val (predictions, rmse, mae) =
        ghsom.trainAndTestRegression(processedDataset, processedTestDataset, groundTruthRDD, attributes, tau1, tau2, epochs)

      predictions.foreach(p => {
        writeFile("/mrghsom/output/" + executionString + ".predictions", p._1.toString + "," + p._2.toString)
      })

      writeFile("/mrghsom/output/" + executionString + ".errors", rmse.toString + "," + mae.toString)

      println("Daily RMSE: " + rmse)
      println("Daily MAE: " + mae)

      errorList :+= (rmse.toDouble, mae.toDouble)
    }
  }
    //************************************************************************************************
    def writeFile(fileName: String, content: String): Unit = {
      val fw = new FileWriter(fileName, true)
      try {
        fw.write(content + "\r\n")
      }
      fw.close()
    }
  //************************************************************************************************
  def selectGroundTruth(testTblDF: DataFrame, currentDate: String, dateField: String, targetField: String): DataFrame = {

    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = formatter.parse(currentDate)

    println("Current date " + currentDate)

    val testDate = LocalDate.fromDateFields(date)
    val testDF = testTblDF.filter(dateField + " LIKE '" + testDate.toString() + "%'").sort(dateField)

    val groundTruth: DataFrame = testDF.selectExpr("id",targetField)

    (groundTruth)
  }
  //************************************************************************************************
  def selectGroundTruthDay(testTblDF: DataFrame, currentDate: String, dateField: String): DataFrame = {

    println("Current date " + currentDate)
    val testDF = testTblDF.filter(dateField + "=" + currentDate).sort(dateField)
    val groundTruth: DataFrame = testDF.selectExpr("id","power")

    (groundTruth)
  }
  //************************************************************************************************
  def selectDataFrameFromTxt(fileName: String): DataFrame = {

    val df: DataFrame = SparkConfig.getSqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")       // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter",",")
      .load(fileName)

    df
  }
  //************************************************************************************************
  def getTrainTestDay(trainTblDF: DataFrame, testTblDF: DataFrame, currentDate: String, windowSize: Int, dateField: String) = {

    // denormalize day
    val doubleDay = currentDate.toDouble
    val denormalized = ((doubleDay * (366-1)) + 1).toInt
    var startPoint = denormalized - windowSize

    if(startPoint<0)
      startPoint=1

    val prevDay = doubleDay - 0.01

    val normStartPoint = (((startPoint-1).toDouble)/(366-1)).toDouble
    val truncatedStartPoint = truncateAt(normStartPoint, 3)

    println("Query between " + truncatedStartPoint + " and " + prevDay.toString)
    println("Test day: " + currentDate)

    val trainDF = trainTblDF.filter(trainTblDF.col(dateField)
      .between(truncatedStartPoint, prevDay.toString))
      .drop("data").drop("anno").cache()

    val testDF = testTblDF.filter(dateField + "=" + currentDate)

    //println("Test rows: " + testDF.count())

    val testDates = testDF.selectExpr("id",dateField)
    val testClean = testDF.drop("data").drop("anno")

    val testDatesCollected = testDates.map(x => (x.apply(0).asInstanceOf[Number].longValue(), x.apply(1).toString)).collect().toMap
    (trainDF, testClean, testDatesCollected)
  }
  //************************************************************************************************
  def getTrainTest(trainTblDF: DataFrame, testTblDF: DataFrame, currentDate: String, windowSize: Int, dateField: String) = {

    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = formatter.parse(currentDate)

    println("Current date " + currentDate)

    val testDate = LocalDate.fromDateFields(date)
    val trainEndDate = testDate.minusDays(1)
    val trainStartDate = trainEndDate.minusDays(windowSize)

    println("Query between " + trainStartDate + " and " + trainEndDate)
    println("Test day: " + testDate)

    val trainDF = trainTblDF.filter(trainTblDF.col(dateField)
      .between(trainStartDate.toString(), trainEndDate.toString()))
      .drop("data").drop("anno").cache()

    val testDF = testTblDF.filter(dateField + " LIKE '" + testDate.toString() + "%'").sort(dateField)

    val testDates: DataFrame = testDF.selectExpr("id",dateField)
    val testClean: DataFrame = testDF.drop("data").drop("anno")

    val testDatesCollected = testDates.map(x => (x.apply(0).toString.toLong, x.apply(1).toString)).collect().toMap
    (trainDF, testClean, testDatesCollected)
  }
  //************************************************************************************************
  def truncateAt(n: Double, p: Int): Double = { val s = math pow (10, p); (math floor n * s) / s }
  //************************************************************************************************

}
