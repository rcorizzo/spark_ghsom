package com.sparkghsom.main.utils

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Date
import com.sparkghsom.main.input_generator.{PVNRELMT,PVITALYMT,BurlingtonMT}
import com.sparkghsom.main.globals.SparkConfig
import com.sparkghsom.main.input_generator._
import com.sparkghsom.main.mr_ghsom.{GHSomParamsMT, Instance,InstanceMT}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDate

object SensorForecastingMT {

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
    val numTargets = args(5).toInt


/*
    val path = "/Users/hydergine/git/mr_ghsom/"
    val datasetName = "PV_NREL"
    val tau1 = 0.3
    val tau2 = 0.7
    val epochs = 10
    val numTargets = 19
*/
    execute(path, datasetName, tau1, tau2, epochs, numTargets)

  }

  def execute(path: String,
              datasetName: String,
              tau1: Double,
              tau2: Double,
              epochs: Int,
              numTargets: Int): Unit = {

    val (datasetFile,testFile,dateFile,dateField,targetField) = {
      if(datasetName.equals("PV_NREL")) {
        (path + "datasets/pv_nrel_daily/pv_nrel-head.csv",
          path + "datasets/pv_nrel_daily/pv_nrel-head.csv",
          path + "dates/pv_nrel_1.txt",
          "day",
          "power2, power3, power4, power5, power6, power7, power8, power9, power10, power11, power12, power13, power14, power15, power16, power17, power18, power19, power20"
        )
      }
      else if (datasetName.equals("PV_ITALY")) {
        (path + "datasets/pv_italy_daily/pv_italy_train-head.csv",
          path + "datasets/pv_italy_daily/pv_italy_test-head.csv",
          path + "dates/pv_italy_1.txt",
          "data",
          "kwh2, kwh3, kwh4, kwh5, kwh6, kwh7, kwh8, kwh9, kwh10, kwh11, kwh12, kwh13, kwh14, kwh15, kwh16, kwh17, kwh18, kwh19, kwh20")
      }
      else if (datasetName.equals("BURLINGTON")) {
        (path + "datasets/burlington_daily/burlington_daily_train-head.csv",
          path + "datasets/burlington_daily/burlington_daily_test-head.csv",
          path + "dates/burlington_1.txt",
          "data",
          "mtr_ac_power_delv_avg0,mtr_ac_power_delv_avg1,mtr_ac_power_delv_avg2,mtr_ac_power_delv_avg3,mtr_ac_power_delv_avg4,mtr_ac_power_delv_avg5,mtr_ac_power_delv_avg6,mtr_ac_power_delv_avg7,mtr_ac_power_delv_avg8,mtr_ac_power_delv_avg9,mtr_ac_power_delv_avg10,mtr_ac_power_delv_avg11,mtr_ac_power_delv_avg12,mtr_ac_power_delv_avg13,mtr_ac_power_delv_avg14,mtr_ac_power_delv_avg15,mtr_ac_power_delv_avg16,mtr_ac_power_delv_avg17,mtr_ac_power_delv_avg18,mtr_ac_power_delv_avg19,mtr_ac_power_delv_avg20,mtr_ac_power_delv_avg21,mtr_ac_power_delv_avg22,mtr_ac_power_delv_avg23")
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
          selectGroundTruthMT(testTblDF, date, dateField, targetField.split(","))
      }

      val groundTruthParsed = groundTruth.map(_.toString.replace("[", "").replace("]", ""))

      //println("ground truth")
      //groundTruthParsed.take(5).foreach(e => println(e.toString))


      val groundTruthRDD: RDD[(Int, Array[String])] = groundTruthParsed.map(e => {
        val split = e.split(",")
        val id = split.apply(0).toInt
        val actual = split.takeRight(numTargets)
        (id, actual)
      }).rdd

      var (processedDataset, processedTestDataset, attributes) = {
        if (datasetName.equals("PV_NREL")) {
          val numTargets = 19
          var datasetReader = new PVNRELMT(stringRDDdataset,numTargets)
          val processedDataset: RDD[InstanceMT] = datasetReader.getDataset
          datasetReader = new PVNRELMT(testDatasetDummyWithID,numTargets)
          val processedTestDataset: RDD[InstanceMT] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else if (datasetName.equals("PV_ITALY")) {
          val numTargets = 19
          var datasetReader = new PVITALYMT(stringRDDdataset,numTargets)
          val processedDataset: RDD[InstanceMT] = datasetReader.getDataset
          datasetReader = new PVITALYMT(testDatasetDummyWithID,numTargets)
          val processedTestDataset: RDD[InstanceMT] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else // (datasetName.equals("BURLINGTON"))
         {
          val numTargets = 24
          var datasetReader = new BurlingtonMT(stringRDDdataset,numTargets)
          val processedDataset = datasetReader.getDataset
          datasetReader = new BurlingtonMT(testDatasetDummyWithID,numTargets)
          val processedTestDataset: RDD[InstanceMT] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
       }
      }

      val ghsom = GHSomParamsMT()

      val (predictions, rmse, mae) =
        ghsom.trainAndTestRegression(processedDataset, processedTestDataset, groundTruthRDD, attributes, tau1, tau2, epochs)

      predictions.foreach(p => {
        writeFile("output/" + executionString + ".predictions", p._1.toString + "," + p._2.toString)
      })

      writeFile("output/" + executionString + ".errors", rmse.toString + "," + mae.toString)

      println("Daily RMSE: " + rmse)
      println("Daily MAE: " + mae)

      errorList :+= (rmse.toDouble, mae.toDouble)
    }

    val rmseAVG = errorList.map(e => e._1).sum/errorList.size
    val maeAVG = errorList.map(e => e._2).sum/errorList.size

    println("RMSE average: " + rmseAVG)
    println("MAE average: " + maeAVG)

    writeFile("output/" + executionString + ".average", rmseAVG.toString + "," + maeAVG.toString)

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
  def selectGroundTruthMT(testTblDF: DataFrame, currentDate: String, dateField: String, targetField: Array[String]): DataFrame = {

    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = formatter.parse(currentDate)

    println("Current date " + currentDate)

    val testDate = LocalDate.fromDateFields(date)
    val testDF = testTblDF.filter(dateField + " LIKE '" + testDate.toString() + "%'").sort(dateField)

    val completeFeatureSet = "id" +: targetField

    val groundTruth: DataFrame = testDF.selectExpr(completeFeatureSet:_*)

    (groundTruth)
  }
  //************************************************************************************************
  def selectGroundTruthDay(testTblDF: DataFrame, currentDate: String, dateField: String): DataFrame = {

    println("Current date " + currentDate)
    val testDF = testTblDF.filter(dateField + "=" + currentDate).sort(dateField)
    val cols = Array("id","power2","power3","power4","power5","power6","power7","power8","power9","power10","power11","power12","power13","power14","power15","power16","power17","power18","power19","power20")
    val groundTruth: DataFrame = testDF.selectExpr(cols:_*)

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
  def writeFile(fileName: String, content: String): Unit = {
    val fw = new FileWriter(fileName, true)
    try {
      fw.write(content + "\r\n")
    }
    fw.close()
  }
}
