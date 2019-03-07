package com.sparkghsom.main.utils

import org.apache.log4j.{Level, LogManager, Logger}
import com.sparkghsom.main.globals.SparkConfig
import java.io.{File, FileInputStream, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.joda.time.LocalDate
import com.sparkghsom.main.input_generator._
import com.sparkghsom.main.globals.GHSomConfig
import com.sparkghsom.main.mr_ghsom.{GHSomParams, Instance}
import com.sparkghsom.main.globals.SparkConfig
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{DecisionTreeRegressor, IsotonicRegression, LinearRegression}

// Used for time-series data with dates splits

object RegressionRunner {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkSession: SparkSession = SparkConfig.getSparkSession
  import sparkSession.implicits._

  def main(args: Array[String]): Unit = {

/*    val datasetFile = args(0)
    val testFile = args(1)
    val datasetName = args(2)
    val dateFile = args(3)
    val splitNum = args(4)
    val dateField = args(5)
    val windowSize = args(6).toInt
    val tau1 = args(7).toDouble
    val tau2 = args(8).toDouble
    val epochs = args(9).toInt
    val targetField = args(10)
    val algorithm = args(11)
*/
    val algorithm = "LR"

    val maxIter = 10
    val regParam = 0.1
    val elasticNetParam = 0.8

    // ____________________________________
    // PV NREL
    // ____________________________________
    /*  val datasetFile = "/Users/hydergine/git/mr_ghsom/datasets/pv_nrel/pv_nrel_hourly.csv"
      val testFile = "/Users/hydergine/git/mr_ghsom/datasets/pv_nrel/pv_nrel_hourly.csv"
      val datasetName = "PV_NREL"
      val dateFile = "/Users/hydergine/git/mr_ghsom/datasets/pv_nrel/test_dates.txt"
      val dateField = "day"
    */

    // ____________________________________
    // BIKE SHARING
    // ____________________________________
   /* val datasetFile = "/mrghsom/datasets/bike_sharing/bike_sharing.csv"
    val testFile = "/mrghsom/datasets/bike_sharing/bike_sharing.csv"
    val datasetName = "BIKE_SHARING"
    val dateFile = "/mrghsom/dates/bike_sharing_1.txt"
    val dateField = "data"
    val targetField = "cnt"*/

    /* ____________________________________
    // BURLINGTON
    // ____________________________________ */
    val datasetFile = "/Users/hydergine/git/mr_ghsom/datasets/burlington/burlington_train.csv"
    val testFile = "/Users/hydergine/git/mr_ghsom/datasets/burlington/burlington_test.csv"
    val datasetName = "BURLINGTON"
    val dateFile = "/mrghsom/dates/burlington_1.txt"
    val dateField = "data"
    val targetField = "power"

    /*
    // ____________________________________
    // PV ITALY
    // ____________________________________
    val datasetFile = "/Users/hydergine/git/mr_ghsom/datasets/pv_italy/pv_italy_training.csv"
    val testFile = "/Users/hydergine/git/mr_ghsom/datasets/pv_italy/pv_italy_test.csv"
    val datasetName = "PV_ITALY"
    val dateFile = "/Users/hydergine/git/mr_ghsom/datasets/pv_italy/test_dates.txt"
    val dateField = "data"
    val splitNum = 1
    */

    // ____________________________________
    // GENERAL
    // ____________________________________
      val splitNum = 1
      val tau1 = 0.5
      val tau2 = 0.7
      val epochs = 15
      val windowSize = 60


    var executionString = ""

    if(algorithm.equals("GHSOM")) {
      executionString = datasetName + "_split_" + splitNum + "_tau1_" + tau1 + "_tau2_" + tau2 + "_epochs_" + epochs

      writeFile("/mrghsom/output/" + executionString + ".errors", "rmse,mae")
      writeFile("/mrghsom/output/" + executionString + ".predictions", "prediction,actual")

      println("EPOCHS: " + epochs)
      println("TAU1: " + tau1)
      println("TAU2: " + tau2)
    }

    else
      executionString = datasetName + "_split_" + splitNum + "_maxIter_" + maxIter + "_regParam_" + regParam + "_elasticNetParam_" + elasticNetParam

    val source = scala.io.Source.fromFile(dateFile)
    val lines = try source.mkString finally source.close()
    val testDates = lines.split(" ").filter(_!=(""," ")).toList

    val trainTblDF = selectDataFrameFromTxt(datasetFile)
    val testTblDF = selectDataFrameFromTxt(testFile)

    var errorList: Array[(Double, Double)] = Array[(Double,Double)]()

    for (date <- testDates) {

      val (trainDF, testDF, datesToTest) = {
        if (datasetName == "PV_NREL")
          getTrainTestDay(trainTblDF, testTblDF, date, windowSize, dateField)
        else
          getTrainTest(trainTblDF, testTblDF, date, windowSize, dateField)
      }

      println("Train dataset: " + trainDF.count())
      println("Test dataset: " + testDF.count())

      val stringRDDdataset: RDD[String] = trainDF.rdd.map(_.toString.replace("[","").replace("]",""))
      val stringRDDtest = testDF.rdd.map(_.toString.replace("[","").replace("]",""))
      val testDatasetDummyWithID: RDD[String] = stringRDDtest.map(x => x.split(",").toList.dropRight(1).mkString(",") + ",0.0")

      val groundTruth: DataFrame = {
        if(datasetName=="PV_NREL")
          selectGroundTruthDay(testTblDF, date, dateField)
        else
          selectGroundTruth(testTblDF, date, dateField, targetField)
      }

      val groundTruthParsed = groundTruth.map(_.toString.replace("[","").replace("]",""))

      val groundTruthRDD: RDD[(Int, String)] = groundTruthParsed.map(e => {
        val split = e.split(",")
        val id = split.apply(0).toInt
        val actual = split.apply(1)
        (id,actual)
      }).rdd

      if(algorithm.equals("GHSOM")) {

        var (processedDataset, processedTestDataset, attributes) = {
          if(datasetName.equals("PV_NREL")) {
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
          else if (datasetName.equals("BURLINGTON")) {
            var datasetReader = new Burlington(stringRDDdataset)
            val processedDataset: RDD[Instance] = datasetReader.getDataset
            datasetReader = new Burlington(testDatasetDummyWithID)
            val processedTestDataset: RDD[Instance] = datasetReader.getDataset
            (processedDataset, processedTestDataset, datasetReader.attributes)
          }
          else if (datasetName.equals("BIKE_SHARING")) {
            var datasetReader = new BikeSharing(stringRDDdataset)
            val processedDataset: RDD[Instance] = datasetReader.getDataset
            datasetReader = new BikeSharing(testDatasetDummyWithID)
            val processedTestDataset: RDD[Instance] = datasetReader.getDataset
            (processedDataset, processedTestDataset, datasetReader.attributes)
          }
          else {
            var datasetReader = new WINDNREL(stringRDDdataset)
            val processedDataset: RDD[Instance] = datasetReader.getDataset
            datasetReader = new WINDNREL(testDatasetDummyWithID)
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

        errorList :+= (rmse.toDouble,mae.toDouble)
      }
      else {

        var (processedDataset, processedTestDataset, attributes) = {
          if(datasetName.equals("PV_NREL")) {
            val features = Array("idplant","lat","lon","day","ora","temperature","pressure","windspeed","humidity","icon","dewpoint","windbearing","cloudcover","altitude","azimuth","power")
            val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
            val assembledTrain: DataFrame = assembler.transform(trainDF).select("features",targetField)
            val assembledTest: DataFrame = assembler.transform(testDF).select("features")
            (assembledTrain,assembledTest,features)
          }
          else if (datasetName.equals("PV_ITALY")) {
            val features = Array("idsito","lat","lon","day","ora","temperatura_ambiente","irradiance","pressure","windspeed","humidity","icon","dewpoint","windbearing","cloudcover","altitude","azimuth","power")
            val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
            val assembledTrain: DataFrame = assembler.transform(trainDF).select("features",targetField)
            val assembledTest: DataFrame = assembler.transform(testDF).select("features")
            (assembledTrain,assembledTest,features)
          }
          else if (datasetName.equals("BURLINGTON")) {
            val features = Array("idplant","lat","lon","day","hour","temperature","irradiance","pressure","windspeed","humidity","icon","dewpoint","windbearing","cloudcover","precipitationintensity","precipitationprobability","precipitationaccumulation")
            val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
            val assembledTrain: DataFrame = assembler.transform(trainDF).select("features",targetField)
            val assembledTest: DataFrame = assembler.transform(testDF).select("features",targetField).withColumnRenamed(targetField,"label")
            (assembledTrain,assembledTest,features)
          }
          else {    // {BIKE_SHARING}
            val features = Array("season","month","hour","holiday","weekday","workingday","weathersit","temp","atemp","hum","windspeed","casual","registered")
            val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
            val assembledTrain: DataFrame = assembler.transform(trainDF).select("features",targetField)
            val assembledTest: DataFrame = assembler.transform(testDF).select("features")
            (assembledTrain,assembledTest,features)
          }
        }

        execIsotonicRegression(processedDataset, processedTestDataset, groundTruthRDD, targetField)

        //execLinearRegression(processedDataset, processedTestDataset, groundTruthRDD, targetField, maxIter, regParam, elasticNetParam)

        //execRegressionTree(processedDataset, processedTestDataset, groundTruthRDD, targetField)
      }


/*      println("predictions")
      predictions.take(3).foreach(println)
      println()
*/

    }

    val rmseAVG = errorList.map(e => e._1).sum/errorList.size
    val maeAVG = errorList.map(e => e._2).sum/errorList.size

    println()
    println("Average error on dates: RMSE " + rmseAVG + " MAE " + maeAVG)

    writeFile("/mrghsom/output/" + executionString + ".average", rmseAVG.toString + "," + maeAVG.toString)
  }
  //************************************************************************************************
  def execRegressionTree(processedDataset: DataFrame,
                         processedTestDataset: DataFrame,
                         groundTruthRDD: RDD[(Int, String)],
                         targetAttribute: String): Unit = {

    val trainDataset = processedDataset.withColumnRenamed(targetAttribute, "label")

    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val model = dt.fit(trainDataset)

    val predictions = model.transform(processedTestDataset)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)


  }
  //************************************************************************************************
  def execIsotonicRegression(processedDataset: DataFrame,
                             processedTestDataset: DataFrame,
                             groundTruthRDD: RDD[(Int, String)],
                             targetAttribute: String) = {


    val trainDataset = processedDataset.withColumnRenamed(targetAttribute, "label")

    trainDataset.take(3).foreach(println)

    val model = new IsotonicRegression().fit(trainDataset)

    val testDataset = processedTestDataset.select("features")

    println("test")
    testDataset.take(3).foreach(println)
    println()

    val prediction: DataFrame = model.transform(testDataset)

    prediction.collect.foreach(println)

  }
  //************************************************************************************************
  def execLinearRegression(processedDataset: DataFrame,
                           processedTestDataset: DataFrame,
                           groundTruthRDD: RDD[(Int, String)],
                           targetAttribute: String,
                           maxIter: Int,
                           regParam: Double,
                           elasticNetParam: Double) = {

    val lr = new LinearRegression()
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setFitIntercept(true)

    val trainDataset = processedDataset.withColumnRenamed(targetAttribute, "label")

    trainDataset.show(3)

    // Fit the model
    val lrModel = lr.setFitIntercept(true)
      .setFeaturesCol("features")
      .fit(trainDataset)

    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"”RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val prediction = lrModel
      .setFeaturesCol("features")
      .setPredictionCol(targetAttribute)
      .transform(processedTestDataset)

    prediction.collect.foreach(println)
  }
  //************************************************************************************************
  def truncateAt(n: Double, p: Int): Double = { val s = math pow (10, p); (math floor n * s) / s }
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
  def writeFile(fileName: String, content: String): Unit = {
    val fw = new FileWriter(fileName, true)
    try {
      fw.write(content + "\r\n")
    }
    fw.close()
  }

}
