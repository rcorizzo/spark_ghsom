package com.sparkghsom.main.utils

import com.sparkghsom.main.globals.SparkConfig
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.log4j.{Level, LogManager, Logger}

object NBExample {

  val sparkSession: SparkSession = SparkSession.builder
    .master("local[2]")
    .appName("test")
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  import sparkSession.implicits._

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  def stringToDouble(input: DataFrame): Unit = {
    val analysisData = input.withColumn("Event", input("Event").cast(org.apache.spark.sql.types.DoubleType))
    analysisData
  }

  def main(args: Array[String]): Unit = {

    // definire a mano le feature categoriche del dataset in categoricalCols
    // training set: datasetDF (di tipo dataframe)
    // test set: testDF (di tipo dataframe)

    val trainingFile = "datasets/bank.csv"
    val testFile = "datasets/bank.csv"

    //val datasetDF = selectDataFrameFromTxt(trainingFile)    // id + label

    val datasetDF: DataFrame = SparkConfig.getSqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")       // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", ";")
      .load(trainingFile)

    //val testDF = selectDataFrameFromTxt(testFile)           // id + label

    val testDF: DataFrame = SparkConfig.getSqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")       // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", ";")
      .load(testFile)

    val features = Array("age", "job", "balance", "day", "duration", "campaign", "pdays", "previous")
    val categoricalCols = Array("job", "marital", "housing", "education", "default", "month", "duration", "pdays", "previous", "loan", "contact", "poutcome", "y")

    val index_transformers: Array[org.apache.spark.ml.PipelineStage] = categoricalCols.map(
      cname => new StringIndexer()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_i")
        .setHandleInvalid("skip")
    )

    val index_pipeline = new Pipeline().setStages(index_transformers)
    val index_model = index_pipeline.fit(datasetDF)
    val df_indexed = index_model.transform(datasetDF)
    val df_indexed_test = index_model.transform(testDF)

    df_indexed.printSchema()

    val indexColumns  = df_indexed.columns.filter(x => ((x endsWith "_i") && (!x.equals("y_i"))))

    val one_hot_encoders: Array[org.apache.spark.ml.PipelineStage] = indexColumns.map(
      cname => new OneHotEncoder()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_vec")
    )

    val pipeline = new Pipeline()
      .setStages(index_transformers ++ one_hot_encoders)

    val one_hot_pipeline = new Pipeline().setStages(one_hot_encoders)

    val fittedOneHot = one_hot_pipeline.fit(df_indexed)

    val finalTrainDF = fittedOneHot.transform(df_indexed).drop(categoricalCols:_*)
    val finalTestDF = fittedOneHot.transform(df_indexed_test).drop(categoricalCols:_*)

    var newFeatureSet = Array.empty[String]
    val numericalFeatures = features.diff(categoricalCols)

    println("numericalFeatures")
    println(numericalFeatures.mkString(","))

    val exceptions = Array("y")
    val categorical_vectorized = categoricalCols.diff(exceptions).map(s => s + "_i_vec")

    println("categoricalVectorized")
    println(categorical_vectorized.mkString(","))

    newFeatureSet = numericalFeatures.union(categorical_vectorized)

    println("Feature set: " + newFeatureSet.mkString(","))

    finalTrainDF.printSchema()

    val assembler = new VectorAssembler().setInputCols(newFeatureSet).setOutputCol("features")
    val assembledTrain: DataFrame = assembler.transform(finalTrainDF).select("features","y_i")
    val assembledTest: DataFrame = assembler.transform(finalTestDF).select("id","features")

    // (testRowID,indexedLabel)
    val groundTruth: Dataset[(Int, Double)] = finalTestDF.select("id","y_i").map(e => (e.apply(0).toString.toInt,e.apply(1).toString.toDouble))

    val dt = new DecisionTreeClassifier()
      .setLabelCol("y_i")
      .setFeaturesCol("features")

    val trainedDT: DecisionTreeClassificationModel = dt.fit(assembledTrain)

    val preds: RDD[(Int, Double)] = trainedDT.transform(assembledTest).select("id","prediction").map(row => {
      val splitter = row.toString().replace("[","").replace("]","").split(",")
      val id = splitter.apply(0).toInt
      val pred = splitter.apply(1).toDouble
      (id,pred)
    }).rdd

    val actualPred: RDD[(Double, Double)] = groundTruth.rdd.join(preds).map({
      case (id,actual_pred) => actual_pred
    })

    println("Prediction count: " + actualPred.count())

    val metrics_multi = new MulticlassMetrics(actualPred)
    println(s"Weighted precision: ${metrics_multi.weightedPrecision}")
    println(s"Weighted recall: ${metrics_multi.weightedRecall}")
    println(s"Weighted F1 score: ${metrics_multi.weightedFMeasure}")

    val wPrec = metrics_multi.weightedPrecision
    val wRec = metrics_multi.weightedRecall
    val wFM = metrics_multi.weightedFMeasure

  }
  // ************************************************************************************
  def selectDataFrameFromTxt(fileName: String): DataFrame = {

    val df: DataFrame = SparkConfig.getSqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")       // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(fileName)

    df
  }
  // ************************************************************************************


}
