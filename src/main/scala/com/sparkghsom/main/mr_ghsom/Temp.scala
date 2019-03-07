package com.sparkghsom.main.mr_ghsom

import com.sparkghsom.main.globals.SparkConfig
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Dataset}


object Temp {


  def main(args: Array[String]): Unit = {

    // Pipeline: DataFrame -> Categorical Features Treatment: (String Indexer -> One Hot Encoding) -> Algorithm -> Prediction + Error

    val session = SparkConfig.getSparkSession
    import session.implicits._

    val trainingFile = "/Users/hydergine/git/mr_ghsom/datasets/automotive/imports-85.csv"

    val datasetDF = selectDataFrameFromTxt(trainingFile)    // id + label

    val features = Array("symboling","normalized-losses","make","fuel-type","aspiration","num-of-doors","body-style","drive-wheels","engine-location",
      "wheel-base","length","width","height","curb-weight","engine-type","num-of-cylinders","engine-size","fuel-system","bore","stroke","compression-ratio",
      "horsepower","peak-rpm","city-mpg","highway-mpg","price")

    // differently from the classification module, the class here is not considered for one hot encoding

    val categoricalCols = Array("make","fuel-type","aspiration","num-of-doors","body-style","drive-wheels","engine-location",
      "engine-type","num-of-cylinders","fuel-system")

    val index_transformers: Array[org.apache.spark.ml.PipelineStage] = categoricalCols.map(
      cname => new StringIndexer()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_i")
        .setHandleInvalid("skip")
    )

    val index_pipeline = new Pipeline().setStages(index_transformers)
    val index_model = index_pipeline.fit(datasetDF)
    val df_indexed = index_model.transform(datasetDF)

    df_indexed.drop(categoricalCols:_*).select("symboling","normalized-losses","make_i","fuel-type_i","aspiration_i","num-of-doors_i","body-style_i","drive-wheels_i","engine-location_i",
      "wheel-base","length","width","height","curb-weight","engine-type_i","num-of-cylinders_i","engine-size","fuel-system_i","bore","stroke","compression-ratio",
      "horsepower","peak-rpm","city-mpg","highway-mpg","price").rdd.saveAsTextFile("testSaved")

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
}
