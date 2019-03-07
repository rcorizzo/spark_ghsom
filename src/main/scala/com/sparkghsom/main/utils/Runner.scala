package com.sparkghsom.main.utils

import java.io.{File, FileInputStream, FileWriter}
import java.util

import com.sparkghsom.main.globals.{GHSomConfig, SparkConfig}
import com.sparkghsom.main.input_generator._
import com.sparkghsom.main.mr_ghsom.{GHSom, GHSomParams, Instance}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{LabeledPoint, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import weka.classifiers.AbstractClassifier
import weka.classifiers.functions.LibSVM
import weka.core.{Instances, SelectedTag, Tag, WekaPackageManager}
import weka.core.converters.ConverterUtils.DataSource
import weka.classifiers.Evaluation
import weka.classifiers.evaluation.Prediction

object Runner {

  // Object used for classification tasks
  // GHSOM works on file without headings
  // other algorithms require the heading with the attributes list

  val sqlC = SparkConfig.getSqlContext
  import sqlC.implicits._

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val (appname,splitFrom,splitTo) =
    try {
      val prop = new java.util.Properties()
      prop.load(new FileInputStream("config.properties"))
      (
        prop.getProperty("appname"),
        prop.getProperty("splitFrom"),
        prop.getProperty("splitTo")
      )
    } catch { case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
    }

  // ************************************************************************************
  def basicExecutionGHSOM(path: String,
                     datasetName: String,
                     trainFileName: String,
                     testFileName: String,
                     tau1: Double,
                     tau2: Double,
                     epochs: Int,
                     splitNum: Int): (Double,Double,Double) = {

      val executionString = "GHSOM_" + datasetName + "_split_" + splitNum + "_tau1_" + tau1 + "_tau2_" + tau2 + "_epochs_" + epochs

      writeFile("output/" + executionString + ".predictions", "prediction,actual")
      writeFile("output/" + executionString + ".errors", "precision,recall,fmeasure")

      val trainingFile = path + datasetName + "/" + trainFileName
      val testFile = path + datasetName + "/" + testFileName

      val logger = LogManager.getLogger(appname)

      val sc = SparkConfig.getSparkContext

      println("EPOCHS: " + epochs)
      println("TAU1: " + tau1)
      println("TAU2: " + tau2)

      val dataset: RDD[String] = sc.textFile(trainingFile)    // id + label

      val testDataset = sc.textFile(testFile)
      val testDatasetDummyWithID: RDD[String] = testDataset.map(x => x.split(",").toList.dropRight(1).mkString(",") + ",dummy")

      val groundTruth: RDD[(Int, String)] = testDataset.map(x => {
        val row = x.split(",")
        (row.take(1).apply(0).toInt , row.takeRight(1).apply(0))
      })

      var (processedDataset, processedTestDataset, attributes) = {
        if(datasetName.equals("KDDCUP99")) {
          var datasetReader = new KddCupDatasetReader(dataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new KddCupDatasetReader(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else if(datasetName.equals("anneal")) {
          var datasetReader = new AnnealDataset(dataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new AnnealDataset(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else if(datasetName.equals("contraceptive")) {
          var datasetReader = new ContraceptiveMethodDataset(dataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new ContraceptiveMethodDataset(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else if(datasetName.equals("yeast")) {
          var datasetReader = new YeastDataset(dataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new YeastDataset(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else if(datasetName.equals("hepatitis")) {
          var datasetReader = new HepatitisDataset(dataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new HepatitisDataset(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else if(datasetName.equals("credit")) {
          var datasetReader = new CreditData(dataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new CreditData(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else if(datasetName.equals("car")) {
          var datasetReader = new Car(dataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new Car(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else if(datasetName.equals("census_income")) {
          var datasetReader = new CensusDataset(dataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new CensusDataset(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else if(datasetName.equals("dblp")) {
          var datasetReader = new Dblp(dataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new Dblp(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
        else {
          var datasetReader = new KddCupDatasetReader(dataset)
          val processedDataset: RDD[Instance] = datasetReader.getDataset
          datasetReader = new KddCupDatasetReader(testDatasetDummyWithID)
          val processedTestDataset: RDD[Instance] = datasetReader.getDataset
          (processedDataset, processedTestDataset, datasetReader.attributes)
        }
      }

      val ghsom = new GHSomParams()

      val (predictions, precision, recall, fmeasure) =
        ghsom.trainAndTest(processedDataset, processedTestDataset, groundTruth, attributes, tau1, tau2, epochs)

      predictions.foreach(e => {
        writeFile("output/" + executionString + ".predictions", e._1.toString + "," + e._2.toString)
      })

      writeFile("output/" + executionString + ".errors", precision + "," + recall + "," + fmeasure)

      println("Precision: " + precision)
      println("Recall: " + recall)
      println("F-Measure: " + fmeasure)
      (precision,recall,fmeasure)
    }
    //******************************************************************
    def basicExecutionRegressionGHSOM(path: String,
                                      datasetName: String,
                                      trainFileName: String,
                                      testFileName: String,
                                      tau1: Double,
                                      tau2: Double,
                                      epochs: Int,
                                      splitNum: Int): (Double,Double) = {

    val executionString = "GHSOM_" + datasetName + "_split_" + splitNum + "_tau1_" + tau1 + "_tau2_" + tau2 + "_epochs_" + epochs

    writeFile("output/" + executionString + ".predictions", "prediction,actual")
    writeFile("output/" + executionString + ".errors", "precision,recall,fmeasure")

    val trainingFile = path + datasetName + "/" + trainFileName
    val testFile = path + datasetName + "/" + testFileName

    println("Training file: " + trainingFile)
    println("Test file: " + testFile)

    val logger = LogManager.getLogger(appname)
    val sc = SparkConfig.getSparkContext

    println("EPOCHS: " + epochs)
    println("TAU1: " + tau1)
    println("TAU2: " + tau2)

    val dataset: RDD[String] = sc.textFile(trainingFile)    // id + label

    val testDataset = sc.textFile(testFile)
    val testDatasetDummyWithID: RDD[String] = testDataset.map(x => x.split(",").toList.dropRight(1).mkString(",") + ",0.0")

    val groundTruth: RDD[(Int, String)] = testDataset.map(x => {
      val row = x.split(",")
      (row.take(1).apply(0).toInt , row.takeRight(1).apply(0))
    })

    var (processedDataset, processedTestDataset, attributes) = {
      if((datasetName.equals("energy_efficiency_Y1"))||(datasetName.equals("energy_efficiency_Y2"))) {
        var datasetReader = new EnergyEfficiency(dataset)
        val processedDataset: RDD[Instance] = datasetReader.getDataset
        datasetReader = new EnergyEfficiency(testDatasetDummyWithID)
        val processedTestDataset: RDD[Instance] = datasetReader.getDataset
        (processedDataset, processedTestDataset, datasetReader.attributes)
      }
      else if(datasetName.equals("forest")) {
        var datasetReader = new Forest(dataset)
        val processedDataset: RDD[Instance] = datasetReader.getDataset
        datasetReader = new Forest(testDatasetDummyWithID)
        val processedTestDataset: RDD[Instance] = datasetReader.getDataset
        (processedDataset, processedTestDataset, datasetReader.attributes)
      }
      else if(datasetName.equals("facebook_metrics")) {
        var datasetReader = new FacebookMetrics(dataset)
        val processedDataset: RDD[Instance] = datasetReader.getDataset
        datasetReader = new FacebookMetrics(testDatasetDummyWithID)
        val processedTestDataset: RDD[Instance] = datasetReader.getDataset
        (processedDataset, processedTestDataset, datasetReader.attributes)
      }
      else if(datasetName.equals("automotive")) {
        var datasetReader = new Automotive(dataset)
        val processedDataset: RDD[Instance] = datasetReader.getDataset
        datasetReader = new Automotive(testDatasetDummyWithID)
        val processedTestDataset: RDD[Instance] = datasetReader.getDataset
        (processedDataset, processedTestDataset, datasetReader.attributes)
      }
      else {
        var datasetReader = new KddCupDatasetReader(dataset)
        val processedDataset: RDD[Instance] = datasetReader.getDataset
        datasetReader = new KddCupDatasetReader(testDatasetDummyWithID)
        val processedTestDataset: RDD[Instance] = datasetReader.getDataset
        (processedDataset, processedTestDataset, datasetReader.attributes)
      }
    }

    val ghsom = new GHSomParams()

    val (predictions, rmse, mae) =
      ghsom.trainAndTestRegression(processedDataset, processedTestDataset, groundTruth, attributes, tau1, tau2, epochs)

    predictions.foreach(e => {
      writeFile("output/" + executionString + ".predictions", e._1.toString + "," + e._2.toString)
    })

    writeFile("output/" + executionString + ".errors", rmse + "," + mae)

    println("RMSE: " + rmse)
    println("MAE: " + mae)

    (rmse, mae)
  }
  //******************************************************************
  def basicExecutionCompetitorsRegression(path: String,
                                          datasetName: String,
                                          trainFileName: String,
                                          testFileName: String,
                                          algorithm: String,
                                          splitNum: Int): (Double,Double) = {

    // Pipeline: DataFrame -> Categorical Features Treatment: (String Indexer -> One Hot Encoding) -> Algorithm -> Prediction + Error

    val executionString = algorithm + "_" + datasetName + "_split_" + splitNum

    writeFile("output/" + executionString + ".predictions", "prediction,actual")
    writeFile("output/" + executionString + ".errors", "precision,recall,fmeasure")

    val session = SparkConfig.getSparkSession
    import session.implicits._

    val trainingFile = path + datasetName + "/" + trainFileName
    val testFile = path + datasetName + "/" + testFileName

    val datasetDF = selectDataFrameFromTxt(trainingFile)    // id + label
    val testDF = selectDataFrameFromTxt(testFile)           // id + label

    datasetDF.show(3)

    println("Test set size: " + testDF.count())

    val testDatasetDummyWithID: Dataset[String] = testDF.map(row => row.toString().replace("[","").replace("]","").split(",").toList.dropRight(1).mkString(",") + ",dummy")

    val features = {
      if ((datasetName.equals("energy_efficiency_Y1"))||(datasetName.equals("energy_efficiency_Y2"))) {
        Array("X1","X2","X3","X4","X5","X6","X7","X8")
      }
      else if (datasetName.equals("facebook_metrics")) {
        Array("page_total_likes","type","category","post_month","post_weekday","post_hour","paid")
      }
      else if (datasetName.equals("forest")) {
        Array("X","Y","month","day","FFMC","DMC","DC","ISI","temp","RH","wind","rain")
      }
      else if (datasetName.equals("automotive")) {
        Array("symboling","normalized-losses","make","fuel-type","aspiration","num-of-doors","body-style","drive-wheels","engine-location",
          "wheel-base","length","width","height","curb-weight","engine-type","num-of-cylinders","engine-size","fuel-system","bore","stroke","compression-ratio",
          "horsepower","peak-rpm","city-mpg","highway-mpg")
      }
      else
        Array("nan")
    }

    // differently from the classification module, the class here is not considered for one hot encoding

    val categoricalCols = {
      if ((datasetName.equals("energy_efficiency_Y1"))||(datasetName.equals("energy_efficiency_Y2")))
        Array("X6")

      else if (datasetName.equals("forest")) {
        Array("month","day")
      }

      else if (datasetName.equals("facebook_metrics")) {
        Array("type","category","paid")
      }

      else if (datasetName.equals("automotive")) {
        Array("make","fuel-type","aspiration","num-of-doors","body-style","drive-wheels","engine-location",
          "engine-type","num-of-cylinders","fuel-system")
      }

      else
        Array("nan")
    }

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

    // encoding columns
    val indexColumns = df_indexed.columns.filter(x => ((x endsWith "_i") && (!x.equals("class_i"))))

    println("indexColumns")
    println(indexColumns.mkString(","))

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

    val numericalFeatures: Array[String] = features.diff(categoricalCols)

    println("numericalFeatures")
    println(numericalFeatures.mkString(","))

    val exceptions = Array("class")
    val categorical_vectorized = categoricalCols.diff(exceptions).map(s => s + "_i_vec")

    println("categoricalVectorized")
    println(categorical_vectorized.mkString(","))

    newFeatureSet = numericalFeatures.union(categorical_vectorized)

    println("Feature set: " + newFeatureSet.mkString(","))

    finalTrainDF.printSchema()

    val assembler = new VectorAssembler().setInputCols(newFeatureSet).setOutputCol("features")
    val assembledTrain: DataFrame = assembler.transform(finalTrainDF).select("features","class")
    val assembledTest: DataFrame = assembler.transform(finalTestDF).select("id","features")

    // (testRowID,indexedLabel)
    val groundTruth: Dataset[(Int, Double)] = finalTestDF.select("id","class").map(e => (e.apply(0).toString.toInt,e.apply(1).toString.toDouble))

    val (actualPred,rmse,mae) = {

      if(algorithm.equals("DT")) {
        trainAndTestDTRegression(assembledTrain, assembledTest, groundTruth)
      }

      else if(algorithm.equals("ISOREG")) {
        trainAndTestIsotonicRegression(assembledTrain, assembledTest, groundTruth)
      }

      else {  // (algorithm.equals("LINREG_xx"))
        val regParam = algorithm.split("_").apply(1).toDouble
        val (ap,r,m) = trainAndTestLinearRegression(assembledTrain, assembledTest, groundTruth.rdd, "class", 10, regParam, 0.8)
        (ap,r,m)
      }

      // if (algorithm.equals("MLP"))
      //trainAndTestMLPRegression(assembledTrain, assembledTest, groundTruth)
    }

    actualPred.collect.foreach(e => writeFile("output/" + executionString + ".predictions", e._1.toString + "," + e._2.toString))
    writeFile("output/" + executionString + ".errors", rmse.toString + "," + mae.toString)

    (rmse,mae)
  }
  //******************************************************************
  def basicExecutionCompetitors(path: String,
                                 datasetName: String,
                                 trainFileName: String,
                                 testFileName: String,
                                 algorithm: String,
                                 splitNum: Int): (Double,Double,Double) = {

      // Pipeline: DataFrame -> Categorical Features Treatment: (String Indexer -> One Hot Encoding) -> Algorithm -> Prediction + Error

      val executionString = algorithm + "_" + datasetName + "_split_" + splitNum

      writeFile("output/" + executionString + ".predictions", "prediction,actual")
      writeFile("output/" + executionString + ".errors", "precision,recall,fmeasure")

      val session = SparkConfig.getSparkSession
      import session.implicits._

      val trainingFile = path + datasetName + "/" + trainFileName
      val testFile = path + datasetName + "/" + testFileName

      val datasetDF = selectDataFrameFromTxt(trainingFile)    // id + label
      val testDF = selectDataFrameFromTxt(testFile)           // id + label

      println("Test set size: " + testDF.count())

      val testDatasetDummyWithID: Dataset[String] = testDF.map(row => row.toString().replace("[","").replace("]","").split(",").toList.dropRight(1).mkString(",") + ",dummy")

      val features = {
        if (datasetName.equals("KDDCUP99")) {
          Array("duration", "protocol_type", "service", "flag", "src_bytes", "dst_bytes", "land", "wrong_fragment",
            "urgent", "hot", "num_failed_logins", "logged_in", "num_compromised", "root_shell", "su_attempted", "num_root",
            "num_file_creations", "num_shells", "num_access_files", "num_outbound_cmds", "is_host_login", "is_guest_login",
            "count", "srv_count", "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate", "same_srv_rate",
            "diff_srv_rate", "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count", "dst_host_same_srv_rate",
            "dst_host_diff_srv_rate", "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate", "dst_host_serror_rate",
            "dst_host_srv_serror_rate", "dst_host_rerror_rate", "dst_host_srv_rerror_rate")
        }

        else if (datasetName.equals("anneal")) {
          Array("family", "product-type", "steel", "carbon", "hardness", "temper_rolling", "condition", "formability",
            "strength", "non-ageing", "surface-finish", "surface-quality", "enamelability", "bc", "bf", "bt", "bwme",
            "bl", "m", "chrom", "phos", "cbond", "marvi", "exptl", "ferro", "corr", "bbvc", "lustre", "jurofm", "s", "p",
            "shape", "thick", "width", "len", "oil", "bore", "packing")
        }

        else if (datasetName.equals("yeast"))
          Array("mcg", "gvh", "alm", "mit", "erl", "pox", "vac", "nuc")

        else if (datasetName.equals("hepatitis"))
          Array("age", "sex", "steroid", "antivirals", "fatigue", "malaise", "anorexia", "liver_big", "liver_firm",
            "spleen_palpable", "spiders", "ascites", "varices", "bilirubin", "alk_phosphate", "sgot", "albumin", "protime",
            "histology")

        else if (datasetName.equals("credit"))
          Array("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11", "a12", "a13", "a14", "a15")

        else if (datasetName.equals("contraceptive"))
          Array("wife_age","wife_education","husband_education","children","wife_religion","wife_working","husband_occupation","std_living","media_exposure")

        else if (datasetName.equals("car"))
          Array("buying","maint","doors","persons","lug_boot","safety")

        else if (datasetName.equals("census_income"))
          Array("age", "workclass", "fnlwgt", "education", "education_num", "marital_status", "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "native_country")

        else if (datasetName.equals("dblp"))
          Array("year","name","type")

        else
          Array("nan")
      }

      val categoricalCols = {
        if (datasetName.equals("KDDCUP99"))
          Array("protocol_type","service","flag","land","class")

        else if (datasetName.equals("anneal"))
          Array("carbon", "thick", "strength", "family", "product-type", "steel", "temper_rolling", "condition", "formability", "non-ageing",
            "surface-finish", "surface-quality", "enamelability", "bc", "bf", "bt", "bwme", "bl", "m", "chrom",
            "phos", "cbond", "marvi", "exptl", "ferro", "corr", "bbvc", "lustre", "jurofm", "s", "p", "shape",
            "len", "oil", "bore", "packing", "class")

        else if (datasetName.equals("yeast"))
          Array("class")

        else if (datasetName.equals("hepatitis"))
          Array("sex", "steroid", "antivirals", "fatigue", "malaise", "anorexia", "liver_big", "liver_firm", "spleen_palpable", "spiders", "ascites", "varices", "histology", "class")

        else if (datasetName.equals("credit"))
          Array("a1", "a4", "a5", "a6", "a7", "a9", "a10", "a12", "a13", "class")

        else if (datasetName.equals("contraceptive"))
          Array("wife_education","husband_education", "wife_religion","wife_working","husband_occupation","std_living","media_exposure","class")

        else if (datasetName.equals("car"))
          Array("buying","maint", "doors", "persons","lug_boot","safety","class")

        else if (datasetName.equals("census_income"))
          Array("workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country", "class")

        else if (datasetName.equals("dblp"))
          Array("name","type","class")

        else
          Array("nan")
      }

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

      // encoding columns
      val indexColumns  = {
        if (datasetName.equals("anneal"))
          df_indexed.columns.filter(x => ((x endsWith "_i") && (!x.equals("class_i")) && (!x.equals("product-type_i") && (!x.equals("m_i")) && (!x.equals("marvi_i")) && (!x.equals("corr_i")) && (!x.equals("jurofm_i") && (!x.equals("s_i")) && (!x.equals("p_i"))))))
        else
          df_indexed.columns.filter(x => ((x endsWith "_i") && (!x.equals("class_i"))))
      }

      println("indexColumns")
      println(indexColumns.mkString(","))

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

      if(datasetName.equals("anneal")) {
        val exceptions = Array("product-type", "m", "marvi", "corr", "jurofm", "s", "p", "class")   // features with one distinct value which cannot be vectorzed + class
        val categorical_vectorized = categoricalCols.diff(exceptions).map(s => s + "_i_vec")
        val categorical_indexed = exceptions.map(s => s + "_i")
        newFeatureSet = features.diff(categoricalCols).union(categorical_indexed).union(categorical_vectorized)
      }
      else {
        val numericalFeatures = features.diff(categoricalCols)

        println("numericalFeatures")
        println(numericalFeatures.mkString(","))

        val exceptions = Array("class")
        val categorical_vectorized = categoricalCols.diff(exceptions).map(s => s + "_i_vec")

        println("categoricalVectorized")
        println(categorical_vectorized.mkString(","))

        newFeatureSet = numericalFeatures.union(categorical_vectorized)
      }

      println("Feature set: " + newFeatureSet.mkString(","))

      finalTrainDF.printSchema()

      val assembler = new VectorAssembler().setInputCols(newFeatureSet).setOutputCol("features")
      val assembledTrain: DataFrame = assembler.transform(finalTrainDF).select("features","class_i")
      val assembledTest: DataFrame = assembler.transform(finalTestDF).select("id","features")

      // (testRowID,indexedLabel)
      val groundTruth: Dataset[(Int, Double)] = finalTestDF.select("id","class_i").map(e => (e.apply(0).toString.toInt,e.apply(1).toString.toDouble))

      val (actualPred,wPrec,wRec,wFM) = {
        if(algorithm.equals("NB"))
          trainAndTestNB(assembledTrain, assembledTest, groundTruth)

        else if(algorithm.equals("DT"))
          trainAndTestDT(assembledTrain, assembledTest, groundTruth)

        else if(algorithm.equals("LR"))
          trainAndTestLR(assembledTrain, assembledTest, groundTruth)

        else // (algorithm.equals("MLP"))
          trainAndTestMLP(assembledTrain, assembledTest, groundTruth)
      }

      actualPred.collect.foreach(e => writeFile("output/" + executionString + ".predictions", e._1.toString + "," + e._2.toString))
      writeFile("output/" + executionString + ".errors", wPrec.toString + "," + wRec.toString + "," + wFM.toString)

      (wPrec,wRec,wFM)
  }
  // ************************************************************************************
  def gridKMeans(assembledTrain: DataFrame, assembledTest: DataFrame, groundTruth: Dataset[(Int, Double)], numInstances: Double) = {

    val sqrtDiv8 = Math.sqrt(numInstances)/8
    val sqrtDiv4 = Math.sqrt(numInstances)/4
    val sqrtDiv2 = Math.sqrt(numInstances)/2
    val sqrt = Math.sqrt(numInstances)
    val sqrtMul2 = Math.sqrt(numInstances)*2
    val sqrtMul4 = Math.sqrt(numInstances)*4
    val sqrtMul8 = Math.sqrt(numInstances)*8

    val params = Array(sqrtDiv8,sqrtDiv4,sqrtDiv2,sqrt,sqrtMul2,sqrtMul4,sqrtMul8)

    params.foreach(k => {

      val (actualPred,wPrec,wRec,wFM) = trainAndTestKMeans(assembledTrain, assembledTest, groundTruth, k.toInt)

    })
  }
  // ************************************************************************************
  def trainAndTestKMeans(assembledTrain: DataFrame,
                         assembledTest: DataFrame,
                         groundTruth: Dataset[(Int, Double)],
                         k: Int) = {

    val trainDF = assembledTrain.withColumnRenamed("class_i","label")

    val kmeans = new KMeans().setK(k).setSeed(1L)

    val model = kmeans
      .setFeaturesCol("features")
      .fit(trainDF)

    println("clusterCenters")
    model.clusterCenters.foreach(println)
    println()

    println("show transform")
    model.transform(assembledTest).show(10)
    println()

    val preds: RDD[(Int, Int)] = model.transform(assembledTest).select("id","prediction").map(row => {
      val splitter = row.toString().replace("[","").replace("]","").split(",")
      val idTestInstance = splitter.apply(0).toInt
      val predictedIdCentroid = splitter.apply(1).toInt
      (predictedIdCentroid,idTestInstance)
    }).rdd

    /*
    println("preds")
    preds.take(10).foreach(println)
    println()
    */

    /*println("clusterCenters")
    model.clusterCenters.zipWithIndex.foreach(println)*/

    val idCentroid_target: Array[(Int, Double)] = model.clusterCenters.zipWithIndex
      .map {
        case (centroid, idCentroid) => {
          println(centroid.toArray.takeRight(1).apply(0))
          (idCentroid, Math.round(centroid.toArray.takeRight(1).apply(0)).toDouble)
      }
    }

    println("idCentroid_target")
    idCentroid_target.foreach(println)
    println()


    val idCentroid_target_RDD: RDD[(Int, Double)] = SparkConfig.sc.parallelize(idCentroid_target)

    val joinedPreds: RDD[(Int, Double)] = preds.join(idCentroid_target_RDD).map {
      case(idCentroid, (idTestInstance, prediction)) => {
        (idTestInstance, prediction)
      }
    }

    //joinedPreds.collect().foreach(println)

    println("Prediction count: " + joinedPreds.count())

    /*
    val predictions = model.transform(assembledTest)
      .select("features","prediction")
      .rdd

    val predIds: RDD[(Int, Double)] = predictions.map(row => {
      val allString = row.toString().split("],")
      val idRow = allString.apply(1).split(",").apply(0).replace("[","").toDouble.toInt
      val pred = allString.apply(1).split(",").last.replace("]","").toDouble
      (idRow,pred)
    })
*/
    val actualPred: RDD[(Double, Double)] = groundTruth.rdd.join(joinedPreds).map({
      case (id,actual_pred) => actual_pred
    })

    actualPred.collect.foreach(println)

    //println("Prediction count: " + actualPred.count())

    // Calculate errors
    val metrics_multi = new MulticlassMetrics(actualPred)
    println(s"Weighted precision: ${metrics_multi.weightedPrecision}")
    println(s"Weighted recall: ${metrics_multi.weightedRecall}")
    println(s"Weighted F1 score: ${metrics_multi.weightedFMeasure}")

    val wPrec = metrics_multi.weightedPrecision
    val wRec = metrics_multi.weightedRecall
    val wFM = metrics_multi.weightedFMeasure

    (actualPred,wPrec,wRec,wFM)

  }
  //************************************************************************************************
  def trainAndTestSVR(path: String,
                      datasetName: String,
                      trainFile: String,
                      testFile: String,
                      modality: String,
                      splitNumber: Int): (Double, Double) = {

    WekaPackageManager.loadPackages( false, true, false )

    val classifier : AbstractClassifier = new LibSVM()

    val options: String = {
      if(modality.equals("linear"))
        "-S 4 -K 0 -D 3 -G 0.0 -R 0.0 -N 0.5 -M 40.0 -C 1.0 -E 0.001 -P 0.1"
      else if (modality.equals("polynomial"))
        "-S 4 -K 1 -D 3 -G 0.0 -R 0.0 -N 0.5 -M 40.0 -C 1.0 -E 0.001 -P 0.1"
      else if (modality.equals("rbf"))
        "-S 4 -K 2 -D 3 -G 0.0 -R 0.0 -N 0.5 -M 40.0 -C 1.0 -E 0.001 -P 0.1"
      else if (modality.equals("sigmoid"))
        "-S 4 -K 3 -D 3 -G 0.0 -R 0.0 -N 0.5 -M 40.0 -C 1.0 -E 0.001 -P 0.1"
      else
        ""
    }
    val optionsArray = options.split(" ")
    classifier.setOptions(optionsArray)

    val trainDataset: Instances = new DataSource(path + datasetName + trainFile + ".csv").getDataSet
    val testDataset: Instances = new DataSource(path + datasetName + testFile + ".csv").getDataSet

    trainDataset.setClassIndex(testDataset.numAttributes()-1)
    testDataset.setClassIndex(testDataset.numAttributes()-1)

    classifier.buildClassifier(trainDataset)

    val eval : Evaluation  = new Evaluation(trainDataset)
    eval.evaluateModel(classifier, testDataset)

    val predictions: util.ArrayList[Prediction] = eval.predictions()
    val predArray: Array[String] = predictions.toArray.map(p => p.toString)   // to return

    println("Predictions:")
    println(predArray.mkString(","))
    println()
    println("RMSE:" + eval.rootMeanSquaredError())
    println("MAE:" + eval.meanAbsoluteError())

    writeFile("output/SVR_" + modality + "_" + datasetName + "_" + splitNumber + ".predictions", "prediction,actual")

    predArray.foreach(e => {
      val split = e.split(" ")
      val measured = split(1)
      val predicted =  split(2)
      writeFile("output/SVR_" + modality + "_" + datasetName + "_" + splitNumber + ".predictions", predicted + "," + measured)
    })

    writeFile("output/SVR_" + modality + "_" + datasetName + "_" + splitNumber + ".errors", eval.rootMeanSquaredError() + "," + "rmse,mae")
    writeFile("output/SVR_" + modality + "_" + datasetName + "_" + splitNumber + ".errors", eval.rootMeanSquaredError() + "," + eval.meanAbsoluteError())

    (eval.rootMeanSquaredError(),eval.meanAbsoluteError())
  }
  //************************************************************************************************
  def trainAndTestLinearRegression(processedDataset: DataFrame,
                           processedTestDataset: DataFrame,
                           groundTruthRDD: RDD[(Int, Double)],
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

    // Fit the model
    val lrModel = lr.setFitIntercept(true)
      .setFeaturesCol("features")
      .fit(trainDataset)

    val prediction: DataFrame = lrModel
      .setFeaturesCol("features")
      .setPredictionCol(targetAttribute)
      .transform(processedTestDataset)

    val preds: RDD[(Int, Double)] = prediction.select("id","class").map(row => {
      val splitter = row.toString().replace("[","").replace("]","").split(",")
      val id = splitter.apply(0).toInt
      val pred = splitter.apply(1).toDouble
      (id,pred)
    }).rdd

    val actualPred: RDD[(Double, Double)] = groundTruthRDD.join(preds).map({
      case (id,actual_pred) => actual_pred
    })

    println("Prediction count: " + actualPred.count())

    // Calculate errors
    val predsCollected: Array[Double] = actualPred.map(x => x._1).collect
    val actualCollected: Array[Double] = actualPred.map(x => x._2).collect
    val (rmse,mae) = calculateErrors(predsCollected, actualCollected)
    println("RMSE: " + rmse)
    println("MAE: " + mae)

    (actualPred,rmse,mae)
  }
  // ************************************************************************************
  def trainAndTestDTRegression(assembledTrain: DataFrame, assembledTest: DataFrame, groundTruth: Dataset[(Int, Double)]): (RDD[(Double, Double)], Double, Double) = {

    // assembledTrain : (features, class_i)
    // assembledTest  : (id, features)
    // groundTruth    : (idRow, doubleClassLabel)

    val dt = new DecisionTreeRegressor()
      .setLabelCol("class")
      .setFeaturesCol("features")

    val trainedDT = dt.fit(assembledTrain)

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

    // Calculate errors
    val predsCollected: Array[Double] = actualPred.map(x => x._1).collect
    val actualCollected: Array[Double] = actualPred.map(x => x._2).collect
    val (rmse,mae) = calculateErrors(predsCollected, actualCollected)
    println("RMSE: " + rmse)
    println("MAE: " + mae)

    (actualPred,rmse,mae)
  }
  // ************************************************************************************
  def trainAndTestNB(assembledTrain: DataFrame, assembledTest: DataFrame, groundTruth: Dataset[(Int, Double)]): (RDD[(Double, Double)], Double, Double, Double) = {

    val trainDF = assembledTrain.withColumnRenamed("class_i","label")

    val nb = new NaiveBayes()
      .setFeaturesCol("features")
      .fit(trainDF)

    val preds: RDD[(Int, Double)] = nb.transform(assembledTest).select("id","prediction").map(row => {
      val splitter = row.toString().replace("[","").replace("]","").split(",")
      val id = splitter.apply(0).toInt
      val pred = splitter.apply(1).toDouble
      (id,pred)
    }).rdd
/*
    val predictions = nb.transform(assembledTest).select("features","prediction").rdd

    val predIds: RDD[(Int, Double)] = predictions.map(row => {
      val allString = row.toString().split("],")
      val idRow = allString.apply(1).split(",").apply(0).replace("[","").toDouble.toInt
      val pred = allString.apply(1).split(",").last.replace("]","").toDouble
      (idRow,pred)
    })
*/
    val actualPred: RDD[(Double, Double)] = groundTruth.rdd.join(preds).map({
      case (id,actual_pred) => actual_pred
    })

    println("Prediction count: " + actualPred.count())

    // Calculate errors
    val metrics_multi = new MulticlassMetrics(actualPred)

    println("Weighted precision: " + metrics_multi.weightedPrecision)
    println("Weighted recall: " + metrics_multi.weightedRecall)
    println("Weighted F1 score: " + metrics_multi.weightedFMeasure)

    val wPrec = metrics_multi.weightedPrecision
    val wRec = metrics_multi.weightedRecall
    val wFM = metrics_multi.weightedFMeasure

    (actualPred,wPrec,wRec,wFM)

  }
  // ************************************************************************************
  def trainAndTestLR(assembledTrain: DataFrame, assembledTest: DataFrame, groundTruth: Dataset[(Int, Double)]): (RDD[(Double, Double)], Double, Double, Double) = {

    val trainDF = assembledTrain.withColumnRenamed("class_i","label")

    val classifier: LogisticRegression = new LogisticRegression()
      .setMaxIter(10)
      .setTol(1E-6)
      .setFitIntercept(true)

    val ovr = new OneVsRest().setClassifier(classifier)

    val ovrModel = ovr.fit(trainDF)

    val preds: RDD[(Int, Double)] = ovrModel.transform(assembledTest).select("id","prediction").map(row => {
      val splitter = row.toString().replace("[","").replace("]","").split(",")
      val id = splitter.apply(0).toInt
      val pred = splitter.apply(1).toDouble
      (id,pred)
    }).rdd

    val actualPred: RDD[(Double, Double)] = groundTruth.rdd.join(preds).map({
      case (id,actual_pred) => actual_pred
    })

    println("Prediction count: " + actualPred.count())

    // Calculate errors
    val metrics_multi = new MulticlassMetrics(actualPred)

    println("Weighted precision: " + metrics_multi.weightedPrecision)
    println("Weighted recall: " + metrics_multi.weightedRecall)
    println("Weighted F1 score: " + metrics_multi.weightedFMeasure)

    val wPrec = metrics_multi.weightedPrecision
    val wRec = metrics_multi.weightedRecall
    val wFM = metrics_multi.weightedFMeasure

    (actualPred,wPrec,wRec,wFM)

  }
  // ************************************************************************************
  def trainAndTestMLP(assembledTrain: DataFrame,
                      assembledTest: DataFrame,
                      groundTruth: Dataset[(Int, Double)]): (RDD[(Double, Double)], Double, Double, Double) = {

    val numFeatures = assembledTrain.schema.apply("features").metadata.json.split("\"num_attrs\":").apply(1).dropRight(2).toInt

    val hiddenLayerSize: Int = Math.round((numFeatures.toDouble * 3) / 4).toInt

    val trainDF = assembledTrain.withColumnRenamed("class_i","label")

    val numClasses = trainDF.schema.apply("label").metadata.json.split("\"vals\":").apply(1).split(",").length.toInt

    val layers = Array[Int](numFeatures, hiddenLayerSize, numClasses)

    trainDF.printSchema()

    println("Num features: " + numFeatures + " - hiddenLayerSize: " +  hiddenLayerSize)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(10)

    // train the model
    val model = trainer.fit(trainDF)

    val preds: RDD[(Int, Double)] = model.transform(assembledTest).select("id","prediction").map(row => {
      val splitter = row.toString().replace("[","").replace("]","").split(",")
      val id = splitter.apply(0).toInt
      val pred = splitter.apply(1).toDouble
      (id,pred)
    }).rdd

    val actualPred: RDD[(Double, Double)] = groundTruth.rdd.join(preds).map({
      case (id,actual_pred) => actual_pred
    })

    println("Prediction count: " + actualPred.count())

    // Calculate errors
    val metrics_multi = new MulticlassMetrics(actualPred)

    println("Weighted precision: " + metrics_multi.weightedPrecision)
    println("Weighted recall: " + metrics_multi.weightedRecall)
    println("Weighted F1 score: " + metrics_multi.weightedFMeasure)

    val wPrec = metrics_multi.weightedPrecision
    val wRec = metrics_multi.weightedRecall
    val wFM = metrics_multi.weightedFMeasure

    (actualPred,wPrec,wRec,wFM)

  }
  // ************************************************************************************
  def trainAndTestIsotonicRegression(assembledTrain: DataFrame,
                                     assembledTest: DataFrame,
                                     groundTruth: Dataset[(Int, Double)]): (RDD[(Double, Double)], Double, Double) = {

    val numFeatures = assembledTrain.schema.apply("features").metadata.json.split("\"num_attrs\":").apply(1).dropRight(2).toInt

    val hiddenLayerSize: Int = Math.round((numFeatures.toDouble * 3) / 4).toInt

    val trainDF = assembledTrain.withColumnRenamed("class","label")

    trainDF.printSchema()

    // create the trainer and set its parameters
    val trainer = new org.apache.spark.ml.regression.IsotonicRegression().fit(trainDF)

    val preds: RDD[(Int, Double)] = trainer.transform(assembledTest).select("id","prediction").map(row => {
      val splitter = row.toString().replace("[","").replace("]","").split(",")
      val id = splitter.apply(0).toInt
      val pred = splitter.apply(1).toDouble
      (id,pred)
    }).rdd

    val actualPred: RDD[(Double, Double)] = groundTruth.rdd.join(preds).map({
      case (id,actual_pred) => actual_pred
    })

    println("Prediction count: " + actualPred.count())

    // Calculate errors
    val predsCollected: Array[Double] = actualPred.map(x => x._1).collect
    val actualCollected: Array[Double] = actualPred.map(x => x._2).collect
    val (rmse,mae) = calculateErrors(predsCollected, actualCollected)
    println("RMSE: " + rmse)
    println("MAE: " + mae)

    (actualPred,rmse,mae)
  }
  // ************************************************************************************
  def trainAndTestDT(assembledTrain: DataFrame, assembledTest: DataFrame, groundTruth: Dataset[(Int, Double)]): (RDD[(Double, Double)], Double, Double, Double) = {

    // assembledTrain : (features, class_i)
    // assembledTest  : (id, features)
    // groundTruth    : (idRow, doubleClassLabel)

    val dt = new DecisionTreeClassifier()
      .setLabelCol("class_i")
      .setFeaturesCol("features")

    val trainedDT: DecisionTreeClassificationModel = dt.fit(assembledTrain)

    val preds: RDD[(Int, Double)] = trainedDT.transform(assembledTest).select("id","prediction").map(row => {
      val splitter = row.toString().replace("[","").replace("]","").split(",")
      val id = splitter.apply(0).toInt
      val pred = splitter.apply(1).toDouble
      (id,pred)
    }).rdd

    /*
    val predictions = trainedDT.transform(assembledTest).select("features","prediction").rdd

    val predIds: RDD[(Int, Double)] = predictions.map(row => {
      val allString = row.toString().split("],")
      val idRow = allString.apply(1).split(",").apply(0).replace("[","").toDouble.toInt
      val pred = allString.apply(1).split(",").last.replace("]","").toDouble
      (idRow,pred)
    })
*/
    val actualPred: RDD[(Double, Double)] = groundTruth.rdd.join(preds).map({
      case (id,actual_pred) => actual_pred
    })

    println("Prediction count: " + actualPred.count())

    // Calculate errors
    val metrics_multi = new MulticlassMetrics(actualPred)
    println(s"Weighted precision: ${metrics_multi.weightedPrecision}")
    println(s"Weighted recall: ${metrics_multi.weightedRecall}")
    println(s"Weighted F1 score: ${metrics_multi.weightedFMeasure}")

    val wPrec = metrics_multi.weightedPrecision
    val wRec = metrics_multi.weightedRecall
    val wFM = metrics_multi.weightedFMeasure

    (actualPred,wPrec,wRec,wFM)
  }
  // ************************************************************************************
/*  def crossValidationExecution(): Unit = {

    writeFile("output/" + datasetName + ".stats", "precision,recall,fmeasure")

    var splitErrors: Array[(Double,Double,Double)] = Array.empty[(Double,Double,Double)]

    for (i <- splitFrom to splitTo) {

      writeFile("output/" + datasetName + "_" + i + ".predictions", "prediction,actual")

      println("Processing split " + i)

      val trainSplitPath = path + datasetName + "_train_" + i + "/part-00000"
      val testSplitPath = path + datasetName + "_test_" + i + "/part-00000"

      val logger = LogManager.getLogger(appname)

      val sc = SparkConfig.getSparkContext

      var epochs = GHSomConfig.epochs

      println("EPOCHS: " + epochs)
      println("TAU1: " + GHSomConfig.tau1)
      println("TAU2: " + GHSomConfig.tau2)

      val dataset = sc.textFile(trainSplitPath)    // id + label
      var datasetReader = new KddCupDatasetReader(dataset)
      val processedDataset = datasetReader.getDataset

      val testDataset: RDD[String] = sc.textFile(testSplitPath)
      val testDatasetDummyWithID: RDD[String] = testDataset.map(x => x.split(",").toList.dropRight(1).mkString(",") + ",dummy")

      val groundTruth: RDD[(Int, String)] = testDataset.map(x => {
        val row = x.split(",").toList
        (row.take(1).apply(0).toInt , row.takeRight(1).apply(0))
      })

      datasetReader = new KddCupDatasetReader(testDatasetDummyWithID)
      val processedTestDataset: RDD[Instance] = datasetReader.getDataset

      val ghsom = GHSom()

      val (predictions, precision, recall, fmeasure) =
        ghsom.trainAndTest(processedDataset, processedTestDataset, groundTruth, datasetReader.attributes, epochs)

      splitErrors :+ (precision,recall,fmeasure)

      predictions.foreach(e => {
        writeFile("output/" + datasetName + "_" + i + ".predictions", e._1.toString + "," + e._2.toString)
      })

      writeFile("output/" + datasetName + ".splitErrors", precision + "," + recall + "," + fmeasure)

    }

    println("Average Precision: " + splitErrors.map(x => x._1).sum/splitErrors.size)
    println("Average Recall: " + splitErrors.map(x => x._2).sum/splitErrors.size)
    println("Average F-Measure: " + splitErrors.map(x => x._3).sum/splitErrors.size)
  } */
  // ************************************************************************************
  def writeFile(fileName: String, content: String): Unit = {
    val fw = new FileWriter(fileName, true)
    try {
      fw.write(content + "\r\n")
    }
    catch { case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
    }
    fw.close()
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
  def rowToVec(x: Array[String]):Array[Double] = {
    /***
      *  Convert a row to an array excluding the initial value (id)
      */

    val size = x.length

    val y =
      for(i <- 1 to size-1)
        yield x.apply(i).toDouble

    y.toArray
  }
  // ****************************************************************************************************
  def calculateErrors(predicted: Array[Double], expected: Array[Double]): (Double, Double) = {

    //println("Predicted: " + predicted.mkString(","))
    //println("Expected: " + expected.mkString(","))

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
}
