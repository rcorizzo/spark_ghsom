package com.sparkghsom.main.utils

import com.sparkghsom.main.globals.{GHSomConfig, SparkConfig}
import com.sparkghsom.main.input_generator._
import com.sparkghsom.main.mr_ghsom.{GHSom, Instance}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD

object FileExec {

  def main(args : Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val datasetName: String = args(0)
    val numPartitions: Int = args(1).toInt
    execute(datasetName,numPartitions)

  }

  def execute(datasetName: String, numPartitions: Int) {

    val path = GHSomConfig.path
    val completePath = path + datasetName + ".csv"

    val logger = LogManager.getLogger("spark-ghsom")
    val sc = SparkConfig.getSparkContext

    var epochs = GHSomConfig.epochs

    println("EPOCHS: " + epochs)
    println("TAU1: " + GHSomConfig.tau1)
    println("TAU2: " + GHSomConfig.tau2)

    val dataset: RDD[String] = sc.textFile(completePath, numPartitions)    // id + label

    if(datasetName == "KDDCUP99") {
      var datasetReader = new KddCupDatasetReader(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "KDDCUP99_2x") {
      var datasetReader = new KddCupDatasetReader2(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "KDDCUP99_4x") {
      var datasetReader = new KddCupDatasetReader4(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "KDDCUP99_8x") {
      var datasetReader = new KddCupDatasetReader8(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "KDDCUP99_16x") {
      var datasetReader = new KddCupDatasetReader16(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
 /*   else if(datasetName == "KDDCUP99_32x") {
      var datasetReader = new KddCupDatasetReader32(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    } */
    else if(datasetName == "alon") {
      var datasetReader = new Alon(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "borovecki") {
      var datasetReader = new Borovecki(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "burczynski") {
      var datasetReader = new Burczynski(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "chiaretti") {
      var datasetReader = new Chiaretti(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "chin") {
      var datasetReader = new Chin(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "christensen") {
      var datasetReader = new Christensen(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "chowdary") {
      var datasetReader = new Chowdary(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "golub") {
      var datasetReader = new Golub(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "gordon") {
      var datasetReader = new Gordon(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "gravier") {
      var datasetReader = new Gravier(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "khan") {
      var datasetReader = new Khan(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "pomeroy") {
      var datasetReader = new Pomeroy(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "shipp") {
      var datasetReader = new Shipp(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "sorlie") {
      var datasetReader = new Sorlie(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "singh") {
      var datasetReader = new Singh(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "su") {
      var datasetReader = new Su(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "subramanian") {
      var datasetReader = new Subramanian(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "tian") {
      var datasetReader = new Tian(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "west") {
      var datasetReader = new West(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "yeoh") {
      var datasetReader = new Yeoh(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }
    else if(datasetName == "imagedata") {
      var datasetReader = new ImageData(dataset)
      val processedDataset: RDD[Instance] = datasetReader.getDataset
      val ghsom = GHSom()
      ghsom.train(processedDataset, datasetReader.attributes, epochs)
    }


    //if(path.contains("_8x.csv"))
    //    var datasetReader = new KddCupDatasetReader8(dataset)
    //if(path.contains("_4x.csv"))
    //    var datasetReader = new KddCupDatasetReader4(dataset)
    //else if(path.contains("_2x.csv"))
    //    var datasetReader = new KddCupDatasetReader2(dataset)

  }
}
