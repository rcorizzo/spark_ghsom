package com.sparkghsom.main.utils

import com.sparkghsom.main.globals.{GHSomConfig, SparkConfig}
import com.sparkghsom.main.input_generator.{KddCupDatasetReader, KddCupDatasetReader2}
import com.sparkghsom.main.mr_ghsom.{GHSom, Instance}
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD

object Scalability {

  def main(args : Array[String]): Unit = {

    execute()

  }

  def execute() {

    val logger = LogManager.getLogger("spark-ghsom")
    val sc = SparkConfig.getSparkContext

    val path = GHSomConfig.path
    var epochs = GHSomConfig.epochs

    println("EPOCHS: " + epochs)
    println("TAU1: " + GHSomConfig.tau1)
    println("TAU2: " + GHSomConfig.tau2)

    println(path)

    val dataset: RDD[String] = sc.textFile(path, 400)    // id + label

    //if(path.contains("_8x.csv"))
    //    var datasetReader = new KddCupDatasetReader8(dataset)
    //if(path.contains("_4x.csv"))
    //    var datasetReader = new KddCupDatasetReader4(dataset)
    //else if(path.contains("_2x.csv"))
    //    var datasetReader = new KddCupDatasetReader2(dataset)


    var datasetReader = new KddCupDatasetReader(dataset)

    //var datasetReader = new KddCupDatasetReader2(dataset)

    val processedDataset: RDD[Instance] = datasetReader.getDataset

    val ghsom = GHSom()
    ghsom.train(processedDataset, datasetReader.attributes, epochs)
  }
}
