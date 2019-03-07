package com.sparkghsom.main.utils

import org.apache.log4j.{Level, Logger}

object ClassifyCVCompetitors {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    // algorithm    =    { NB, DT, KMeans }
    // datasetName  =    { KDDCUP99, anneal, credit, yeast, hepatitis, contraceptive}

    val path = args(0)
    val datasetName = args(1)
    val maxSplits = args(2).toInt
    val algorithm = args(3)

    println("Execution...")
    execute(path, datasetName, maxSplits, algorithm)
  }
  //*******************************************************************************************
  def execute(path: String,
              datasetName: String,
              maxSplits: Int,
              algorithm: String) = {

    var perFoldResults = Array.empty[(Double,Double,Double)]

    // 5 FOLD CV FOR NB, LR, DT or GHSOM with fixed T1,T2, params

    for(split <- 1 to maxSplits) {
      val trainFileName = "/" + datasetName + "_train_" + split + "/part-00000-head"
      val testFileName = "/" + datasetName + "_test_" + split + "/part-00000-head"

      println("Training: " + trainFileName)
      val (precision,recall,fmeasure) = Runner.basicExecutionCompetitors(path, datasetName, trainFileName, testFileName, algorithm, split)
      perFoldResults :+= (precision,recall,fmeasure)
    }

    val precDiv = perFoldResults.map(x => x._1).sum/perFoldResults.size
    val recDiv = perFoldResults.map(x => x._2).sum/perFoldResults.size
    val fMeasDiv = perFoldResults.map(x => x._3).sum/perFoldResults.size

    println("Average results: " + precDiv + "," + recDiv + "," + fMeasDiv)
  }
}
