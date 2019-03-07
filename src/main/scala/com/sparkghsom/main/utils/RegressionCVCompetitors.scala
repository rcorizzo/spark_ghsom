package com.sparkghsom.main.utils

import org.apache.log4j.{Level, Logger}

object RegressionCVCompetitors {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

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

    var perFoldResults = Array.empty[(Double,Double)]

    // 5 FOLD CV FOR NB, LR, DT or GHSOM with fixed T1,T2, params

    for(split <- 1 to maxSplits) {
      val trainFileName = "/" + datasetName + "_train_" + split + "/part-00000-head"
      val testFileName = "/" + datasetName + "_test_" + split + "/part-00000-head"

      println("Training: " + trainFileName)
      val (rmse, mae) = {
        if(algorithm.contains("SVR"))
          Runner.trainAndTestSVR(path, datasetName, trainFileName, testFileName, algorithm.split("_").apply(1), split)
        else
          Runner.basicExecutionCompetitorsRegression(path, datasetName, trainFileName, testFileName, algorithm, split)
      }
      perFoldResults :+= (rmse, mae)
    }

    val rmseDiv = perFoldResults.map(x => x._1).sum/perFoldResults.size
    val maeDiv = perFoldResults.map(x => x._2).sum/perFoldResults.size

    println("Average results: " + rmseDiv + "," + maeDiv)
  }
}
