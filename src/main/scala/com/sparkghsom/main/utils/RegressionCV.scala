package com.sparkghsom.main.utils

import java.io.FileWriter

import org.apache.log4j.{Level, Logger}

object RegressionCV {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val path = args(0)
    val datasetName = args(1)
    val tau1 = args(2).toDouble
    val tau2 = args(3).toDouble
    val epochs = args(4).toInt
    val maxSplits = args(5).toInt

    execute(path, datasetName, tau1, tau2, epochs, maxSplits)

  }
  //*******************************************************************************************
  def execute(path: String,
              datasetName: String,
              tau1: Double,
              tau2: Double,
              epochs: Int,
              maxSplits: Int) = {

    println("Execution...")

    var perFoldResults = Array.empty[(Double,Double)]

    for(split <- 1 to maxSplits) {
      val trainFileName = "/" + datasetName + "_train_" + split + "/part-00000"
      val testFileName = "/" + datasetName + "_test_" + split + "/part-00000"
      val (rmse,mae) = Runner.basicExecutionRegressionGHSOM(path, datasetName, trainFileName, testFileName, tau1, tau2, epochs, split)
      perFoldResults :+= (rmse,mae)
    }

    val rmseDiv = perFoldResults.map(x => x._1).sum/perFoldResults.size
    val maeDiv = perFoldResults.map(x => x._2).sum/perFoldResults.size

    println("Average results: " + rmseDiv + "," + maeDiv)

    val executionString = "GHSOM_" + datasetName + "_tau1_" + tau1 + "_tau2_" + tau2 + "_epochs_" + epochs

    writeFile("output/" + executionString + ".average", "rmse,mae")
    writeFile("output/" + executionString + ".average", rmseDiv.toString() + "," + maeDiv.toString())

  }
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
}
