package com.sparkghsom.main.utils

import java.io.FileWriter

import org.apache.log4j.{Level, Logger}


object ClassifySingleSplitGHSOM {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val path = args(0)
    val datasetName = args(1)
    val trainFileName = args(2)
    val testFileName = args(3)
    val tau1 = args(4).toDouble
    val tau2 = args(5).toDouble
    val epochs = args(6).toInt
   // val maxSplits = args(7).toInt

    execute(path, datasetName, trainFileName, testFileName, tau1, tau2, epochs)

  }
  //*******************************************************************************************
  def execute(path: String,
              datasetName: String,
              trainFileName: String,
              testFileName: String,
              tau1: Double,
              tau2: Double,
              epochs: Int) = {

    println("Execution...")

    var perFoldResults = Array.empty[(Double,Double,Double)]

    val (precision,recall,fmeasure) = Runner.basicExecutionGHSOM(path, datasetName, trainFileName, testFileName, tau1, tau2, epochs, 1)

    println("Average results: " + precision + "," + recall + "," + fmeasure)

    val executionString = "GHSOM_" + datasetName + "_tau1_" + tau1 + "_tau2_" + tau2 + "_epochs_" + epochs

    writeFile("output/" + executionString + ".average", "precision,recall,fmeasure")
    writeFile("output/" + executionString + ".average", precision.toString() + "," + recall.toString() + "," + fmeasure.toString())

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
