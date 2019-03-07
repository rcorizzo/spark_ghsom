package com.sparkghsom.main.utils

import java.io.FileWriter

import org.apache.log4j.{Level, Logger}

object ClassifyCVOneConfigGHSOM {

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

    var perFoldResults = Array.empty[(Double,Double,Double)]

    // 5 FOLD CV FOR NB, LR, DT or GHSOM with fixed T1,T2, params

    for(split <- 1 to maxSplits) {
      val trainFileName = "/" + datasetName + "_train_" + split + "/part-00000"
      val testFileName = "/" + datasetName + "_test_" + split + "/part-00000"
      val (precision,recall,fmeasure) = Runner.basicExecutionGHSOM(path, datasetName, trainFileName, testFileName, tau1, tau2, epochs, split)
      perFoldResults :+= (precision,recall,fmeasure)
    }

    val precDiv = perFoldResults.map(x => x._1).sum/perFoldResults.size
    val recDiv = perFoldResults.map(x => x._2).sum/perFoldResults.size
    val fMeasDiv = perFoldResults.map(x => x._3).sum/perFoldResults.size

    println("Average results: " + precDiv + "," + recDiv + "," + fMeasDiv)

    val executionString = "GHSOM_" + datasetName + "_tau1_" + tau1 + "_tau2_" + tau2 + "_epochs_" + epochs

    writeFile("output/" + executionString + ".average", "precision,recall,fmeasure")
    writeFile("output/" + executionString + ".average", precDiv.toString() + "," + recDiv.toString() + "," + fMeasDiv.toString())

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
