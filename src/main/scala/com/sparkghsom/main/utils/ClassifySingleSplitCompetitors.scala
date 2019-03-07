package com.sparkghsom.main.utils

import java.io.FileWriter

import org.apache.log4j.{Level, Logger}

object ClassifySingleSplitCompetitors {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val path = args(0)
    val datasetName = args(1)
    val trainFileName = args(2)
    val testFileName = args(3)
    val algorithm = args(4)

    execute(path, datasetName, trainFileName, testFileName, algorithm)

  }
  //*******************************************************************************************
  def execute(path: String,
              datasetName: String,
              trainFileName: String,
              testFileName: String,
              algorithm: String) = {

    println("Execution...")

    var perFoldResults = Array.empty[(Double,Double,Double)]

    val (precision,recall,fmeasure) = Runner.basicExecutionCompetitors(path, datasetName, trainFileName, testFileName, algorithm, 1)

    println("Average results: " + precision + "," + recall + "," + fmeasure)

    val executionString = algorithm + "_" + datasetName

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
