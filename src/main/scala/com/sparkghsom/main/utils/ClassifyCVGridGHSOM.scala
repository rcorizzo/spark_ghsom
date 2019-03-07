package com.sparkghsom.main.utils

object ClassifyCVGridGHSOM {

  def main(args: Array[String]): Unit = {

    val path = args(0)
    val datasetName = args(1)
    val epochs = args(2).toInt
    val maxSplits = args(3).toInt

    println("Execution...")

    execute(path, datasetName, epochs, maxSplits)

  }
  //*******************************************************************************************
  def execute(path: String,
              datasetName: String,
              epochs: Int,
              maxSplits: Int) = {

    for (t1 <- Array(0.7, 0.5, 0.3)) {
      for (t2 <- Array(0.7, 0.5, 0.3)) {

        println("Executing CV with t1=" + t1 + " , t2=" + t2)

        var foldResults = Array.empty[(Double, Double, Double)]

        for (split <- 1 to maxSplits) {
          val trainFileName = "/" + datasetName + "_train_" + split + "/part-00000"
          val testFileName = "/" + datasetName + "_test_" + split + "/part-00000"
          val (precision, recall, fmeasure) = Runner.basicExecutionGHSOM(path, datasetName, trainFileName, testFileName, t1, t2, epochs, split)
          foldResults :+= (precision, recall, fmeasure)
        }

        val precDiv = foldResults.map(x => x._1).sum / foldResults.size
        val recDiv = foldResults.map(x => x._2).sum / foldResults.size
        val fMeasDiv = foldResults.map(x => x._3).sum / foldResults.size
        println("Average results with t1:" + t1 + " t2: " + t2 + ": " + precDiv + "," + recDiv + "," + fMeasDiv)

      }
    }
  }
}
