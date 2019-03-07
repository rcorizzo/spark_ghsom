package com.sparkghsom.main.utils

import org.apache.log4j.{Level, Logger}

object ClassifyFromIDE {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    // **************************
    // Datasets
    // **************************

    val path = "/Users/hydergine/git/mr_ghsom/datasets/"

     val datasetName =
    // "anneal"
    // "KDDCUP99"
    // "contraceptive"
    // "credit"
    // "hepatitis"
    // "yeast"
    //   "car"
    //"census_income"
    //  "dblp"

    // REGRESSION NEW
    //"energy_efficiency_Y1"
    //"energy_efficiency_Y2"
    // "forest"
   //    "facebook_metrics"
    "automotive"

    // **************************
    // Run GHSOM CV Single Config
    // **************************

    // GHSOM Params

    val tau1 = 0.3
    val tau2 = 0.3
    val epochs = 30
    val maxSplits = 5

    // *** GHSOM CLASSIFICATION ***
    //ClassifyCVOneConfigGHSOM.execute(path, datasetName, tau1, tau2, epochs, maxSplits)

    // *** GHSOM REGRESSION ***
    RegressionCV.execute(path, datasetName, tau1, tau2, epochs, maxSplits)

    // **************************
    // Run GHSOM with Single train - test split
    // **************************

    // GHSOM doesn't need attribute names in csv (head)
    // Competitors need attribute names in csv

    // GHSOM Params
    /*
    val tau1 = 0.3
    val tau2 = 0.7
    val epochs = 15
    val split = 1

    val trainFileName =
      "census_income_with_id.data"
      //"kddcup_id_full_train"
    val testFileName =
      "census_income_with_id.test"
      //"kddcup_id_full_test_"

    val (precision,recall,fmeasure) = Runner.basicExecutionGHSOM(path, datasetName, trainFileName, testFileName, tau1, tau2, epochs, split)
*/
    // **************************
    // Run Competitors CV
    // **************************

    val algorithm =
    // "LR"  // Logistic Regression
     "DT"  // Decision Trees
    // "NB"  // Naive Bayes
    //"MLP" // Multi Layer Perceptron

    //  "ISOREG"
    //  "LINREG_0.01"
    //"LINREG_0.075"
    //"LINREG_0.15"
    //"LINREG_0.3"
    //"LINREG_0.45"

    //"SVR_linear"
    //"SVR_polynomial"
    //"SVR_rbf"
    //"SVR_sigmoid"

    // *** CLASSIFICATION ***
    //   ClassifyCVCompetitors.execute(path, datasetName, maxSplits, algorithm)

    // *** REGRESSION ***
    //RegressionCVCompetitors.execute(path, datasetName, maxSplits, algorithm)

    // **************************
    // Competitors with Single train - test split
    // **************************

        //val algorithm = "NB"
        //val split = 1
        //val trainFileName =
          //"census_income_with_id.data-head"
          //"/kddcup_id_full_train"
        //val testFileName =
          //"/kddcup_id_full_test_"
          //"census_income_with_id.test-head"

    //val (precision,recall,fmeasure) = Runner.basicExecutionCompetitors(path, datasetName, trainFileName, testFileName, algorithm, split)



  }
}
