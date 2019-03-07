package com.sparkghsom.main.utils

import java.io.{FileInputStream, FileWriter, File}

import com.sparkghsom.main.globals.SparkConfig
import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.commons.io.FileUtils

object SplitGenerator {

  val sc = SparkConfig.getSparkContext

  def main(args : Array[String]) {

    // execute on csv file without features preamble

    val input = "/Users/hydergine/git/mr_ghsom/datasets/burlington_daily/burlington_daily.csv"
    val dataset = sc.textFile(input)    // id + label
    val numFolds = 5
    val seed = 3
    val trainOut = "burlington_daily_train"
    val testOut = "burlington_daily_test"
    val preamble = "id,idplant,lat,lon,anno,data,day,hour0,temperature0,srs_irrad_avg_50,dewpoint0,humidity0,windspeed0,windbearing0,cloudcover0,pressure0,icon0,precipitationtype0,precipitationintensity0,precipitationprobability0,precipitationaccumulation0,hour1,temperature1,srs_irrad_avg_51,dewpoint1,humidity1,windspeed1,windbearing1,cloudcover1,pressure1,icon1,precipitationtype1,precipitationintensity1,precipitationprobability1,precipitationaccumulation1,hour2,temperature2,srs_irrad_avg_52,dewpoint2,humidity2,windspeed2,windbearing2,cloudcover2,pressure2,icon2,precipitationtype2,precipitationintensity2,precipitationprobability2,precipitationaccumulation2,hour3,temperature3,srs_irrad_avg_53,dewpoint3,humidity3,windspeed3,windbearing3,cloudcover3,pressure3,icon3,precipitationtype3,precipitationintensity3,precipitationprobability3,precipitationaccumulation3,hour4,temperature4,srs_irrad_avg_54,dewpoint4,humidity4,windspeed4,windbearing4,cloudcover4,pressure4,icon4,precipitationtype4,precipitationintensity4,precipitationprobability4,precipitationaccumulation4,hour5,temperature5,srs_irrad_avg_55,dewpoint5,humidity5,windspeed5,windbearing5,cloudcover5,pressure5,icon5,precipitationtype5,precipitationintensity5,precipitationprobability5,precipitationaccumulation5,hour6,temperature6,srs_irrad_avg_56,dewpoint6,humidity6,windspeed6,windbearing6,cloudcover6,pressure6,icon6,precipitationtype6,precipitationintensity6,precipitationprobability6,precipitationaccumulation6,hour7,temperature7,srs_irrad_avg_57,dewpoint7,humidity7,windspeed7,windbearing7,cloudcover7,pressure7,icon7,precipitationtype7,precipitationintensity7,precipitationprobability7,precipitationaccumulation7,hour8,temperature8,srs_irrad_avg_58,dewpoint8,humidity8,windspeed8,windbearing8,cloudcover8,pressure8,icon8,precipitationtype8,precipitationintensity8,precipitationprobability8,precipitationaccumulation8,hour9,temperature9,srs_irrad_avg_59,dewpoint9,humidity9,windspeed9,windbearing9,cloudcover9,pressure9,icon9,precipitationtype9,precipitationintensity9,precipitationprobability9,precipitationaccumulation9,hour10,temperature10,srs_irrad_avg_510,dewpoint10,humidity10,windspeed10,windbearing10,cloudcover10,pressure10,icon10,precipitationtype10,precipitationintensity10,precipitationprobability10,precipitationaccumulation10,hour11,temperature11,srs_irrad_avg_511,dewpoint11,humidity11,windspeed11,windbearing11,cloudcover11,pressure11,icon11,precipitationtype11,precipitationintensity11,precipitationprobability11,precipitationaccumulation11,hour12,temperature12,srs_irrad_avg_512,dewpoint12,humidity12,windspeed12,windbearing12,cloudcover12,pressure12,icon12,precipitationtype12,precipitationintensity12,precipitationprobability12,precipitationaccumulation12,hour13,temperature13,srs_irrad_avg_513,dewpoint13,humidity13,windspeed13,windbearing13,cloudcover13,pressure13,icon13,precipitationtype13,precipitationintensity13,precipitationprobability13,precipitationaccumulation13,hour14,temperature14,srs_irrad_avg_514,dewpoint14,humidity14,windspeed14,windbearing14,cloudcover14,pressure14,icon14,precipitationtype14,precipitationintensity14,precipitationprobability14,precipitationaccumulation14,hour15,temperature15,srs_irrad_avg_515,dewpoint15,humidity15,windspeed15,windbearing15,cloudcover15,pressure15,icon15,precipitationtype15,precipitationintensity15,precipitationprobability15,precipitationaccumulation15,hour16,temperature16,srs_irrad_avg_516,dewpoint16,humidity16,windspeed16,windbearing16,cloudcover16,pressure16,icon16,precipitationtype16,precipitationintensity16,precipitationprobability16,precipitationaccumulation16,hour17,temperature17,srs_irrad_avg_517,dewpoint17,humidity17,windspeed17,windbearing17,cloudcover17,pressure17,icon17,precipitationtype17,precipitationintensity17,precipitationprobability17,precipitationaccumulation17,hour18,temperature18,srs_irrad_avg_518,dewpoint18,humidity18,windspeed18,windbearing18,cloudcover18,pressure18,icon18,precipitationtype18,precipitationintensity18,precipitationprobability18,precipitationaccumulation18,hour19,temperature19,srs_irrad_avg_519,dewpoint19,humidity19,windspeed19,windbearing19,cloudcover19,pressure19,icon19,precipitationtype19,precipitationintensity19,precipitationprobability19,precipitationaccumulation19,hour20,temperature20,srs_irrad_avg_520,dewpoint20,humidity20,windspeed20,windbearing20,cloudcover20,pressure20,icon20,precipitationtype20,precipitationintensity20,precipitationprobability20,precipitationaccumulation20,hour21,temperature21,srs_irrad_avg_521,dewpoint21,humidity21,windspeed21,windbearing21,cloudcover21,pressure21,icon21,precipitationtype21,precipitationintensity21,precipitationprobability21,precipitationaccumulation21,hour22,temperature22,srs_irrad_avg_522,dewpoint22,humidity22,windspeed22,windbearing22,cloudcover22,pressure22,icon22,precipitationtype22,precipitationintensity22,precipitationprobability22,precipitationaccumulation22,hour23,temperature23,srs_irrad_avg_523,dewpoint23,humidity23,windspeed23,windbearing23,cloudcover23,pressure23,icon23,precipitationtype23,precipitationintensity23,precipitationprobability23,precipitationaccumulation23,mtr_ac_power_delv_avg0,mtr_ac_power_delv_avg1,mtr_ac_power_delv_avg2,mtr_ac_power_delv_avg3,mtr_ac_power_delv_avg4,mtr_ac_power_delv_avg5,mtr_ac_power_delv_avg6,mtr_ac_power_delv_avg7,mtr_ac_power_delv_avg8,mtr_ac_power_delv_avg9,mtr_ac_power_delv_avg10,mtr_ac_power_delv_avg11,mtr_ac_power_delv_avg12,mtr_ac_power_delv_avg13,mtr_ac_power_delv_avg14,mtr_ac_power_delv_avg15,mtr_ac_power_delv_avg16,mtr_ac_power_delv_avg17,mtr_ac_power_delv_avg18,mtr_ac_power_delv_avg19,mtr_ac_power_delv_avg20,mtr_ac_power_delv_avg21,mtr_ac_power_delv_avg22,mtr_ac_power_delv_avg23"
    generateKfold(dataset, numFolds, seed, trainOut, testOut, preamble)

    val path = "/Users/hydergine/git/mr_ghsom/"
    copyCSV(path+trainOut, path+testOut, numFolds)
  }

  //************************************************************************
  def copyCSV(trainOutputFileName: String,
              testOutputFileName: String,
              numFolds: Int) = {

    var count = 1

    for (count <- 1 to numFolds) {

      FileUtils.copyFile(new File(trainOutputFileName + "_" + count + "/part-00000-head"), new File(trainOutputFileName + "_" + count + "/part-00000-head.csv"))
      FileUtils.copyFile(new File(testOutputFileName + "_" + count + "/part-00000-head"), new File(testOutputFileName + "_" + count + "/part-00000-head.csv"))
    }

  }
  //************************************************************************
  def generateKfold(dataset: RDD[String],
                    numFolds: Int,
                    seed: Int,
                    trainOutputFileName: String,
                    testOutputFileName: String,
                    preamble: String): Unit = {
    val splits = MLUtils.kFold(dataset, numFolds, seed)
    var count = 1
    splits.zipWithIndex.foreach {
      case ((training, test), splitIndex) =>

        training.coalesce(1).saveAsTextFile(trainOutputFileName + "_" + count)
        test.coalesce(1).saveAsTextFile(testOutputFileName + "_" + count)

        writeFile(trainOutputFileName + "_" + count + "/part-00000-head", preamble)
        writeFile(testOutputFileName + "_" + count + "/part-00000-head", preamble)

        training.collect.foreach(s => {
          writeFile(trainOutputFileName + "_" + count + "/part-00000-head", s)
        })

        test.collect.foreach(s => {
          writeFile(testOutputFileName + "_" + count + "/part-00000-head", s)
        })

        FileUtils.deleteQuietly(new File(testOutputFileName + "_" + count + "/.part-00000.crc"))
        FileUtils.deleteQuietly(new File(trainOutputFileName + "_" + count + "/.part-00000.crc"))

        count+=1
    }
  }
  //************************************************************************************************
  def writeFile(fileName: String, content: String): Unit = {
    val fw = new FileWriter(fileName, true)
    try {
      fw.write(content + "\r\n")
    }
    fw.close()
  }
}
