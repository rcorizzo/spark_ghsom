package com.sparkghsom.main.globals

import java.io.FileInputStream

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, DataFrame, SQLContext, SaveMode}

object SparkConfig {

  val (appname, master) =
    try {
      val prop = new java.util.Properties()
      prop.load(new FileInputStream("config.properties"))
      (
        prop.getProperty("appname"),
        prop.getProperty("master")
      )
    } catch { case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
    }

  val sparkSession: SparkSession = SparkSession.builder
    .master(master)
    .appName(appname)
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  val sc : SparkContext = sparkSession.sparkContext
  val sqlC : SQLContext = sparkSession.sqlContext

  def getSparkSession = {
    sparkSession
  }

  def getSqlContext: SQLContext = {
    sqlC
  }

  def getSparkContext : SparkContext = {
    sc
  }
}