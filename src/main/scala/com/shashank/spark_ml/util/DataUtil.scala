package com.shashank.spark_ml.util

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by shashank on 04/11/2017.
  */
object DataUtil {

  def loadCsv(sparkSession:SparkSession, filePath:String):DataFrame = {
    sparkSession.read.options(Map("inferSchema"->"true","header" -> "true")).csv(filePath)
  }
}
