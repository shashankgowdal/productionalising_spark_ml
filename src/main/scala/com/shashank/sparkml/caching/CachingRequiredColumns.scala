package com.shashank.sparkml.caching

import com.shashank.sparkml.util.DataUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by shashank on 09/11/2017.
  */
object CachingRequiredColumns {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sparkSession = SparkSession.builder.master("local").appName("example").getOrCreate()
    val housingData = DataUtil.loadCsv(sparkSession, "src/main/resources/housing.csv")

    val data = (1 to 100).foldRight(housingData)((index, interData) => interData.union(housingData.selectExpr(housingData.columns: _*)))

    data.cache().count()

    data.select("housing_median_age","total_rooms","total_bedrooms").cache().count()

    Thread.sleep(100000)
  }

}
