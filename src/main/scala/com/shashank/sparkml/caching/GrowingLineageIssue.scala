package com.shashank.sparkml.caching

import com.shashank.sparkml.datapreparation.NullHandlerEstimator
import com.shashank.sparkml.util.DataUtil
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
  * Created by shashank on 10/11/2017.
  */
object GrowingLineageIssue {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sparkSession = SparkSession.builder.master("local").appName("example").getOrCreate()
    val loadedDf = DataUtil.loadCsv(sparkSession, "src/main/resources/housing.csv")

    val multiColumnDf = (0 to 250).foldRight(loadedDf)((i, df) ⇒ {
      val columnName = "housing_median_age"
      println(s"Here $i")
      df.withColumn(s"$columnName$i", col(columnName))
    })

    multiColumnDf.cache()
    multiColumnDf.count()

    val startTime = System.currentTimeMillis()

    (0 to 250).foldRight(multiColumnDf)((i, df) ⇒ {
      val nullHandler = new NullHandlerEstimator()
      val columnName = "housing_median_age"
      nullHandler.setInputCol(s"$columnName$i")
      nullHandler.setHandleWith("mean")
      nullHandler.fit(df).transform(df)
    })
    val endTime = System.currentTimeMillis()
    println("total time is " + (endTime - startTime) + " in milli seconds")

    Thread.sleep(100000)

  }

}
