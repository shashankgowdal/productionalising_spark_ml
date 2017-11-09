package com.shashank.sparkml.caching

import java.util.Date

import com.shashank.sparkml.datapreparation._
import com.shashank.sparkml.util.DataUtil
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{NumericType, StringType, StructType}

import scala.collection.mutable

/**
  * Created by shashank on 05/11/2017.
  */
object PipelineWithSampling {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sparkSession = SparkSession.builder.master("local").appName("example").getOrCreate()
    val housingData = DataUtil.loadCsv(sparkSession, "src/main/resources/housing.csv")

    val data = (1 to 100).foldRight(housingData)((index, interData) => interData.union(housingData.selectExpr(housingData.columns:_*)))

    data.cache()



    val sampledDatas = data.randomSplit(Array(0.6, 0.8))
    val trainData = sampledDatas(0)
    val testData = sampledDatas(1)

    //trainData.cache()
    //testData.cache()

    val startTime = new Date()
    val pipelineStages = DataUtil.createPipeline(data.schema, "median_house_value")
    val pipeline = new Pipeline()
    pipeline.setStages(pipelineStages)
    val pipelineModel = pipeline.fit(trainData)

    val predictedData = pipelineModel.transform(testData)
    predictedData.count()
    val endTime = new Date()
    //transformedData.explain(true)

    println(s"Time taken for running regression on a ${data.columns.length} column dataset is ${(endTime.getTime - startTime.getTime)/1000}s")

    Thread.sleep(100000)

    /*
    * Caching data before RandomSplit
    *   45s
    *
    * Caching data after RandomSplit
    *   39s (13% less time)
    * */

  }

}

