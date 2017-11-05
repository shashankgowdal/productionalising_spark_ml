package com.shashank.spark_ml.operationalize

import java.util.UUID

import com.shashank.spark_ml.util.DataUtil
import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession

/**
  * Created by shashank on 05/11/2017.
  */
object ModelPersistence {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sparkSession = SparkSession.builder.master("local").appName("example").getOrCreate()
    val data = DataUtil.loadCsv(sparkSession, "src/main/resources/housing.csv")

    val pipelineStages = DataUtil.createPipeline(data.schema, "median_house_value")
    val pipeline = new Pipeline()
    pipeline.setStages(pipelineStages)
    val pipelineModel = pipeline.fit(data)

    val modelPath = s"/tmp/${UUID.randomUUID().toString}"
    pipelineModel.save(modelPath)

    val savedModel = PipelineModel.load(modelPath)
    val predictedData = savedModel.transform(data)
    predictedData.count()

  }

}
