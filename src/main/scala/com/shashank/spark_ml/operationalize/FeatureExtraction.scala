package com.shashank.spark_ml.operationalize

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession

/**
  * Created by shashank on 05/11/2017.
  */
object FeatureExtraction {

  def getFeatures(pipelineModel:PipelineModel):Array[String] = {
    val vectorAssembler = pipelineModel.stages.filter(_.isInstanceOf[VectorAssembler]).headOption.getOrElse(throw new IllegalArgumentException("Invalid model")).asInstanceOf[VectorAssembler]
    val featureNames = vectorAssembler.getInputCols

    featureNames.flatMap(featureName => {
      val oneHotEncoder = pipelineModel.stages.filter(_.isInstanceOf[OneHotEncoder]).map(_.asInstanceOf[OneHotEncoder]).find(_.getOutputCol == featureName)
      val oneHotEncoderInputCol = oneHotEncoder.map(_.getInputCol).getOrElse(featureName)

      val stringIndexer = pipelineModel.stages.filter(_.isInstanceOf[StringIndexerModel]).map(_.asInstanceOf[StringIndexerModel]).find(_.getOutputCol == oneHotEncoderInputCol)
      val stringIndexerInput = stringIndexer.map(_.getInputCol).getOrElse(featureName)

      oneHotEncoder.map(encoder => {
        val labelValues = stringIndexer.getOrElse(throw new IllegalArgumentException("Invalid model")).labels
        labelValues.map(label => s"$stringIndexerInput-$label")
      }).getOrElse(Array(stringIndexerInput))
    })
  }

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder.master("local").appName("example").getOrCreate()

    val pipelineModel = PipelineModel.load("/tmp/58dd2148-66b5-4fd4-8f11-173cb362863f")
    val featureNames = getFeatures(pipelineModel)
    featureNames.foreach(println)
  }

}
