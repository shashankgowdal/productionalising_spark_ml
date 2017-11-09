package com.shashank.sparkml.operationalize

import java.util.Date

import com.shashank.sparkml.util.DataUtil
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
  * Created by shashank on 05/11/2017.
  */
object PredictSchemaIssue {

  def validateSchema(modelSchema:StructType, dataSchema:StructType, labelColumn:String): Unit ={
    val modelFeatureNames = modelSchema.fields.filter(_.name != labelColumn).map(_.name)
    val dataFeatureNames = dataSchema.map(_.name)
    val missingFeatureNames = modelFeatureNames.diff(dataFeatureNames)
    if(missingFeatureNames.nonEmpty)
      throw new IllegalArgumentException(s"Features ${missingFeatureNames.mkString(",")} are mandatory but is missing")

    val modelFeatureFields = modelSchema.fields.filter(_.name != labelColumn)
    modelFeatureFields.foreach(field => {
      val dataType = dataSchema(field.name).dataType
      if(dataType != field.dataType)
        throw new IllegalArgumentException(s"Feature ${field.name} was expected to be ${field.dataType.typeName} instead is of type ${dataType.typeName}")
    })
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sparkSession = SparkSession.builder.master("local").appName("example").getOrCreate()
    val trainData = DataUtil.loadCsv(sparkSession, "src/main/resources/housing.csv")
    val testData = DataUtil.loadCsv(sparkSession, "src/main/resources/customers.csv")

    val pipelineStages = DataUtil.createPipeline(trainData.schema, "median_house_value")
    val pipeline = new Pipeline()
    pipeline.setStages(pipelineStages)
    val pipelineModel = pipeline.fit(trainData)

    //validateSchema(trainData.schema, testData.schema, "median_house_value")
    val predictedData = pipelineModel.transform(testData)
    predictedData.count()

  }

}
