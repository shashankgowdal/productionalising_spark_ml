package com.shashank.spark_ml.caching

import java.util.Date

import com.shashank.spark_ml.data_preparation._
import com.shashank.spark_ml.util.DataUtil
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
  def createPipeline(schema:StructType, labelColumn:String):Array[PipelineStage] = {
    val featureColumns = mutable.ArrayBuffer[String]()
    val nullHandleWithMap = mutable.HashMap[String, String]()
    val preprocessingStages = schema.fields.filter(_.name != labelColumn).flatMap(field => {
      field.dataType match {
        case stringType:StringType =>
          val naValuesHandler = new NaValuesHandler()
          naValuesHandler.setInputCol(field.name)

          nullHandleWithMap += (field.name -> "NA")

          val stringIndexer = new StringIndexer()
          stringIndexer.setInputCol(field.name).setOutputCol(s"${field.name}_indexed")

          val oneHotEncoder = new OneHotEncoder()
          oneHotEncoder.setInputCol(s"${field.name}_indexed").setOutputCol(s"${field.name}_encoded")

          featureColumns += (s"${field.name}_encoded")
          Array[PipelineStage](naValuesHandler, stringIndexer, oneHotEncoder)

        case numericType:NumericType =>

          nullHandleWithMap += (field.name -> "mean")

          featureColumns += (field.name)

          Array.empty[PipelineStage]

        case _ =>
          Array.empty[PipelineStage]
      }
    })


    val vectorAssembler = new VectorAssembler()
    vectorAssembler.setInputCols(featureColumns.toArray).setOutputCol("features")

    val algorithmStages = schema.apply(labelColumn).dataType match {
      case stringType:StringType =>
        val naValuesHandler = new NaValuesHandler()
        naValuesHandler.setInputCol(labelColumn)

        nullHandleWithMap += (labelColumn -> "NA")

        val stringIndexer = new StringIndexer()
        stringIndexer.setInputCol(labelColumn).setOutputCol(s"${labelColumn}_indexed")

        val decisionTreeClassifier = new DecisionTreeClassifier()
        decisionTreeClassifier.setFeaturesCol("features").setLabelCol(s"${labelColumn}_indexed")

        Array(naValuesHandler, stringIndexer, decisionTreeClassifier)

      case numericType:NumericType =>
        nullHandleWithMap += (labelColumn -> "mean")

        val castTransformer = new CastTransformer()
        castTransformer.setInputCol(labelColumn)

        val decisionTreeRegressor = new DecisionTreeRegressor()
        decisionTreeRegressor.setFeaturesCol("features").setLabelCol(labelColumn)

        Array(castTransformer, decisionTreeRegressor)
    }

    val nullHandler = new MultiColumnNullHandler()
    nullHandler.setHandleWithMap(nullHandleWithMap.toMap)

    (Array(nullHandler) ++ preprocessingStages :+ vectorAssembler) ++ algorithmStages
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val sparkSession = SparkSession.builder.master("local").appName("example").getOrCreate()
    val housingData = DataUtil.loadCsv(sparkSession, "src/main/resources/housing.csv")

    val data = (1 to 100).foldRight(housingData)((index, interData) => interData.union(housingData.selectExpr(housingData.columns:_*)))

    //data.cache()

    val sampledDatas = data.randomSplit(Array(0.6, 0.8))
    val trainData = sampledDatas(0)
    val testData = sampledDatas(1)

    trainData.cache()
    testData.cache()

    val startTime = new Date()
    val pipelineStages = createPipeline(data.schema, "median_house_value")
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

