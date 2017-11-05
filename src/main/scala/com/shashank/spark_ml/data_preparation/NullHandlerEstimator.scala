package com.shashank.spark_ml.data_preparation

import com.shashank.spark_ml.util.DataUtil
import com.shashank.spark_ml.util.Params._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

/**
  * Created by shashank on 04/11/2017.
  */
class NullHandlerEstimator(override val uid: String) extends Estimator[NullHandlerEstimatorModel] with HasInputCol with HasHandleWith {

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setHandleWith(value: String): this.type = set(handleWith, value)

  override def fit(dataset: Dataset[_]): NullHandlerEstimatorModel = {
    val meanOption = getHandleWith match {
      case "mean" =>
        val meanValue = dataset.selectExpr(s"mean($getInputCol)").first().getDouble(0)
        Some(meanValue)
      case _ => None
    }
    val nullHandlerModel = new NullHandlerEstimatorModel(uid)
    nullHandlerModel.setMean(meanOption)
    copyValues(nullHandlerModel.setParent(this))
  }

  override def copy(extra: ParamMap): Estimator[NullHandlerEstimatorModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  def this() = this(Identifiable.randomUID("nullHandler"))

}

class NullHandlerEstimatorModel (override val uid: String)
  extends Model[NullHandlerEstimatorModel] with HasOptionalMean with HasInputCol with HasHandleWith {

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setHandleWith(value: String): this.type = set(handleWith, value)

  def setMean(value: Option[Double]): this.type = set(optionalMean, value)

  override def copy(extra: ParamMap): NullHandlerEstimatorModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val columnName = getInputCol
    getHandleWith match {
      case "zero" =>
        dataset.na.fill(0, Seq(columnName))
      case "mean" =>
        val meanValue = getOptionalMean.getOrElse(0.0)
        dataset.na.fill(meanValue, Seq(columnName))
      case "droprows" =>
        dataset.na.drop(Seq(columnName))
      case anyOtherValue =>
        dataset.na.fill(anyOtherValue, Seq(columnName))
    }
  }

  override def transformSchema(schema: StructType): StructType = schema

  def this() = this(Identifiable.randomUID("nullHandlerModel"))
}

object NullHandlerEstimatorTrainTest {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder.master("local").appName("example").getOrCreate()
    val data = DataUtil.loadCsv(sparkSession, "src/main/resources/housing.csv")

    val dataArray = data.randomSplit(Array(0.6, 0.2, 0.2), seed = 20000)
    val trainData = dataArray(0)
    val testData1 = dataArray(1)
    val testData2 = dataArray(2).filter("isnull(total_bedrooms)")

    val nullHandler = new NullHandlerEstimator()
    nullHandler.setInputCol("total_bedrooms")
    nullHandler.setHandleWith("mean")
    val nullHandlerModel = nullHandler.fit(trainData)

    val nullHanledTrainData = nullHandlerModel.transform(trainData)
    println(s"Train Data")
    println(s"Nulls before: ${trainData.filter("isnull(total_bedrooms)").count()}")
    println(s"Nulls after: ${nullHanledTrainData.filter("isnull(total_bedrooms)").count()}")

    val nullHandledTestData1 = nullHandlerModel.transform(testData1)
    println(s"Test Data - 1")
    println(s"Nulls before: ${testData1.filter("isnull(total_bedrooms)").count()}")
    println(s"Nulls after: ${nullHandledTestData1.filter("isnull(total_bedrooms)").count()}")

    val nullHandledTestData2 = nullHandlerModel.transform(testData2)
    println(s"Test Data - 2")
    println(s"Nulls before: ${testData2.filter("isnull(total_bedrooms)").count()}")
    println(s"Nulls after: ${nullHandledTestData2.filter("isnull(total_bedrooms)").count()}")

    println(s"Mean value ${nullHandlerModel.getOptionalMean}")

  }
}

