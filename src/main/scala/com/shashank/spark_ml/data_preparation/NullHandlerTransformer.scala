package com.shashank.spark_ml.data_preparation

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, Params, StringArrayParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import com.shashank.spark_ml.util.Params._
import com.shashank.spark_ml.util.DataUtil

/**
  * Created by shashank on 04/11/2017.
  */
class NullHandlerTransformer(override val uid: String) extends Transformer with HasInputCol with HasHandleWith{

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setHandleWith(value: String): this.type = set(handleWith, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val columnName = getInputCol
    getHandleWith match {
      case "zero" =>
        dataset.na.fill(0, Seq(columnName))
      case "mean" =>
        val meanValue = dataset.selectExpr(s"mean($columnName)").first().getDouble(0)
        dataset.na.fill(meanValue, Seq(columnName))
      case "droprows" =>
        dataset.na.drop(Seq(columnName))
      case anyOtherValue =>
        dataset.na.fill(anyOtherValue, Seq(columnName))
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  def this() = this(Identifiable.randomUID("nullHandler"))

}


object NullHandlerTransformerTest {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder.master("local").appName("example").getOrCreate()
    val data = DataUtil.loadCsv(sparkSession, "src/main/resources/housing.csv")

    println("Rows having nulls in column total_bedrooms " + data.filter("isnull(total_bedrooms)").count())

    val nullHandler = new NullHandlerTransformer()
    nullHandler.setInputCol("total_bedrooms")
    nullHandler.setHandleWith("mean")
    val totalBedsNullHandledData = nullHandler.transform(data)

    println(s"Rows having nulls in column total_bedrooms ${totalBedsNullHandledData.filter("isnull(total_bedrooms)").count()} after null handling\n\n\n\n")

    println("Rows having nulls in column state " + data.filter("isnull(state)").count())
    totalBedsNullHandledData.groupBy("state").count().show()

    val nullHandler2 = new NullHandlerTransformer()
    nullHandler2.setInputCol("state")
    nullHandler2.setHandleWith("NA")
    val stateNullHandledData = nullHandler2.transform(totalBedsNullHandledData)

    println(s"Rows having nulls in column state ${stateNullHandledData.filter("isnull(state)").count()} after null handling")
    stateNullHandledData.groupBy("state").count().show()
  }
}


object NullHandlerTransformerTrainTest {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder.master("local").appName("example").getOrCreate()
    val data = DataUtil.loadCsv(sparkSession, "src/main/resources/housing.csv")

    val dataArray = data.randomSplit(Array(0.6, 0.2, 0.2), seed = 20000)
    val trainData = dataArray(0)
    val testData1 = dataArray(1)
    val testData2 = dataArray(2).filter("isnull(total_bedrooms)")

    val nullHandler = new NullHandlerTransformer()
    nullHandler.setInputCol("total_bedrooms")
    nullHandler.setHandleWith("mean")

    val nullHanledTrainData = nullHandler.transform(trainData)
    println(s"Train Data")
    println(s"Nulls before: ${trainData.filter("isnull(total_bedrooms)").count()}")
    println(s"Nulls after: ${nullHanledTrainData.filter("isnull(total_bedrooms)").count()}")

    val nullHandledTestData1 = nullHandler.transform(testData1)
    println(s"Test Data - 1")
    println(s"Nulls before: ${testData1.filter("isnull(total_bedrooms)").count()}")
    println(s"Nulls after: ${nullHandledTestData1.filter("isnull(total_bedrooms)").count()}")

    val nullHandledTestData2 = nullHandler.transform(testData2)
    println(s"Test Data - 2")
    println(s"Nulls before: ${testData2.filter("isnull(total_bedrooms)").count()}")
    println(s"Nulls after: ${nullHandledTestData2.filter("isnull(total_bedrooms)").count()}")

  }
}
