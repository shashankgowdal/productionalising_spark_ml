package com.shashank.sparkml.datapreparation

import com.shashank.sparkml.util.DataUtil
import com.shashank.sparkml.util.Params.{HasHandleWith, HasInputCol, HasNaValues}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
  * Created by shashank on 04/11/2017.
  */
class NaValuesHandler(override val uid: String) extends Transformer with HasInputCol with HasNaValues{

  setNaValues(Array("", "null"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setNaValues(values: Array[String]): this.type  = set(naValues, values)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val naValueReplaceUdf = udf({ (value:String) =>
      if(getNaValues.contains(value)) null
      else if(value != null && value.trim.isEmpty) null
      else value
    })

    val naValueHandledExprs = dataset.columns.map(columnName => {
      if(getInputCol == columnName) naValueReplaceUdf(col(columnName)).as(columnName)
      else col(columnName)
    })

    dataset.select(naValueHandledExprs:_*)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  def this() = this(Identifiable.randomUID("naValuesHandler"))

}

object NaValuesHandlerTest {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder.master("local").appName("example").getOrCreate()
    val data = DataUtil.loadCsv(sparkSession, "src/main/resources/customers.csv")
    data.show()

    val naValuesHandler = new NaValuesHandler()
    naValuesHandler.setInputCol("customerName")

    naValuesHandler.transform(data).show()
  }
}
