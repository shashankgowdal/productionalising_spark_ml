package com.shashank.spark_ml.data_preparation

import com.shashank.spark_ml.util.DataUtil
import com.shashank.spark_ml.util.Params._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.types.{NumericType, StructType}

/**
  * Created by shashank on 05/11/2017.
  */
class MultiColumnNullHandler (override val uid: String) extends Estimator[MultiColumnNullHandlerModel] with HasHandleWithMap {

  def setHandleWithMap(value: Map[String, String]): this.type = set(handleWithMap, value)

  override def fit(dataset: Dataset[_]): MultiColumnNullHandlerModel = {
    val meanExpressions = getHandleWithMap.flatMap{
      case (columnName, "mean") =>
        Some(expr(s"mean($columnName)").as(s"mean_$columnName"))
      case _ =>
        None
    }.toArray

    val meanValuesRow = dataset.select(meanExpressions:_*).first()

    val meanValueMap = getHandleWithMap.flatMap{
      case (columnName, "mean") =>
        val meanValue = meanValuesRow.getDouble(meanValuesRow.fieldIndex(s"mean_$columnName"))
        Some(columnName -> meanValue)
      case _ =>
        None
    }

    val nullHandlerModel = new MultiColumnNullHandlerModel(uid)
    nullHandlerModel.setMeanValueMap(meanValueMap)
    copyValues(nullHandlerModel.setParent(this))
  }

  override def copy(extra: ParamMap): Estimator[MultiColumnNullHandlerModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  def this() = this(Identifiable.randomUID("nullHandler"))

  setHandleWithMap(Map.empty[String, String])

}

class MultiColumnNullHandlerModel (override val uid: String)
  extends Model[MultiColumnNullHandlerModel] with HasHandleWithMap with HasMeanValueMap {

  def setHandleWithMap(value: Map[String, String]): this.type = set(handleWithMap, value)

  def setMeanValueMap(value: Map[String, Double]): this.type = set(meanValueMap, value)

  override def copy(extra: ParamMap): MultiColumnNullHandlerModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val nullHandledColumnExpres = dataset.columns.map(columnName => {
      val handleWith = getHandleWithMap.get(columnName)
      val columnType = dataset.schema.apply(columnName).dataType
      handleWith match {
        case Some("mean"|"zero") if Array("double", "float").contains(columnType.typeName) =>
          val meanValue = getMeanValueMap.get(columnName).getOrElse(0.0)
          coalesce(col(columnName), lit(meanValue).cast(columnType)).as(columnName)

        case Some("mean"|"zero") =>
          val meanValue = getMeanValueMap.get(columnName).getOrElse(0.0)
          coalesce(col(columnName), lit(meanValue).cast(columnType)).as(columnName)

        case Some(otherHandleWith) if columnType.isInstanceOf[NumericType] =>
          coalesce(col(columnName), lit(otherHandleWith.toDouble).cast(columnType)).as(columnName)

        case Some(otherHandleWith) =>
          coalesce(col(columnName), lit(otherHandleWith).cast(columnType)).as(columnName)

        case None =>
          col(columnName)
      }
    })

    dataset.select(nullHandledColumnExpres:_*)
  }

  override def transformSchema(schema: StructType): StructType = schema

  def this() = this(Identifiable.randomUID("multiColumnNullHandlerModel"))
}

object MultiColumnNullHandlerTrainTest {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder.master("local").appName("example").getOrCreate()
    val data = DataUtil.loadCsv(sparkSession, "src/main/resources/housing.csv")

  }
}
