package com.shashank.sparkml.datapreparation

import com.shashank.sparkml.util.Params.HasInputCol
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Created by shashank on 05/11/2017.
  */
class CastTransformer(override val uid: String) extends Transformer with HasInputCol {
  override def transform(dataset: Dataset[_]): DataFrame = {
    val castExprs = dataset.columns.map({
      case columnName if columnName == getInputCol =>
        col(columnName).cast(DoubleType).as(columnName)
      case columnName =>
        col(columnName)
    })

    dataset.select(castExprs:_*)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val outputFields = schema.fields.map(field => {
      if (field.name == getInputCol)
        field.copy(dataType = DoubleType)
      else
        field
    })
    StructType(outputFields)
  }

  def setInputCol(value: String): this.type = set(inputCol, value)

  def this() = this(Identifiable.randomUID("castTransformer"))

}
