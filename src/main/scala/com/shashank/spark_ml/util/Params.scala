package com.shashank.spark_ml.util

import org.apache.spark.ml.param.{Param, Params, StringArrayParam}

/**
  * Created by shashank on 04/11/2017.
  */
object Params {

  trait HasInputCol extends Params {

    final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

    final def getInputCol: String = $(inputCol)

  }

  trait HasHandleWith extends Params {

    final val handleWith: Param[String] = new Param[String](this, "handleWith", "handle with parameter")

    final def getHandleWith: String = $(handleWith)

  }

  trait HasOptionalMean extends Params {

    final val optionalMean: Param[Option[Double]] = new Param[Option[Double]](this, "optionalMean", "mean value")

    final def getOptionalMean:Option[Double] = $(optionalMean)
  }

  trait HasNaValues extends Params {

    final val naValues:StringArrayParam = new StringArrayParam(this, "naValues", "array of na values")

    final def getNaValues:Array[String] = $(naValues)

  }

  trait HasHandleWithMap extends Params {

    final def handleWithMap:Param[Map[String, String]] = new Param[Map[String, String]](this, "handleWithMap", "map of column name and handleWith")

    final def getHandleWithMap:Map[String, String] = $(handleWithMap)

  }

  trait HasMeanValueMap extends Params {

    final def meanValueMap:Param[Map[String, Double]] = new Param[Map[String, Double]](this, "meanValueMap", "map of column name and mean value")

    final def getMeanValueMap:Map[String, Double] = $(meanValueMap)

  }


}
