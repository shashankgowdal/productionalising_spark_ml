package com.shashank.spark_ml.operationalize.stages

import org.apache.spark.ml.param.{Param, Params, StringArrayParam}
import spray.json._
import spray.json.DefaultJsonProtocol._

/**
  * Created by shashank on 04/11/2017.
  */
object PersistentParams {

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

    final def handleWithMap:Param[Map[String, String]] = new MapParam[String, String](this, "handleWithMap", "map of column name and handleWith")

    final def getHandleWithMap:Map[String, String] = $(handleWithMap)

  }

  trait HasMeanValueMap extends Params {

    final def meanValueMap:Param[Map[String, Double]] = new MapParam[String, Double](this, "meanValueMap", "map of column name and mean value")

    final def getMeanValueMap:Map[String, Double] = $(meanValueMap)

  }

  class OptionParam[T:JsonFormat](parent: Params, name: String, doc: String, isValid: Option[T] => Boolean)
    extends Param[Option[T]](parent, name, doc, isValid) {

    def this(parent: Params, name: String, doc: String) =
      this(parent, name, doc, alwaysTrue)

    override def jsonEncode(value: Option[T]): String = {
      value.toJson.compactPrint
    }

    override def jsonDecode(json: String): Option[T] = {
      json.parseJson.convertTo[Option[T]]
    }
  }

  class MapParam[K:JsonFormat, V:JsonFormat](parent: Params, name: String, doc: String, isValid: Map[K, V] => Boolean)
    extends Param[Map[K, V]](parent, name, doc, isValid) {

    import spray.json._
    import spray.json.DefaultJsonProtocol._

    def this(parent: Params, name: String, doc: String) =
      this(parent, name, doc, alwaysTrue)

    override def jsonEncode(value: Map[K, V]): String = {
      value.toJson.compactPrint
    }

    override def jsonDecode(json: String):  Map[K, V] = {
      json.parseJson.convertTo[Map[K, V]]
    }
  }

  def alwaysTrue[T]: T => Boolean = (_: T) => true

}
