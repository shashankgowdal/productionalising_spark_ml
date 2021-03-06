name := "ProductionalisingSparkML"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % "2.2.0_mod" from "https://github.com/shashankgowdal/productionalising_spark_ml/raw/master/spark-mllib_2.11-2.2.0.jar",
  "org.apache.spark" %% "spark-mllib-local" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.4"
)
    
