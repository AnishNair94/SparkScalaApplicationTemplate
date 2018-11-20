name := "ScalaApplicationTemplate"

version := "0.1"

scalaVersion := "2.11.12"

val spark_version = "2.3.1"

libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % spark_version
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % spark_version
