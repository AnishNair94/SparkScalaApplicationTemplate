package com.acil

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object RddToDf {

  def getProductDF(spark: SparkSession,prodData: RDD[String]) : DataFrame={
 import spark.implicits._

    val productDF = prodData.map(line=> line
      .split(",")
       ).
      map(line => new ProdClass(line(0),line(1))).
      toDF()

    productDF
  }

  def getOrderDF(spark: SparkSession,orderData: RDD[String]) : DataFrame={
    import spark.implicits._

    val orderDF = orderData.map(line=> line
      .split(",")
    ).
      map(line => new orderClass(line(0),line(1))).
      toDF()

    orderDF
  }
}
