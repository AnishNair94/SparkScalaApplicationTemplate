package com.acil

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import utilities.Utility

case class ProdClass(pId:String, pName:String)
case class orderClass(oId:String, oName:String)

object Main {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val envprops = props.getConfig(args(0)) //dev or prod
    val master_url = envprops.getString("execution.mode")

    val spark = Utility.getSparkSession(this.getClass.getName,master_url)

    val inputBasedir = envprops.getString("input.base.dir")
    val outputBasedir = envprops.getString("output.base.dir")

    // if the file is simple text file, We need to convert RDD to DF
    val productData = spark.read.textFile(inputBasedir+"dname").rdd
    val ordersData = spark.read.textFile(inputBasedir+"danme").rdd

    val productDF = RddToDf.getProductDF(spark,productData)
    val ordersDF = RddToDf.getProductDF(spark,ordersData)

    // if the file is Json file
    // Here the Dataframe is returned
    //val productDF = spark.read.json(inputBasedir+"dname")
    //val ordersDF = spark.read.json(inputBasedir+"danme")

    KPIFunctions.productDailyRevenue(spark,productDF,ordersDF,outputBasedir)
    
  }
}
