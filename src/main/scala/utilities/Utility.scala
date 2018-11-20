package utilities

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Utility {

  def getSparkContext(appName: String,master_url: String): SparkContext={

  val sparkConf = new SparkConf().
                      setAppName(appName).
                      setMaster(master_url)
  val sparkContext = SparkContext(sparkConf)
    sparkContext
  }

  def getSparkSession( appName: String, master_url: String): SparkSession={
    val spark = SparkSession.
                builder().
                appName(appName).
                master(master_url).
                getOrCreate()
    spark
  }

}
