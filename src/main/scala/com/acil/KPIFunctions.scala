package com.acil

import org.apache.spark.sql.{DataFrame, SparkSession}

object KPIFunctions {

  def productDailyRevenue(spark:SparkSession, productDF: DataFrame,orderDF:DataFrame,outdir: String)={
    // All  analysis part
    productDF.write.format("csv").save(outdir)
  }

}
