package com.cs.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame

trait ParquetUtils {

  def readTextFile(sqlContext: SQLContext,path: String, schema: StructType, delimiter: String): DataFrame = {
       sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .schema(schema)
      .option("delimiter", delimiter)
      .load(path)
  }
  def writeToParquet(df: DataFrame, path: String): Unit = {
    df.write.parquet(path)
  }
  def readFromParquet(sqlContext: SQLContext, path: String): DataFrame = {
    sqlContext.read.parquet(path)
  }
}