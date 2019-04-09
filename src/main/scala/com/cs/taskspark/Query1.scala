package com.cs.taskspark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.count

import com.cs.utils.ParquetUtils

object Query1 extends App with ParquetUtils {
  /*
   * Query 1.  For each year and month , print the count of 
   * movies having rating greater than equal to 4
   */
  val conf = new SparkConf().setMaster("local[2]").setAppName("Movie-App")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val parquetfilespath = "G:\\project\\spark\\movie_app\\src\\main\\resources\\"

  val genres = "genres"
  val genres_movies = "genres_movies"
  val movies = "movies"
  val occupations = "occupations"
  val ratings = "ratings"
  val users = "users"

  val df_movies = readFromParquet(sqlContext, parquetfilespath + movies)
  val df_ratings = readFromParquet(sqlContext, parquetfilespath + ratings).where(col("rating") >= lit(4))
  val df_join = df_ratings.join(df_movies)
    .withColumn("year", split(col("release_date"), "-").getItem(0))
    .withColumn("month", split(col("release_date"), "-").getItem(1))
    
  val df_output = df_join.groupBy(col("year"), col("month")).agg(count(col("rating")))
  
  writeToParquet(df_output, parquetfilespath+"query1")
}