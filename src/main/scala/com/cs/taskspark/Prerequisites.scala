package com.cs.taskspark

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.cs.utils.ParquetUtils
import com.cs.utils.SchemaUtils
import com.cs.utils.SchemaUtils
import com.cs.utils.SchemaUtils

object Prerequisites extends App with ParquetUtils {
  val textfilespath = "G:\\project\\spark\\"
  val parquetfilespath = "G:\\project\\spark\\movie_app\\src\\main\\resources\\"
  val delimiter = ","

  val genres = "genres"
  val genres_movies = "genres_movies"
  val movies = "movies"
  val occupations = "occupations"
  val ratings = "ratings"
  val users = "users"

  val conf = new SparkConf().setMaster("local[2]").setAppName("Movie-App")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // read and convert into parquet file
  // genres
  writeToParquet(readTextFile(sqlContext, textfilespath + genres + ".csv",
    SchemaUtils.genres, delimiter), parquetfilespath + genres)

  // genres_movies
  writeToParquet(readTextFile(sqlContext, textfilespath + genres_movies + ".csv",
    SchemaUtils.genres, delimiter), parquetfilespath + genres_movies)

  // movies
  writeToParquet(readTextFile(sqlContext, textfilespath + movies + ".csv",
    SchemaUtils.genres, delimiter), parquetfilespath + movies)

  // occupations
  writeToParquet(readTextFile(sqlContext, textfilespath + occupations + ".csv",
    SchemaUtils.genres, delimiter), parquetfilespath + occupations)

  // ratings
  writeToParquet(readTextFile(sqlContext, textfilespath + ratings + ".csv",
    SchemaUtils.genres, delimiter), parquetfilespath + ratings)

  // ratings
  writeToParquet(readTextFile(sqlContext, textfilespath + users + ".csv",
    SchemaUtils.genres, delimiter), parquetfilespath + users)
}