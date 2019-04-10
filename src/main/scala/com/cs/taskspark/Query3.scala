package com.cs.taskspark
import com.cs.utils.ParquetUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.{ array, collect_list }
import org.apache.spark.sql.functions._

object Query3 extends App with ParquetUtils {
  /*
   * Query 3 .  for each month, genre - create a new column that has comma separated list of movies
   */
  System.setProperty("hadoop.home.dir","C:\\hadoop" );
  val conf = new SparkConf().setMaster("local[2]").setAppName("Movie-App")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val parquetfilespath = "G:\\project\\spark\\movie_app\\src\\main\\resources\\"

  val genres = "genres"
  val genres_movies = "genresmovies"
  val movies = "movies"
  val occupations = "occupations"
  val ratings = "ratings"
  val users = "users"

  val df_movies = readFromParquet(sqlContext, parquetfilespath + movies)
    .withColumnRenamed("id", "movies_ids")
    .withColumn("year", split(col("release_date"), "-").getItem(0))
    .withColumn("month", split(col("release_date"), "-").getItem(1))
  val df_genres_movies = readFromParquet(sqlContext, parquetfilespath + genres_movies)
  
  val genres_moviesjoin = df_movies
    .join(df_genres_movies, df_movies("movies_ids") === df_genres_movies("movie_id"))

  val title = collect_list(col("title")).alias("title")

  val df_output = genres_moviesjoin.groupBy(col("month"), col("genre_id"))
    .agg(title)

  writeToParquet(df_output, parquetfilespath + "query2")
}