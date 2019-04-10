package com.cs.taskkafka

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

/*
 * . Read the movie lens dataset in spark, write a spark application 
 * to get the Maximum Rating of each genre and find which genre is most rated by the users?

 */
object Query2 extends App with ParquetUtils {
  System.setProperty("hadoop.home.dir", "C:\\hadoop");
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
  val df_ratings = readFromParquet(sqlContext, parquetfilespath + ratings)
  val df_occupations = readFromParquet(sqlContext, parquetfilespath + occupations)
    .withColumnRenamed("id", "occupations_ids")
  val df_users = readFromParquet(sqlContext, parquetfilespath + users)

  // read the occupations student data only i.e 19,Student

  val users_occupations = df_users
    .join(df_occupations, df_users("occupation_id") === df_occupations("occupations_ids"))
    .where(col("occupations_ids") === lit(19))

  val ratings_movies = df_ratings.join(df_movies,
    df_ratings("movie_id") === df_movies("movies_ids"))
    .withColumn("year", split(col("release_date"), "-").getItem(0))
    .withColumn("month", split(col("release_date"), "-").getItem(1))

  val df_rate = ratings_movies.join(users_occupations,
    ratings_movies("user_id") === users_occupations("id"))
  //val title = collect_list(col("title")).alias("title")

  val df_genres_movies = readFromParquet(sqlContext, parquetfilespath + genres_movies)

  val genres_moviesjoin = df_movies
    .join(df_genres_movies, df_movies("movies_ids") === df_genres_movies("movie_id"))
    .withColumnRenamed("movie_id", "movie_id_1")
  val df = df_rate
    .join(genres_moviesjoin, df_rate("movies_ids") === genres_moviesjoin("movie_id_1"))

  val df_output = df.groupBy(col("movie_id_1"), col("genre_id"))
    .agg(
      first(col("user_id")) as "user_id",
      max(col("rating")) as "maxrating").orderBy(col("user_id").desc)

  println(df_output.show(200))
}