package com.cs.taskspark

import com.cs.utils.ParquetUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.count

object Query2 extends App with ParquetUtils { /*
   * Query 2.  For each year and month , print the count of movies based on Genre having rating greater 
   * than equal to 4 
   * Query 3. Print top-5 movies based on the rating by Students ( in occupation )
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
    .withColumnRenamed("id", "movies_ids")
  val df_ratings = readFromParquet(sqlContext, parquetfilespath + ratings).where(col("rating") >= lit(4))
  val df_occupations = readFromParquet(sqlContext, parquetfilespath + occupations)
    .withColumnRenamed("id", "occupations_ids")
  val df_users = readFromParquet(sqlContext, parquetfilespath + users)

  // read the occupations student data only i.e 19,Student

  val users_occupations = df_users
    .join(df_occupations, df_users("occupation_id") === df_occupations("occupations_ids"))
    .where(col("occupations_ids") === lit(19))

  val ratings_movies = df_ratings.join(df_movies,
    df_ratings("movie_id") === df_movies("movie_ids"))
    .withColumn("year", split(col("release_date"), "-").getItem(0))
    .withColumn("month", split(col("release_date"), "-").getItem(1))

  val df = ratings_movies.join(users_occupations,
    ratings_movies("user_id") === users_occupations("id"))

  val df_output = df.groupBy(col("year"), col("month"))
    .agg(
      col("name") as "name",
      count(col("rating")) as "count")
    .orderBy(col("rating").desc).limit(5)

  writeToParquet(df_output, parquetfilespath + "query2")
}