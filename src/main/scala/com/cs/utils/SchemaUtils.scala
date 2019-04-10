package com.cs.utils

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
/*
 * This is used to create the schema 
 */

object SchemaUtils {
  //  CREATE TABLE occupations (
  //  id integer NOT NULL,
  //  name varchar(255),
  //  PRIMARY KEY (id)
  //);
  val occupations = StructType(StructField("id", IntegerType, true) ::
    StructField("name", StringType, true) :: Nil)
  // CREATE TABLE users (
  //  id integer NOT NULL,
  //  age integer,
  //  gender char(1),
  //  occupation_id integer,
  //  zip_code varchar(255),
  //  PRIMARY KEY (id)

  val users = StructType(StructField("id", IntegerType, true) ::
    StructField("age", IntegerType, true) ::
    StructField("gender", StringType, true) ::
    StructField("occupation_id", IntegerType, true) ::
    StructField("zip_code", StringType, true) :: Nil)
  // CREATE TABLE ratings (
  //  id integer NOT NULL,
  //  user_id integer,
  //  movie_id integer,
  //  rating integer,
  //  rated_at timestamp,
  //  PRIMARY KEY (id)
  val ratings = StructType(StructField("id", IntegerType, true) ::
    StructField("user_id", IntegerType, true) ::
    StructField("movie_id", IntegerType, true) ::
    StructField("rating", IntegerType, true) ::
    StructField("rated_at", DateType, true) :: Nil)
  //CREATE TABLE movies (
  //  id integer NOT NULL,
  //  title varchar(255),
  //  release_date date,
  //  PRIMARY KEY (id)
  val movies = StructType(StructField("id", IntegerType, true) ::
    StructField("title", StringType, true) ::
    StructField("release_date", DateType, true) ::
    Nil)

  //    CREATE TABLE genres (
  //  id integer NOT NULL,
  //  name varchar(255),
  //  PRIMARY KEY (id)
  //);
  val genres = StructType(StructField("id", IntegerType, true) ::
    StructField("name", StringType, true) ::

    Nil)
  //CREATE TABLE genres_movies (
  //  id integer NOT NULL,
  //  movie_id integer,
  //  genre_id integer,
  //  PRIMARY KEY (id)
  //);
  val genres_movies = StructType(StructField("id", IntegerType, true) ::
    StructField("movie_id", IntegerType, true) ::
    StructField("genre_id", IntegerType, true) ::
    Nil)

}