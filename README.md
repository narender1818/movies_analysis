#-----------------------------------------for spark task----------------------------------------
dataset
 https://raw.githubusercontent.com/ankane/movielens.sql/master/movielens.sql 
 
## The why?


-   For each year and month , print the count of movies having rating greater than equal to 4

-  For each year and month , print the count of movies based on Genre having rating greater than equal to 4 Query 3. Print top-5 movies based on the rating by Students ( in occupation )

-  for each month, genre - create a new column that has comma separated list of movie

#-----------------------------------------for kafka task----------------------------------------
## location 
C:\soft\kafka_2.12-2.2.0\kafka_2.12-2.2.0\bin\windowskafka-server-start

## Step 1 

Start zookeeper 

set the class path and then type 

Open command prompt and type zkserver



### to start server

.\bin\windows\kafka-server-start.bat .\config\server.properties

## Create topics 

- kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic genres
- kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic genres_movies
- kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic movies
- kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic occupations
- kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ratings
- kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic users

Now topic is ready, we pass data to this topic. For this assignment I had created the data from
 https://raw.githubusercontent.com/ankane/movielens.sql/master/movielens.sql 
 
- kafka-console-producer.bat --broker-list localhost:9092 --topic movies < G:\project\spark\genres.csv
- kafka-console-producer.bat --broker-list localhost:9092 --topic movies < G:\project\spark\genres_movies.csv
- kafka-console-producer.bat --broker-list localhost:9092 --topic movies < G:\project\spark\movies.csv
- kafka-console-producer.bat --broker-list localhost:9092 --topic movies < G:\project\spark\occupations.csv
- kafka-console-producer.bat --broker-list localhost:9092 --topic movies < G:\project\spark\ratings.csv
- kafka-console-producer.bat --broker-list localhost:9092 --topic movies < G:\project\spark\users.csv
 
## The why?

- Write a spark application to read the Kafka topics(movie lens dataset) and print them in Console
- Read the movie lens dataset in spark, write a spark application to get the Maximum Rating of each genre and find which genre is most rated by the users?


## Schema


CREATE TABLE occupations (
  id integer NOT NULL,
  name varchar(255),
  PRIMARY KEY (id)
);

CREATE TABLE users (
  id integer NOT NULL,
  age integer,
  gender char(1),
  occupation_id integer,
  zip_code varchar(255),
  PRIMARY KEY (id)
);

CREATE TABLE ratings (
  id integer NOT NULL,
  user_id integer,
  movie_id integer,
  rating integer,
  rated_at timestamp,
  PRIMARY KEY (id)
);
CREATE TABLE movies (
  id integer NOT NULL,
  title varchar(255),
  release_date date,
  PRIMARY KEY (id)
);
CREATE TABLE genres (
  id integer NOT NULL,
  name varchar(255),
  PRIMARY KEY (id)
);
CREATE TABLE genres_movies (
  id integer NOT NULL,
  movie_id integer,
  genre_id integer,
  PRIMARY KEY (id)
);

