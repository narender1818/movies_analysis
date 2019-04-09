package com.cs.taskkafka

import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.TaskContext
import java.net.InetAddress
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import com.cs.utils.SchemaUtils
/*
 * . Read the movie lens dataset in spark, write a spark application 
 * to get the Maximum Rating of each genre and find which genre is most rated by the users?

 */
object Query2 extends App  {
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(1))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test-consumer-group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean))

  val topics = Array("genres", "genres_movies", "movies", "occupations", "ratings", "users")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams))

  stream.map(record => (record.key, record.value))

  var df: DataFrame = null

  stream.foreachRDD(rdd =>
    if (!rdd.isEmpty()) {
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      val topicValueStrings = rdd.map(record => {
        Row.fromSeq(record.value().split(",").toSeq)
      })
      val occupation = StructType(StructField("id", StringType, true) ::
        StructField("name", StringType, true) :: Nil)
        
      df = sqlContext.createDataFrame(topicValueStrings, occupation)
    })

  df.show()
  ssc.start()
  ssc.awaitTermination()
}