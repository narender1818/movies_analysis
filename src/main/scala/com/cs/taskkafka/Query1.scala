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

/*
 *Write a spark application to read the Kafka topics(movie lens dataset) and print them in Console
 */
object Query1 extends App {
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
 
  stream.foreachRDD(rdd => {
    rdd.foreachPartition(iterator => {
      while (iterator.hasNext) {
        val next = iterator.next()
        println(next.value())
      }
    })
  })

  
  ssc.start()
  ssc.awaitTermination()
}