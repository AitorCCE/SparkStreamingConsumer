import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.Seconds
//import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf

import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreamingKafkaConsumer extends App {


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test-consumer-group"
  )

  val sparkConf = new SparkConf().setAppName("sparkConsumer").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(5))

  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](Set("KafkaProducerTopic"), kafkaParams)
  )

  stream.foreach(println)

  ssc.start()
  ssc.awaitTermination()

}



