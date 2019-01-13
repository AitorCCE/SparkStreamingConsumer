import kafka.serializer._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming._
import org.apache.spark._


object SparkStreamingConsumer extends App {

  private val conf = new SparkConf()
    .setAppName("SparkStreamingConsumer")
    .setMaster("local[2]")

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(10))

  val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
  val dsKafka = KafkaUtils
    .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("KafkaProducerTopic"))

  dsKafka.map(_._2).foreachRDD { myRDD =>

    //Save to HDFS
    println(myRDD)
    myRDD.saveAsTextFile("hdfs://utad:8020/test/spark-streaming")

    //Aggregate data and save it into ES
    //...

  }

  ssc.start()
  ssc.awaitTermination()

}