import java.util.Properties

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.elasticsearch.spark._
import java.io.PrintWriter

import org.apache.spark.sql.SparkSession

//import org.apache.spark.storage.StorageLevel


object SparkStreamingKafkaConsumer extends App {

  Logger.getLogger("org.apache.spark.streaming.dstream.DStream").setLevel(Level.DEBUG)
  Logger.getLogger("org.apache.spark.streaming.dstream.WindowedDStream").setLevel(Level.DEBUG)
  Logger.getLogger("org.apache.spark.streaming.DStreamGraph").setLevel(Level.DEBUG)
  Logger.getLogger("org.apache.spark.streaming.scheduler.JobGenerator").setLevel(Level.DEBUG)

  val conf = new SparkConf().setAppName("sparkConsumer").setMaster("local[2]")
    .set("es.index.auto.create", "true")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.es.index.auto.create", "true")
    //.set("spark.es.nodes", "yourESaddress")
    .set("spark.es.port", "9200")
    //.set("spark.es.net.http.auth.user","")
    //.set("spark.es.net.http.auth.pass", "")
    .set("spark.es.nodes.wan.only", "true")

  val ssc = new StreamingContext(conf, batchDuration = Seconds(1)) //Para ventana de 10 minutos, sustituir por Minutes(10)

  //val sparkSession = SparkSession.builder().config(conf).appName("sparkConsumer").getOrCreate()

  val kafkaParams = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "ScalaConsumer",
    "auto.offset.reset" -> "earliest"
  )

  val kafkaTopics = Set("KafkaProducerTopic")

  /*
  // -------------------
  // Para Kafka 0-10
  // -------------------

  //val offsets = Map(new TopicPartition("KafkaProducerTopic", 0) -> 2L)
  val preferredHosts = LocationStrategies.PreferConsistent
  val Dstream = KafkaUtils.createDirectStream[String, String](
    ssc,
    preferredHosts,
    ConsumerStrategies.Subscribe[String, String](kafkaTopics, kafkaParams)) //, offsets))
  */


  // -------------------
  // Para Kafka 0-8
  // -------------------

  val kafkaParams2 = Map(
    "zookeeper.connect" -> "localhost:2181",
    "group.id" -> "spark-streaming-test",
    "zookeeper.connection.timeout.ms" -> "1000",
    "bootstrap.servers" -> "localhost:9092")

  val Dstream = KafkaUtils.createDirectStream(ssc, kafkaParams2, kafkaTopics)
  //val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-consumer-group", Map("KafkaProducerTopic" -> 1))


  // Otra forma para fijar ventana de 10 minutos:
  // val Dstream10 = Dstream.window(Minutes(10))

  println("--- kafkaStream: "+ Dstream)
  println("--- KafkaTopics: " + kafkaTopics)
  println("--- SSC: "+ ssc)
  //println("--- PreferredHosts: "+ preferredHosts)


  // Prueba simple de escritura de un Dstream en HDFS y ElasticSearch
  Dstream.foreachRDD(d => {d.collect().foreach(println)
    d.saveAsTextFile("hdfs://utad:8020/test/spark-streaming")
    d.saveToEs("sparkstreamingtests/pruebas")
    })

/*
  Dstream.foreachRDD(d => if (!d.isEmpty()) {d.collect().foreach(println)
    d.saveAsTextFile("hdfs://utad:8020/test/spark-streaming")
    d.saveToEs("sparkstreamintests/pruebas")})*/

  /*
    kafkaStream.foreachRDD(p => {
    p.collect().foreach(p2 => p.saveAsTextFile("hdfs://utad:8020/test/spark-streaming"))
    p.saveToEs("sparkstreamintests/pruebas")})
  */


  // Una vez correcta la escritura en los distintos destinos,
  // realizar operaciones sobre la ventana de 10 minutos
  // antes de llevar a ElasticSearch (Por ejemplo reduceByKeyAndWindow?)

  /*
    // El siguiente bloque escribe correctamente el texto
    // "Esto es una prueba" en un nuevo fichero "prueba.txt"
    // generado en la carpeta test de HDFS

    val config = new Configuration()
    config.set("fs.defaultFS", "hdfs://utad:8020/test")
    val fs= FileSystem.get(config)
    val output = fs.create(new Path("prueba.txt"))
    val writer = new PrintWriter(output)
    writer.write("Esto es una prueba")
    writer.close()
    */

  /*
  // Este bloque escribe correctamente en ElasticSearch
  // los siguientes datos de prueba
  val numeros = Map("uno" -> 1, "dos" -> 2, "tres" -> 3)
  val prueba = Map("texto" -> "Prueba", "Resultado" -> "OK")
  ssc.sparkContext.makeRDD(Seq(numeros, prueba)).saveToEs("spark/docs")


  */

  ssc.start()
  ssc.awaitTermination()
  ssc.stop()

}

