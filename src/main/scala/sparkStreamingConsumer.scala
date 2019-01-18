import kafka.serializer._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming._
import org.apache.spark._
import org.joda.time.DateTime

object sparkStreamingConsumer extends App {

  private val conf = new SparkConf().setAppName("SparkStreamingConsumer").setMaster("local[2]")

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(10))

  val ss = SparkSession.builder().appName("SparkStreamingConsumer").getOrCreate()
  val sqlContext: SQLContext = ss.sqlContext

  import ss.implicits._

  val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
  val dsKafka = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("KafkaProducerTopic"))

  case class metadata_schema(tripduration : String, starttime : String, stoptime : String, start_station_id : String,
                             start_station_name : String, start_station_latitude : String, start_station_longitude : String,
                             end_station_id : String, end_station_name : String, end_station_latitude : String,
                             end_station_longitude : String, bikeid : String, usertype : String, birth_year : String,
                             gender: String)

// -----------------------------------------------------------------------------------------------------------------------
// OPCIÓN 1 :: Sin usar collect.
// -----------------------------------------------------------------------------------------------------------------------
/*
      dsKafka.map(_._2).foreachRDD { r =>
        println("r: " + r.getClass)                    // 1.1.- r: RDD[String]
        val rdd_schema = r
          .map(row => row.split(','))                  // 1.2.- ¿Mala deserializacion?
          .map(field => metadata_schema(field(0), field(1), field(2), field(3),
              field(4), field(5), field(6), field(7),
              field(8), field(9), field(10), field(11),
              field(12), field(13), field(14)))
        println("rdd_schema: " + rdd_schema.getClass)  // 1.3.- rdd_schema: RDD[metadata_schema]

        val df = ss.createDataFrame(rdd_schema)
        println("df: " + df.getClass)                  // 1.5.- df: sql.DataFrame
        df.show()

        //val df_2 = rdd_schema.toDF()
        //println("df_2: " + df_2.getClass)              // 1.8.- df_2: sql.DataFrame
        //df_2.show()

        val time = new DateTime().toString("yyyy-MM-dd_HH-mm-ss")  //  Para evitar el
        df.write.json("hdfs://utad:8020/SparkStreaming/" + time)   //  error path already exists
      }
*/

// -----------------------------------------------------------------------------------------------------------------------
// OPCIÓN 2 :: Usando collect.
// -----------------------------------------------------------------------------------------------------------------------

    dsKafka.map(_._2).foreachRDD { r =>
      println("r: " + r.getClass)                     // 2.1.- r: RDD[String]
      val rdd_collect = r.collect()
      println("collect: " + rdd_collect.getClass)      // 2.2.- rdd_colect: Array[String]
      val rdd_schema = rdd_collect
        .map((row => row.split(',')))                 // 2.3.- ¿Mala deserializacion?
        .map(field => metadata_schema(field(0), field(1), field(2), field(3),
          field(4), field(5), field(6), field(7),
          field(8), field(9), field(10), field(11),
          field(12), field(13), field(14)))
      println("rdd_schema: " + rdd_schema.getClass)   // 2.4.- rdd_schema: Array[metadata_schema]
      val df = ss.createDataFrame(rdd_schema)
      println("df: " + df.getClass)                   // 2.5.- df: sql.DataFrame
      df.show()

      val time = new DateTime().toString("yyyy-MM-dd_HH-mm-ss")  //  Para evitar el
      df.write.json("hdfs://utad:8020/SparkStreaming/" + time)   //  error path already exists
    }


// -----------------------------------------------------------------------------------------------------------------------
// OPCIÓN 3 :: Usando ".as[metadata_schema]" para deserializar con una case class. No muestra resultado alguno
// -----------------------------------------------------------------------------------------------------------------------
/*
  dsKafka.map(_._2).foreachRDD { r =>
    r.collect().map(data => ss.sqlContext
                              .read
                              .json(data)
                              .as[metadata_schema].toDF().show())}}   // 3.1.- No se si también se podría deserializar así
*/

// -----------------------------------------------------------------------------------------------------------------------
// PRUEBA 1 (Muestra resultados parcialmente correctos) ::   - En lugar de Dstream, leer desde un fichero .json
//                                                           - Mapear el fichero leído a la case class metadata_schema
//                                                           - Convertirlo a DF y escribirlo en HDFS
//                                                           - Creación/escritura en tabla
// -----------------------------------------------------------------------------------------------------------------------
/*
      val rdd_json_metadata = ss.sparkContext.textFile("/home/utad/Escritorio/TFM/entrada.json")
      val rdd_json_schema = rdd_json_metadata
        .map(row => row.split(','))
        .map(field => metadata_schema(field(0), field(1), field(2), field(3), field(4), field(5),
                                      field(6), field(7), field(8), field(9), field(10), field(11),
                                      field(12), field(13), field(14)))
      rdd_json_schema.toDF().write.json("hdfs://utad:8020/test/")
      val df_json = rdd_json_schema.toDF()
      df_json.createOrReplaceTempView("tabla_prueba")
      df_json.sqlContext.sql("SELECT * FROM tabla_prueba").show()
*/


// -----------------------------------------------------------------------------------------------------------------------
// PRUEBA 0 (OK) ::   - leer desde fichero a DF
//                    - escribir DF de json a HDFS
//                    - crear TABLE a partir del DF
// -----------------------------------------------------------------------------------------------------------------------
/*
      val df = sqlContext.read.json("/home/utad/Escritorio/TFM/entrada.json")
      df.write.json("hdfs://utad:8020/test/")
      df.printSchema()
      df.createOrReplaceTempView("prueba_cubo")
      df.sqlContext.sql("SELECT * FROM prueba_cubo").show()
*/

    //Aggregate data and save it into ES
    //...

  ssc.start()
  ssc.awaitTermination()

}