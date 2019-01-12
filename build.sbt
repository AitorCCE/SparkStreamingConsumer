
scalaVersion := "2.11.8"
name := "kafka-spark"
organization := "ch.epfl.scala"
version := "1.0"

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.8.2.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0"

//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.4.0" % "provided" // Indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.3.2" % "provided" // Indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.3.1" % "provided" // Indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.3.0" % "provided" // Indica KafkaUtils obsoleto
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.2.2" % "provided" // NO indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.2.1" % "provided" // NO indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.2.0" % "provided" // NO indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.3" % "provided" // NO indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.2" % "provided" // NO indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0" % "provided" // NO indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0" % "provided" // NO indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0" % "provided" // NO indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0" % "provided" // NO indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0" % "provided" // NO indica KafkaUtils obsoleto
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.0.0" % "provided" // NO indica KafkaUtils obsoleto


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"
libraryDependencies += "org.typelevel" %% "cats-core" % "1.4.0"
libraryDependencies += "org.json" % "json" % "20180813"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.0-M3"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.5.4"
*/
