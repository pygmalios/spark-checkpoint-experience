package com.pygmalios.sparkCheckpointExperience.kafka.embedded

import java.io.Closeable

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.Logging

import scala.concurrent.duration._

class RunningEmbeddedKafka(producer: KafkaProducer[String, String]) extends Closeable with Logging {
  def publish(topic: String, message: String): Unit = publish(topic, null, message)

  def publish(topic: String, key: String, message: String): Unit = {
    producer.send(new ProducerRecord(topic, key, message)).get(3, SECONDS)
    log.debug(s"Mesage published to $topic. [$key -> $message]")
  }

  override def close(): Unit = producer.close()
}
