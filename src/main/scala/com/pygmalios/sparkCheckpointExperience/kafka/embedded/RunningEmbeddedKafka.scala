package com.pygmalios.sparkCheckpointExperience.kafka.embedded

import java.io.Closeable

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.duration._

class RunningEmbeddedKafka(producer: KafkaProducer[String, String]) extends Closeable {
  def publish(topic: String, message: String): Unit = publish(topic, null, message)

  def publish(topic: String, key: String, message: String): Unit =
    producer.send(new ProducerRecord(topic, key, message)).get(3, SECONDS)

  override def close(): Unit = producer.close()
}
