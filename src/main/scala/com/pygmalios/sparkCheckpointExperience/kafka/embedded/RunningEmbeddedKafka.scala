package com.pygmalios.sparkCheckpointExperience.kafka.embedded

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.Logging

import scala.concurrent.duration._

class RunningEmbeddedKafka(producer: KafkaProducer[String, String],
                           consumer: SimpleConsumer,
                           topic: String) extends Logging {
  val topicAndPartition = TopicAndPartition(topic, 0)

  def publish(topic: String, message: String): Unit = publish(topic, null, message)

  def publish(topic: String, key: String, message: String): Unit = {
    producer.send(new ProducerRecord(topic, key, message)).get(3, SECONDS)

    val latest    = getOffset(OffsetRequest.LatestTime)
    val earliest  = getOffset(OffsetRequest.EarliestTime)
    log.info(f"$topic [$earliest%3d:$latest%3d]: $key%3s -> $message%3s")
  }

  private def getOffset(time: Long): Long = {
    val response = consumer.getOffsetsBefore(OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(time, 100))))
    response.partitionErrorAndOffsets(topicAndPartition).offsets.head
  }
}