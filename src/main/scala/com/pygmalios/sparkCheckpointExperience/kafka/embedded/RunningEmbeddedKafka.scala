package com.pygmalios.sparkCheckpointExperience.kafka.embedded

import java.io.Closeable

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.Logging

import scala.concurrent.duration._

class RunningEmbeddedKafka(producer: KafkaProducer[String, String]) extends Closeable with Logging {
  val simpleConsumer = new SimpleConsumer("localhost", 6001, 1000000, 64*1024, "")

  def publish(topic: String, message: String): Unit = publish(topic, null, message)

  def publish(topic: String, key: String, message: String): Unit = {
    producer.send(new ProducerRecord(topic, key, message)).get(3, SECONDS)
    log.info(s"$topic: $key -> $message")

    val topicAndPartition = TopicAndPartition(topic, 0)

    val latestRequest = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 100)))
    val offsetResponse = simpleConsumer.getOffsetsBefore(latestRequest)
    log.debug(offsetResponse.partitionErrorAndOffsets(topicAndPartition).offsets.mkString("Latest time: ", ",", ""))

    val earliestRequest = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 100)))
    val offsetResponse2 = simpleConsumer.getOffsetsBefore(earliestRequest)
    log.debug(offsetResponse2.partitionErrorAndOffsets(topicAndPartition).offsets.mkString("Earliest time: ", ",", ""))

    //val topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkZlient)
    //log.debug(topicMetadata.toString())
  }

  override def close(): Unit = producer.close()
}