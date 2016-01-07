package com.pygmalios.sparkCheckpointExperience.kafka.embedded

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.Executors

import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}

import scala.collection.JavaConversions.mapAsJavaMap
import scala.concurrent._
import scala.concurrent.duration._
import scala.reflect.io.Directory
import scala.util.Try

trait EmbeddedKafka {
  private val executorService = Executors.newFixedThreadPool(2)
  implicit private val executionContext = ExecutionContext.fromExecutorService(executorService)

  def withKafka(body: => Unit)(implicit config: EmbeddedKafkaConfig) = {
    val factory = startZooKeeper(config.zooKeeperPort)
    try {
      val broker = startKafka(config)
      try {
        body
      }
      finally {
        broker.shutdown()
      }
      factory.shutdown()
    }
  }

  def publishStringMessageToKafka(topic: String, message: String)(implicit config: EmbeddedKafkaConfig): Unit =
    publishToKafka(topic, message)(config, new StringSerializer)

  private def publishToKafka[T](topic: String, message: T)
                       (implicit config: EmbeddedKafkaConfig, serializer: Serializer[T]): Unit = {

    val kafkaProducer = new KafkaProducer(Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
      ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG -> 3000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 100.toString
    ), new StringSerializer, serializer)

    val sendFuture = kafkaProducer.send(new ProducerRecord(topic, message))
    val sendResult = Try {
      sendFuture.get(3, SECONDS)
    }

    kafkaProducer.close()

    if (sendResult.isFailure) throw new KafkaUnavailableException
  }

  private def startZooKeeper(zooKeeperPort: Int): ServerCnxnFactory = {
    val zkLogsDir = Directory.makeTemp("zookeeper-logs")
    val tickTime = 2000

    val zkServer = new ZooKeeperServer(zkLogsDir.toFile.jfile, zkLogsDir.toFile.jfile, tickTime)

    val factory = ServerCnxnFactory.createFactory
    factory.configure(new InetSocketAddress("localhost", zooKeeperPort), 1024)
    factory.startup(zkServer)
    factory
  }

  private def startKafka(config: EmbeddedKafkaConfig): KafkaServer = {
    val kafkaLogDir = Directory.makeTemp("kafka")

    val zkAddress = s"localhost:${config.zooKeeperPort}"

    val properties: Properties = new Properties
    properties.setProperty("zookeeper.connect", zkAddress)
    properties.setProperty("broker.id", "0")
    properties.setProperty("host.name", "localhost")
    properties.setProperty("auto.create.topics.enable", "true")
    properties.setProperty("port", config.kafkaPort.toString)
    properties.setProperty("log.dir", kafkaLogDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", 1.toString)

    val broker = new KafkaServer(new KafkaConfig(properties))
    broker.startup()
    broker
  }
}