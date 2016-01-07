package com.pygmalios.sparkCheckpointExperience.kafka.embedded

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.Executors

import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}

import scala.collection.JavaConversions.mapAsJavaMap
import scala.concurrent._
import scala.reflect.io.Directory

trait EmbeddedKafka {
  private val executorService = Executors.newFixedThreadPool(2)
  implicit private val executionContext = ExecutionContext.fromExecutorService(executorService)
  private val stringSerializer = new StringSerializer()

  def config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

  def withKafka(body: (RunningEmbeddedKafka) => Unit) = {
    withZookeeper(config.zooKeeperPort) {
      withKafkaServer(config) {
        withProducer(config) { runningEmbeddedKafka =>
          body(runningEmbeddedKafka)
        }
      }
    }
  }

  private def withProducer(config: EmbeddedKafkaConfig)(body: (RunningEmbeddedKafka) => Any): Unit = {
    val kafkaProducer = new KafkaProducer(Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
      ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG -> 3000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 100.toString
    ), stringSerializer, stringSerializer)
    val runningEmbeddedKafka = new RunningEmbeddedKafka(kafkaProducer)
    try {
      body(runningEmbeddedKafka)
    }
    finally {
      runningEmbeddedKafka.close()
    }
  }

  private def withZookeeper(zooKeeperPort: Int)(body: => Any): Unit = {
    val zkLogsDir = Directory.makeTemp("zookeeper-logs")
    val tickTime = 2000

    val zkServer = new ZooKeeperServer(zkLogsDir.toFile.jfile, zkLogsDir.toFile.jfile, tickTime)

    val factory = ServerCnxnFactory.createFactory
    factory.configure(new InetSocketAddress("localhost", zooKeeperPort), 1024)
    factory.startup(zkServer)
    try {
      body
    }
    finally {
      factory.shutdown()
    }
  }

  private def withKafkaServer(config: EmbeddedKafkaConfig)(body: => Any): Unit = {
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
    try {
      body
    }
    finally {
      broker.shutdown()
    }
  }
}