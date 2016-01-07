package com.pygmalios.sparkCheckpointExperience.kafka.embedded

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.Executors

import com.pygmalios.sparkCheckpointExperience.Logging
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.{mapAsJavaMap, _}
import scala.concurrent._
import scala.reflect.io.Directory

trait EmbeddedKafka extends Logging {
  private val executorService = Executors.newFixedThreadPool(2)
  implicit private val executionContext = ExecutionContext.fromExecutorService(executorService)
  private val stringSerializer = new StringSerializer()

  // Override these values if needed
  def confZkPort = 6000
  def confKafkaServerPort = 6001

  def withKafka(body: (RunningEmbeddedKafka) => Unit) = {
    try {
      withZookeeper {
        withKafkaServer {
          withProducer { runningEmbeddedKafka =>
            body(runningEmbeddedKafka)
          }
        }
      }
    }
    catch {
      case ex: Exception =>
        log.error("Well, that hurts!", ex)
        throw ex
    }
  }

  private def withProducer(body: (RunningEmbeddedKafka) => Any): Unit = {
    val log = getSublog("kafkaProducer")
    val config = Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:$confKafkaServerPort",
      ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG -> 3000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 10.toString
    )
    val kafkaProducer = new KafkaProducer(config, stringSerializer, stringSerializer)

    if (log.isDebugEnabled) {
      log.debug(s"Kafka producer config:")
      config.toList.sortBy(_._1).foreach { case (k, v) =>
        log.debug(s"    $k = $v")
      }
    }

    val runningEmbeddedKafka = new RunningEmbeddedKafka(kafkaProducer)
    try {
      body(runningEmbeddedKafka)
    }
    finally {
      runningEmbeddedKafka.close()
    }
  }

  private def withZookeeper(body: => Any): Unit = {
    val log = getSublog("zkServer")
    val zkLogsDir = Directory.makeTemp("zookeeper-logs")
    val tickTime = 2000

    val zkServer = new ZooKeeperServer(zkLogsDir.toFile.jfile, zkLogsDir.toFile.jfile, tickTime)

    val factory = ServerCnxnFactory.createFactory
    factory.configure(new InetSocketAddress("localhost", confZkPort), 1024)

    log.debug(s"Zookeeper port: $confZkPort")
    log.debug(s"Zookeeper logs directory: ${zkLogsDir.toFile}")
    log.debug(s"Starting Zookeeper server ...")
    factory.startup(zkServer)
    log.info(s"Zookeeper started on localhost:$confZkPort")

    try {
      body
    }
    finally {
      log.debug(s"Shutting down Zookeeper...")
      factory.shutdown()
      log.info(s"Zookeeper shut down")
    }
  }

  private def withKafkaServer(body: => Any): Unit = {
    val log = getSublog("kafkaServer")

    val kafkaLogDir = Directory.makeTemp("kafka")

    val properties: Properties = new Properties
    properties.setProperty("zookeeper.connect", s"localhost:$confZkPort")
    properties.setProperty("broker.id", "0")
    properties.setProperty("host.name", "localhost")
    properties.setProperty("auto.create.topics.enable", "true")
    properties.setProperty("port", confKafkaServerPort.toString)
    properties.setProperty("log.dir", kafkaLogDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", 1.toString)

    val kafkaConfig = new KafkaConfig(properties)
    if (log.isDebugEnabled) {
      log.debug(s"Kafka server config:")
      kafkaConfig.props.props.toList.sortBy(_._1).foreach { case (k, v) =>
        log.debug(s"    $k = $v")
      }
    }

    log.debug(s"Starting Kafka server...")
    val broker = new KafkaServer(kafkaConfig)
    log.info(s"Kafka server started on localhost:$confKafkaServerPort")

    broker.startup()
    try {
      body
    }
    finally {
      log.debug(s"Shutting down Kafka server...")
      broker.shutdown()
      log.info(s"Kafka server shut down")
      kafkaLogDir.deleteIfExists()
    }
  }

  private def getSublog(name: String) = LoggerFactory.getLogger(getClass.getPackage.getName + "." + name)
}