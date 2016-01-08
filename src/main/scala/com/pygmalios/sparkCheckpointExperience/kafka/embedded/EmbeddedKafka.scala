package com.pygmalios.sparkCheckpointExperience.kafka.embedded

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.Executors

import com.pygmalios.sparkCheckpointExperience.Logging
import kafka.admin.AdminUtils
import kafka.consumer.SimpleConsumer
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
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
  def zookeeperPort: Int
  def kafkaServerPort: Int
  def retentionSec: Int
  def retentionCheckSec: Int
  def segmentationSec: Int
  def topic: String

  def withKafka(body: (RunningEmbeddedKafka) => Unit) = {
    try {
      withZookeeper { zkClient =>
        withKafkaServer(zkClient) {
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
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:$kafkaServerPort",
      ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG -> 3000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 10.toString
    )
    val kafkaProducer = new KafkaProducer(config, stringSerializer, stringSerializer)
    try {
      if (log.isDebugEnabled) {
        log.debug(s"Kafka producer config:")
        logProperties(config.toList)
      }

      val simpleConsumer = new SimpleConsumer("localhost", kafkaServerPort, 1000000, 64 * 1024, "")
      try {

        body(new RunningEmbeddedKafka(kafkaProducer, simpleConsumer, topic))
      }
      finally {
        simpleConsumer.close()
      }
    }
    finally {
      kafkaProducer.close()
    }
  }

  private def withZookeeper(body: (ZkClient) => Any): Unit = {
    val log = getSublog("zkServer")
    val zkLogsDir = Directory.makeTemp("zookeeper-logs")
    val tickTime = 2000

    val zkServer = new ZooKeeperServer(zkLogsDir.toFile.jfile, zkLogsDir.toFile.jfile, tickTime)

    val factory = ServerCnxnFactory.createFactory
    factory.configure(new InetSocketAddress("localhost", zookeeperPort), 1024)

    log.debug(s"Zookeeper port: $zookeeperPort")
    log.debug(s"Zookeeper logs directory: ${zkLogsDir.toFile}")
    log.debug(s"Starting Zookeeper server ...")
    factory.startup(zkServer)
    log.info(s"Zookeeper started on localhost:$zookeeperPort")

    try {
      val zkClient = new ZkClient(s"localhost:$zookeeperPort", 10000, 10000, ZKStringSerializer)
      try {
        body(zkClient)
      }
      finally {
        zkClient.close()
      }
    }
    finally {
      log.debug(s"Shutting down Zookeeper...")
      factory.shutdown()
      log.info(s"Zookeeper shut down")
    }
  }

  private def withKafkaServer(zkClient: ZkClient)(body: => Any): Unit = {
    val log = getSublog("kafkaServer")

    val kafkaLogDir = Directory.makeTemp("kafka")

    val properties = new Properties
    properties.setProperty("zookeeper.connect", s"localhost:$zookeeperPort")
    properties.setProperty("broker.id", "0")
    properties.setProperty("host.name", "localhost")
    properties.setProperty("auto.create.topics.enable", "true")
    properties.setProperty("port", kafkaServerPort.toString)
    properties.setProperty("log.dir", kafkaLogDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", 1.toString)

    // How often Kafka checks if there is a segment to delete?
    properties.setProperty("log.retention.check.interval.ms", secToMsString(retentionCheckSec))

    val kafkaConfig = new KafkaConfig(properties)
    if (log.isDebugEnabled) {
      log.debug(s"Kafka server config:")
      logProperties(kafkaConfig.props.props.toList)
    }

    log.debug(s"Starting Kafka server...")
    val broker = new KafkaServer(kafkaConfig)
    log.info(s"Kafka server started on localhost:$kafkaServerPort")

    broker.startup()
    try {
      // Log retention starts working only after 30 seconds from server start: LogManager.InitialTaskDelayMs
      val topicProperties = new Properties()
      topicProperties.setProperty("retention.ms", secToMsString(retentionSec))
      topicProperties.setProperty("segment.ms", secToMsString(segmentationSec))
      topicProperties.setProperty("cleanup.policy", "delete")
      topicProperties.setProperty("delete.retention.ms", "1000")

      if (log.isDebugEnabled) {
        log.debug(s"Topic properties:")
        logProperties(topicProperties.toList)
      }

      AdminUtils.createTopic(zkClient, topic, 1, 1, topicProperties)
      log.info(s"Topic $topic created with $retentionSec sec retention (starts 30 seconds after server start)")

      body
    }
    finally {
      log.debug(s"Shutting down Kafka server...")
      broker.shutdown()
      log.info(s"Kafka server shut down")
      kafkaLogDir.deleteIfExists()
    }
  }

  private def secToMsString(sec: Int): String = (sec*1000).toString

  private def getSublog(name: String) = LoggerFactory.getLogger(getClass.getPackage.getName + "." + name)

  private def logProperties(props: Seq[(String, String)]): Unit = {
    props.sortBy(_._1).foreach { case (k, v) =>
      log.debug(s"    $k = $v")
    }
  }
}