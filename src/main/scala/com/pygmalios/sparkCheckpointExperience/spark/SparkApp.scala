package com.pygmalios.sparkCheckpointExperience.spark

import java.util.concurrent.atomic.AtomicBoolean

import _root_.kafka.serializer.{Decoder, StringDecoder}
import _root_.kafka.utils.VerifiableProperties
import com.pygmalios.sparkCheckpointExperience.Logging
import com.pygmalios.sparkCheckpointExperience.kafka.KafkaApp
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.concurrent._

object SparkApp extends App with Logging {
  private type Key = Int
  private type Value = String

  private val lastMessage: Option[Value] = None
  private val lastMessageProcessesed: AtomicBoolean = new AtomicBoolean(false)

  private lazy val checkpointDir = "./checkpoints"
  private lazy val appName = "spark-checkpoint-experience"

  private lazy val stringStateSpec = StateSpec.function[Key, Value, String, (Key, Value)](stateMapping _)

  withSsc() { inputStream =>
    inputStream.mapWithState(stringStateSpec)
  }

  private def withSsc()(action: (DStream[(Key, Value)]) => DStream[(Key, Value)]): Unit = {
    val ssc = StreamingContext.getOrCreate(checkpointDir, () => createSsc(action))

    if (lastMessage.nonEmpty) {
      Future {
        blocking {
          while (!lastMessageProcessesed.get()) {
            Thread.sleep(100)
          }
          log.debug("Stopping SSC after last message...")
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          log.info("Spark streaming context stopped after last message")
        }
      } (ExecutionContext.global)
    }

    ssc.start()

    log.debug("Awaiting termination...")
    ssc.awaitTermination()
    log.info("Spark streaming context terminated")
  }

  private def createSsc(action: (DStream[(Key, Value)]) => DStream[(Key, Value)]): StreamingContext = {
    log.debug(s"Creating Spark streaming context...")

    // Create Spark configuration
    val conf = new SparkConf().setAppName(appName)
      .setMaster("local[2]")
    log.debug(s"Spark configuration: $conf")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("./checkpoints")

    // Connect to embedded Kafka
    val kafkaStream = createKafkaStream(ssc)

    // Invoke action and print it
    action(kafkaStream).foreachRDD { rdd =>
      rdd.foreach {
        case (k: Key, v: Value) =>
          log.info(f"${KafkaApp.topic}: $k%3s -> $v%3s")

          if (k < 0) {
            log.info(s"Brutally killing JVM")
            Runtime.getRuntime.halt(-1)
          }

          if (lastMessage.exists(_ == v)) {
            log.debug(s"Last message received. [$v]")
            lastMessageProcessesed.set(true)
          }

        case other =>
          log.warn(s"Weird message received! [$other]")
      }
    }

    ssc
  }

  private def createKafkaStream(ssc: StreamingContext): DStream[(Key, Value)] = {
    // Configure Kafka
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> s"localhost:${KafkaApp.confKafkaServerPort}",
      "auto.offset.reset" -> "smallest"
    )

    log.debug(s"Kafka direct stream params: $kafkaParams")

    // Create direct Kafka stream
    KafkaUtils.createDirectStream[Key, Value, IntDecoder, StringDecoder](ssc, kafkaParams, Set(KafkaApp.topic))
  }

  private def stateMapping(time: Time,
                           key: Key,
                           value: Option[Value],
                           oldState: State[String]): Option[(Key, String)] = {
    val newState = value.toString
    log.debug(s"State mapping: [key: $key, value: $value, $oldState -> $newState]")
    oldState.update(newState)
    value.map(key -> _)
  }
}

class IntDecoder(props: VerifiableProperties = null) extends Decoder[Int] {
  private val stringDecoder = new StringDecoder(props)
  override def fromBytes(bytes: Array[Byte]): Int =
    stringDecoder.fromBytes(bytes).toInt
}