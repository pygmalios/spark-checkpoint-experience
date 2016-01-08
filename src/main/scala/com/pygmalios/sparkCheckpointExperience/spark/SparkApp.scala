package com.pygmalios.sparkCheckpointExperience.spark

import java.util.concurrent.atomic.AtomicBoolean

import _root_.kafka.serializer.{Decoder, StringDecoder}
import _root_.kafka.utils.VerifiableProperties
import com.pygmalios.sparkCheckpointExperience.Logging
import com.pygmalios.sparkCheckpointExperience.kafka.KafkaApp
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory

import scala.concurrent._

object SparkApp extends App with Logging {
  private type Key = Int
  private type Value = Int
  private type StreamState = Int

  private val lastMessage: Option[Value] = None
  private val lastMessageProcessesed: AtomicBoolean = new AtomicBoolean(false)

  private lazy val checkpointDir = "./checkpoints"
  private lazy val appName = "spark-checkpoint-experience"
  private lazy val kafkaAutoOffsetReset = "smallest"

  private lazy val keyCountStateSpec = StateSpec.function[Key, (Key, Value), StreamState, (Key, Value)](countKeys _)

  // Use Logback as streaming output action
  lazy val streamingOutputLog = LoggerFactory.getLogger("StreamingOutput")

  withSsc() { inputStream =>
    // Count all messages in a state
    val counterStream = inputStream.map { case (k, v) => 1 -> (k, v) }
    counterStream.mapWithState(keyCountStateSpec)
  }

  private def withSsc()(action: (DStream[(Key, Value)] => MapWithStateDStream[Key, (Key, Value), StreamState, (Key, Value)])): Unit = {
    val ssc = StreamingContext.getOrCreate(checkpointDir, () => createSsc(action), createOnError = true)

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
          val logMsg = f"${KafkaApp.topic}: $k%3s -> $v%3s"
          streamingOutputLog.info(logMsg)

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
      "metadata.broker.list" -> s"localhost:${KafkaApp.kafkaServerPort}",
      "auto.offset.reset" -> kafkaAutoOffsetReset
    )

    log.debug(s"Kafka direct stream params: $kafkaParams")

    // Create direct Kafka stream
    KafkaUtils.createDirectStream[Key, Value, IntDecoder, IntDecoder](ssc, kafkaParams, Set(KafkaApp.topic))
  }

  private def countKeys(time: Time,
                        key: Key,
                        value: Option[(Key, Value)],
                        oldState: State[StreamState]): Option[(Key, Value)] = {
    val count = oldState.getOption().getOrElse(0) + 1
    oldState.update(count)
    val stateMsg = s"Count = $count"
    streamingOutputLog.info(stateMsg)

    value
  }
}

class IntDecoder(props: VerifiableProperties = null) extends Decoder[Int] {
  private val stringDecoder = new StringDecoder(props)
  override def fromBytes(bytes: Array[Byte]): Int =
    stringDecoder.fromBytes(bytes).toInt
}