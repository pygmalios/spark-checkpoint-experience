package com.pygmalios.sparkCheckpointExperience.kafka

import java.util.TimerTask
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.pygmalios.sparkCheckpointExperience.kafka.embedded.{EmbeddedKafka, RunningEmbeddedKafka}
import jline.ConsoleReader

import scala.annotation.tailrec

/**
  * Embedded Apache Kafka.
  */
object KafkaApp extends App with EmbeddedKafka {
  val topic = "Experience"

  private val counter = new AtomicInteger()
  private val negativeFlag = new AtomicBoolean()

  withKafka { kafka =>
    val timerTask = publishEverySecond(kafka)
    try {
      readFromConsole()
    }
    finally {
      timerTask.cancel()
    }
  }

  private def readFromConsole(): Unit = {
    val consoleReader = new ConsoleReader()

    @tailrec
    def loop(): Unit = {
      consoleReader.readVirtualKey() match {
        case '\n' =>
          negativeFlag.set(true)
          loop()

        case other =>
          log.debug(other.toString)
      }
    }

    loop()
  }

  private def publishEverySecond(kafka: RunningEmbeddedKafka): TimerTask = {
    val t = new java.util.Timer()
    val task = new java.util.TimerTask {
      def run(): Unit = {
        val key = counter.addAndGet(1)
        val c = if (negativeFlag.getAndSet(false)) -1 else 1
        kafka.publish(topic, (c * key).toString, key.toString)
      }
    }

    t.schedule(task, 1000L, 1000L)
    task
  }
}