package com.pygmalios.sparkCheckpointExperience.kafka

import com.pygmalios.sparkCheckpointExperience.Logging
import com.pygmalios.sparkCheckpointExperience.kafka.embedded.EmbeddedKafka

/**
  * Embedded Apache Kafka.
  */
object KafkaApp extends App with EmbeddedKafka with Logging {
  log.info("Starting Kafka.")
  withKafka {
    log.info("Running Kafka.")

    log.info("msg1")
    publishStringMessageToKafka("topic", "msg1")

    log.info("msg2")
    publishStringMessageToKafka("topic", "msg2")
  }
  log.info("Kafka stopped.")
}
