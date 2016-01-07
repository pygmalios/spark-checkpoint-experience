package com.pygmalios.sparkCheckpointExperience.kafka

import com.pygmalios.sparkCheckpointExperience.Logging
import com.pygmalios.sparkCheckpointExperience.kafka.embedded.EmbeddedKafka

/**
  * Embedded Apache Kafka.
  */
object KafkaApp extends App with EmbeddedKafka with Logging {
  log.info("Starting Kafka.")
  withKafka { kafka =>
    log.info("Running Kafka.")

    log.info("msg1")
    kafka.publish("topic", "msg1")

    log.info("msg2")
    kafka.publish("topic", "msg2")
  }
  log.info("Kafka stopped.")
}
