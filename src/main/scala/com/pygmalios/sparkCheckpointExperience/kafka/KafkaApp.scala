package com.pygmalios.sparkCheckpointExperience.kafka

import com.pygmalios.sparkCheckpointExperience.kafka.embedded.EmbeddedKafka

/**
  * Embedded Apache Kafka.
  */
object KafkaApp extends App with EmbeddedKafka {
  withKafka { kafka =>
    kafka.publish("t1", "k1", "m1")
    kafka.publish("t2", "m2")
  }
}
