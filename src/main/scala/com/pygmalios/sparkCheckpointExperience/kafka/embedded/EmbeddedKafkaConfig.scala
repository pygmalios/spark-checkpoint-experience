package com.pygmalios.sparkCheckpointExperience.kafka.embedded

case class EmbeddedKafkaConfig(kafkaPort: Int = 6001, zooKeeperPort: Int = 6000)

object EmbeddedKafkaConfig {
  implicit val defaultConfig = EmbeddedKafkaConfig()
}