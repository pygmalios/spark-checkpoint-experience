# The Apache Spark Streaming Checkpointing Experience (with Kafka)
Apache Spark Streaming checkpointing playground for everyone to learn by example. Important note is that it uses
embedded Apache Kafka as the data source. That helps us to simulate some problematic scenarios.

## Intro

This application contains two standalone runnable console applications:

1) [KafkaApp](https://github.com/pygmalios/spark-checkpoint-experience/blob/master/src/main/scala/com/pygmalios/sparkCheckpointExperience/kafka/KafkaApp.scala)
2) [SparkApp](https://github.com/pygmalios/spark-checkpoint-experience/blob/master/src/main/scala/com/pygmalios/sparkCheckpointExperience/spark/SparkApp.scala)

### KafkaApp console application

Starts embedded **Zookeeper on localhost:6000** and **Kafka server on localhost:6001**. Creates a single topic named
**Experience** with short **retention of 15 seconds**. Segmentation is set to 1 second and log is checked for cleanup
every 15 seconds. It is important to understand what these settings mean otherwise you will don't know why has the Spark
app crashed.

**Producer** sends every second a message to the **Experience** topic containing **key and value of a counter**. The
counter is initialized to zero and increased by one for every message sent.

The application reads from the standard input and it  

### SparkApp console application
 
## Don't wait longer than 15 seconds with a restart
 
Otherwise log cleaning happens in Kafka and Spark Streaming is not able to restore state from checkpoint because the
log offset does not exist in Kafka anymore.

![The Jimmy Henrix Experience](Are_You_Experienced_-_US_cover-edit.jpg?raw=true)

## Problematic scenario: stop Spark, clean old Kafka logs, start Spark

[SPARK-12693](https://issues.apache.org/jira/browse/SPARK-12693)

The probability of this case is in real world not that high, but definitely not impossible. If log cleaning happens
between application stop and start then the application ends up in an erroneous state and cannot recover from it unless
checkpoints are deleted. I have used smallest, largest and no offset reset and the behavior is the same.