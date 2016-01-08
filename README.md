# The Apache Spark Streaming Checkpointing Experience
Apache Spark Streaming checkpointing playground for everyone to learn by example. Important note is that it uses
embedded Apache Kafka as the data source. That helps us to simulate some problematic scenarios.
 
## Intro
 
## Don't wait longer than 15 seconds with a restart
 
Otherwise log cleaning happens in Kafka and Spark Streaming is not able to restore state from checkpoint because the
log offset does not exist in Kafka anymore.

![The Jimmy Henrix Experience](Are_You_Experienced_-_US_cover-edit.jpg?raw=true)

## Problematic scenario: stop Spark, clean old Kafka logs, start Spark

[SPARK-12693](https://issues.apache.org/jira/browse/SPARK-12693)

The probability of this case is in real world not that high, but definitely not impossible. If log cleaning happens
between application stop and start then the application ends up in an erroneous state and cannot recover from it unless
checkpoints are deleted. I have used smallest, largest and no offset reset and the behavior is the same.