# The Apache Spark Streaming Checkpointing Experience (with Kafka)
Apache Spark Streaming checkpointing playground for everyone to learn by example. Important note is that it uses
embedded Apache Kafka as the data source. That helps us to simulate some problematic scenarios.

![The Jimmy Henrix Experience](Are_You_Experienced_-_US_cover-edit.jpg?raw=true)

## Intro

This application contains two standalone runnable console applications:

1. [KafkaApp](https://github.com/pygmalios/spark-checkpoint-experience/blob/master/src/main/scala/com/pygmalios/sparkCheckpointExperience/kafka/KafkaApp.scala)
2. [SparkApp](https://github.com/pygmalios/spark-checkpoint-experience/blob/master/src/main/scala/com/pygmalios/sparkCheckpointExperience/spark/SparkApp.scala)

### KafkaApp console application

Starts embedded **Zookeeper on localhost:6000** and **Kafka server on localhost:6001**. Creates a single topic named
**Experience** with short **retention of 15 seconds**. Segmentation is set to 1 second and log is checked for cleanup
every 15 seconds. It is important to understand what these settings mean otherwise you will don't know why has the Spark
app crashed.

**Producer** sends every second a message to the **Experience** topic containing **key and value of a counter**. The
counter is initialized to zero and increased by one for every message sent.

The application reads from the standard input and you can **press enter to send one negative key to the topic**. This
causes receiving application to fail intentionally to simulate a crash.

### SparkApp console application

Creates Spark streaming context with **local[2]** as master. It of course uses checkpointing configured to store data in
**./checkpoints** directory. If the application gets into a state when it cannot even start due to an error, delete this
folder.

It creates a [**direct stream (no receivers)**](http://spark.apache.org/docs/latest/streaming-kafka-integration.html#approach-2-direct-approach-no-receivers)
to the previously started `KafkaApp` with **1 second batch duration**. It implements typical map/reduce algorithm to count
number of received messages and **stores the count in a state**.
 
### Logging

Both applications write some useful information to console and they also create files in the **log** directory.
`KafkaApp` produces **log/kafka.log** and `SparkApp` **spark.log** files. Special log file named **streaming-output.log**
serves as an external output storage. It contains received messages and also changes of the state.

`KafkaApp` should every second append a line to its log like `Experience [ 89:109]: 109 -> 109` which has following
format: `TOPIC_NAME [TOPIC_EARLIEST_TIME:TOPIC_LATEST_TIME]: MESSAGE_KEY -> MESSAGE_VALUE`. If you press enter then
you should see a negative message key value.

`SparkApp` should append two info lines to `StreamingOutput` logger every second. The first line contains total number
of messages received `Count = 109` and the second line is key and value successfully processed message
`Experience: 109 -> 109`.

## Correctness check

As long as the `StreamingOutput` log contains pairs of lines with matching numbers of message count and key/value number
you can be sure that the `SparkApp` has:
 
- **not missed** any of the messages sent to Kafka
- and also has not **processed twice** any of them too

*E.g. you want to see lines like this:*
```
[INFO] [2016-01-08 16:18:10,025] [StreamingOutput]: Count = 890
[INFO] [2016-01-08 16:18:10,025] [StreamingOutput]: Experience: 890 -> 890
```

## Hard reset

In case `SparkApp` gets to a situation it cannot correctly continue or it cannot start at all:

1. Stop it
2. Delete `./checkpoints` directory

You lose state but also all checkpointed data that most probably cause problems. 
 
## Scenario #1: Endless retention

1. **Preparation:** set `KafkaApp.retentionSec = 3600` (one hour). Yes, one hour means forever now.
2. Start `KafkaApp`.
3. Start `SparkApp`.
4. Can keep looking at console output of `SparkApp` to monitor what's going on. Now feel free to **stop/start/kill** the
   running `SparkApp` in any way you want and when you start it back it should catch up, process only previously
   unprocessed messages and update total count state accordingly. Check the log so that you are sure every message is
   there exactly once. 

*Periodically check console outputs of both apps for errors. There should be none.*

## Scenario #2: The problem of short Kafka retention

[SPARK-12693](https://issues.apache.org/jira/browse/SPARK-12693)

1. **Preparation:** set `KafkaApp.retentionSec = 15` (15 seconds) and make sure that `KafkaApp.retentionCheckSec` is
   also set to a low value (15 seconds let's say).
2. Start `KafkaApp` and **wait 30 seconds** so that retention scheduled jobs starts. (Kafka has an initial delay)
3. Start `SparkApp` and try to stop/start/kill it just like in the previous scenario, but now there quite a big chance
   that you will not be able to start the app back again.
   
**Wait longer than `KafkaApp.retentionCheckSec` with a restart** to reproduce this problem.

Log cleaning happens in Kafka and Spark Streaming is not able to restore state from checkpoint because the
log offset does not exist in Kafka anymore.

Basically this problem has nothing to do with Spark. It lies somewhere in integration between Spark and Kafka. If log
cleaning happens between application stop and start then the application ends up in an erroneous state and cannot
recover from it unless checkpoints are deleted.

Kafka consumer has a setting `auto.offset.reset` which can be either `smallest` or `largest` but it doesn't help in case
checkpointed is used because offsets stored using chepointing are used.

## Conclusion

**Spark Streaming checkpointing feature works reliably** even in case of brutal JVM killing (`Runtime.getRuntime.halt(-1)`).
It processes every message exactly one assuming that the not-yet-processed messages are still available in Kafka log after
restart.

**In case of bad luck/extreme Kafka retention configuration checkpointed state has to be reset** to be able to restart the
job. There are also other cases (implementation logic/data structure breaking changes) when you have to reset the accumulated
state.

The bottom line is **don't use Spark Streaming checkpoint state to store anything you cannot easily reload**
from an external source.