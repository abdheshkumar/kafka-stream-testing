import java.lang.Long
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.{KStream, KTable, Materialized, Produced}
import org.apache.kafka.streams.state.KeyValueStore

import scala.collection.JavaConverters.asJavaIterableConverter

class WordCountApplication {
  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p
  }

  def countNumberOfWords(inputTopic: String,
                         outputTopic: String, storeName: String): Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val textLines: KStream[String, String] = builder.stream(inputTopic)
    val wordCounts: KTable[String, Long] = textLines
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+").toIterable.asJava)
      .groupBy((_, word) => word)
      .count(Materialized.as(storeName).asInstanceOf[Materialized[String, Long, KeyValueStore[Bytes, Array[Byte]]]])
    wordCounts.toStream().to(outputTopic, Produced.`with`(Serdes.String(), Serdes.Long()))
    builder.build()
  }

  def toLowerCaseStream(inputTopic: String, outputTopic: String): Topology = {
    val builder: StreamsBuilder = new StreamsBuilder()
    val textLines: KStream[String, String] = builder.stream(inputTopic)
    val wordCounts: KStream[String, String] = textLines
      .mapValues(textLine => textLine.toLowerCase)
    wordCounts.to(outputTopic)
    builder.build()
  }
}

object WordCountApplication extends WordCountApplication {
  def main(args: Array[String]) {
    val streams: KafkaStreams = new KafkaStreams(countNumberOfWords("input-topic", "output-topic", "counts-store"), config)
    streams.start()
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      streams.close(10, TimeUnit.SECONDS)
    }))
  }

}