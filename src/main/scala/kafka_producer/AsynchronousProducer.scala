package kafka_producer

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

object AsynchronousProducer {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val topicName = "AsynchronousProducerTopic"
    val key = "Key1"
    val value = "Value-1"
    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092,localhost:9093")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topicName, key, value)
    val resultFuture = send(record)(producer)
    val result = Await.result(resultFuture, Duration.Inf)
    println("Metadata of published message:" + result.offset())
    producer.close()
  }

  def send(record: ProducerRecord[String, String])(producer: KafkaProducer[String, String]): Future[RecordMetadata] = {
    val result = Promise[RecordMetadata]()
    producer.send(record, new MyProducerCallback(result))
    result.future
  }
}

class MyProducerCallback(result: Promise[RecordMetadata]) extends Callback {
  override def onCompletion(metadata: RecordMetadata,
                            exception: Exception): Unit = {
    if (exception == null) result.trySuccess(metadata)
    else println(s"Failed with an exception: ${exception}")
  }
}