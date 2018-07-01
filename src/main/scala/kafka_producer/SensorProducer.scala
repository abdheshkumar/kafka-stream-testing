package kafka_producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SensorProducer {
  val topicName = "SensorTopic"

  val props = new Properties
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("partitioner.class", "kafka_java.SensorPartitioner")
  props.put("speed.sensor.name", "TSS")

  val producer = new KafkaProducer[String, String](props)

  val data = (0 to 10)
  data.foreach { i =>
    producer.send(new ProducerRecord[String, String](topicName, "SSP" + i, "500" + i))
    producer.send(new ProducerRecord[String, String](topicName, "TSS", "500" + i))
  }

  producer.close()

  System.out.println("kafka_java.SimpleProducer Completed.")
}
