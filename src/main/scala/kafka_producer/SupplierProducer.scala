package kafka_producer

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SupplierProducer extends App {
  val topicName = "SupplierTopic"

  val props = new Properties
  props.put("bootstrap.servers", "localhost:9092,localhost:9093")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "kafka_java.SupplierSerializer")

  val producer = new KafkaProducer[String, Supplier](props)

  val df = new SimpleDateFormat("yyyy-MM-dd")
  val sp1 = new Supplier(101, "Xyz Pvt Ltd.", df.parse("2016-04-01"))
  val sp2 = new Supplier(102, "Abc Pvt Ltd.", df.parse("2012-01-01"))

  producer.send(new ProducerRecord[String, Supplier](topicName, "SUP", sp1)).get
  producer.send(new ProducerRecord[String, Supplier](topicName, "SUP", sp2)).get

  System.out.println("kafka_java.SupplierProducer Completed.")
  producer.close()
}
