package kafka_producer

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.jdk.CollectionConverters._

object SupplierConsumer {
  val topicName = "SupplierTopic"
  val groupName = "SupplierTopicGroup"

  val props = new Properties
  props.put("bootstrap.servers", "localhost:9092,localhost:9093")
  props.put("group.id", groupName)
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "kafka_java.SupplierDeserializer")


  val consumer = new KafkaConsumer[String, Supplier](props)
  consumer.subscribe(util.Arrays.asList(topicName))

  while (true) {
    val records = consumer.poll(100)
    for (record <- records.asScala) {
      System.out.println("kafka_java.Supplier id= " + String.valueOf(record.value.getID) + " kafka_java.Supplier  Name = " + record.value.getName + " kafka_java.Supplier Start Date = " + record.value.getStartDate.toString)
    }
  }
}
