import scala.collection.JavaConverters._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.apache.kafka.clients.producer.Callback
import scala.concurrent.Promise
import scala.concurrent.Future
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Node
import java.util.concurrent.{Future => JavaFuture}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.internals.DefaultPartitioner

class ProducerSpec extends AnyFlatSpec with Matchers {

  "Producer" should "produce value" in {

    val mockProducer =
      new MockProducer(true, new StringSerializer(), new StringSerializer())

    val sendResponseAsJavaFuture: JavaFuture[RecordMetadata] =
      mockProducer.send(
        new ProducerRecord("website", "{\"site\" : \"learnscala.co\"}")
      )
    //Using Callback
    val promise = Promise[RecordMetadata]()
    mockProducer.send(
      new ProducerRecord("website", "{\"site\" : \"learnscala.co\"}"),
      (metadata, exception) =>
        if (exception != null) promise.failure(exception)
        else promise.success(metadata)
    )
    val sendResponse: Future[RecordMetadata] = promise.future

    mockProducer.history.asScala.foreach(producerRecord =>
      producerRecord.value() mustBe "{\"site\" : \"learnscala.co\"}"
    )
    mockProducer.history.size mustBe 2
  }

  "Kafka Cluster" should "work with Kafka producer and consumer" in {
    val TOPIC_NAME = "topic-name"
    val partitionInfo0 = new PartitionInfo(TOPIC_NAME, 0, null, null, null)
    val partitionInfo1 = new PartitionInfo(TOPIC_NAME, 1, null, null, null)
    val list = List(partitionInfo0, partitionInfo1)

    val cluster = new Cluster(
      "kafka-cluster",
      List.empty[Node].asJava,
      list.asJava,
      Set.empty.asJava,
      Set.empty.asJava
    )

    val mockProducer =
      new MockProducer(
        cluster,
        true,
        new DefaultPartitioner,
        new StringSerializer(),
        new StringSerializer()
      )
    //val kafkaProducer = new KafkaProducer(mockProducer);
  }
}
