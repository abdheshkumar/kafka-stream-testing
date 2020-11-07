import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WordCountSpec extends AnyFlatSpec with Matchers with TestSpec {
  val wordCountApplication = new WordCountApplication()
  "Convert streaming data into lowercase and publish into output topic" should "push lower text to kafka" in {
    val driver = new TopologyTestDriver(
      wordCountApplication.toLowerCaseStream("input-topic", "output-topic"),
      config
    )
    val recordFactory = new ConsumerRecordFactory(
      "input-topic",
      new StringSerializer(),
      new StringSerializer()
    )
    val words = "Hello, world world test"
    driver.pipeInput(recordFactory.create(words))
    val record: ProducerRecord[String, String] =
      driver.readOutput("output-topic", stringDeserializer, stringDeserializer)
    record.value() shouldBe words.toLowerCase
    driver.close()
  }

  "WorkCount" should "count number of words" in {
    val driver = new TopologyTestDriver(
      wordCountApplication
        .countNumberOfWords("input-topic", "output-topic", "counts-store"),
      config
    )
    val recordFactory = new ConsumerRecordFactory(
      "input-topic",
      new StringSerializer(),
      new StringSerializer()
    )
    val words = "Hello Kafka Streams, All streams lead to Kafka"
    driver.pipeInput(recordFactory.create(words))
    val store: KeyValueStore[String, java.lang.Long] =
      driver.getKeyValueStore("counts-store")
    store.get("hello") shouldBe 1
    store.get("kafka") shouldBe 2
    store.get("streams") shouldBe 2
    store.get("lead") shouldBe 1
    store.get("to") shouldBe 1
    driver.close()

    /*
    val expected: List[(String, java.lang.Long)] = List("hello" -> 1L, "kafka" -> 2L, "streams" -> 2L, "all" -> 1L, "lead" -> 1L, "to" -> 1L)
    expected.foreach { case (key, expectedValue) =>
      store.get(key) shouldBe expectedValue
    }*/
  }

}
