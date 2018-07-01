import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.scalatest.{FlatSpec, Matchers}

class WordCountSpec extends FlatSpec with Matchers with TestSpec {


  val wordCountApplication = new WordCountApplication()
  "toLowerCase" should "push lower text to kafka" in {
    val driver = new TopologyTestDriver(wordCountApplication.toLowerCaseStream("input-topic", "output-topic"), config)
    val recordFactory = new ConsumerRecordFactory("input-topic", new StringSerializer(), new StringSerializer())
    val words = "Hello, world world test"
    driver.pipeInput(recordFactory.create(words))
    val record: ProducerRecord[String, String] = driver.readOutput("output-topic", stringDeserializer, stringDeserializer)
    record.value() shouldBe words.toLowerCase
    driver.close()
  }

  "WorkCount" should "count number of words" in {
    val driver = new TopologyTestDriver(wordCountApplication.countNumberOfWords("input-topic", "output-topic", "counts-store"), config)
    val recordFactory = new ConsumerRecordFactory("input-topic", new StringSerializer(), new StringSerializer())
    val words = "Hello, world world test"
    driver.pipeInput(recordFactory.create(words))
    val store: KeyValueStore[String, java.lang.Long] = driver.getKeyValueStore("counts-store")
    store.get("hello") shouldBe 1
    store.get("world") shouldBe 2
    store.get("test") shouldBe 1
    driver.close()
  }

}
