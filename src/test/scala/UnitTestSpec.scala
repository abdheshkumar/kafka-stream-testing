import java.lang
import java.util.Properties

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.{StreamsConfig, Topology, TopologyTestDriver}
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType, Punctuator}
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore, Stores}
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter

class UnitTestSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {
  private var testDriver: TopologyTestDriver = _
  private var store: KeyValueStore[String, java.lang.Long] = _
  private val stringDeserializer = new StringDeserializer()
  private val longDeserializer = new LongDeserializer()
  private val recordFactory = new ConsumerRecordFactory(new StringSerializer(), new LongSerializer())

  before {
    val topology = new Topology
    topology.addSource("sourceProcessor", "input-topic")
    topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor")
    topology.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore("aggStore"),
        Serdes.String,
        Serdes.Long)
        .withLoggingDisabled, // need to disable logging to allow store pre-populating
      "aggregator")
    topology.addSink("sinkProcessor", "result-topic", "aggregator")
    // setup test driver// setup test driver

    val config = new Properties()
    config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "maxAggregation")
    config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long.getClass.getName)
    testDriver = new TopologyTestDriver(topology, config)

    // pre-populate store
    store = testDriver.getKeyValueStore("aggStore")
    store.put("a", 21L)
  }
  after {
    testDriver.close()
  }

  it should "FlushStoreForFirstInput" in {
    testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 9999L))
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L: java.lang.Long)
    testDriver.readOutput("result-topic", stringDeserializer, longDeserializer)
  }

  it should "not update store for smaller value" in {
    testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 9999L))
    store.get("a") shouldBe 21L
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L: java.lang.Long)
    testDriver.readOutput("result-topic", stringDeserializer, longDeserializer) shouldBe null
  }

  it should "not update store for larger value" in {
    testDriver.pipeInput(recordFactory.create("input-topic", "a", 42L, 9999L))
    store.get("a") shouldBe 42L
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 42L: java.lang.Long)
    testDriver.readOutput("result-topic", stringDeserializer, longDeserializer) shouldBe null
  }
  it should "update stroe for new key" in {
    testDriver.pipeInput(recordFactory.create("input-topic", "b", 21L, 9999L))
    store.get("b") shouldBe 21L
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L: java.lang.Long)
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "b", 21L: java.lang.Long)
    testDriver.readOutput("result-topic", stringDeserializer, longDeserializer) shouldBe null
  }
  it should "Punctuate if wall clock time advances" in {
    testDriver.advanceWallClockTime(60000)
    OutputVerifier.compareKeyValue(testDriver.readOutput("result-topic", stringDeserializer, longDeserializer), "a", 21L: java.lang.Long)
    testDriver.readOutput("result-topic", stringDeserializer, longDeserializer) shouldBe null
  }

}

import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier}

class CustomMaxAggregatorSupplier extends ProcessorSupplier[String, java.lang.Long] {
  override def get: Processor[String, java.lang.Long] = {
    new CustomMaxAggregator()
  }
}

class CustomMaxAggregator extends Processor[String, java.lang.Long] {
  var context: ProcessorContext = null
  private var store: KeyValueStore[String, java.lang.Long] = null

  override def init(context: ProcessorContext): Unit = {
    this.context = context
    context.schedule(60000, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
      override def punctuate(timestamp: Long): Unit = flushStore
    })
    context.schedule(10000, PunctuationType.STREAM_TIME, new Punctuator() {
      override def punctuate(timestamp: Long): Unit = {
        flushStore
      }
    })
    store = this.context.getStateStore("aggStore").asInstanceOf[KeyValueStore[String, java.lang.Long]]
  }

  override def process(key: String, value: lang.Long): Unit = {
    val oldValue = store.get(key)
    if (oldValue == null || value > oldValue) store.put(key, value)
  }

  override def close(): Unit = {}

  private def flushStore(): Unit = {
    val it: KeyValueIterator[String, lang.Long] = store.all
    while (it.hasNext) {
      val next = it.next
      context.forward(next.key, next.value)
    }
  }
}