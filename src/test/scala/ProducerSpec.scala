import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{FlatSpec, MustMatchers}
import scala.collection.JavaConverters._
class ProducerSpec extends FlatSpec with MustMatchers {

  "Producer" should "produce value" in {

    val mockProducer =
      new MockProducer(true, new StringSerializer(), new StringSerializer())

    mockProducer.send(
      new ProducerRecord("example", "{\"site\" : \"learnscala.com\"}")
    )
    mockProducer.history.asScala.foreach(v => println(v.value()))
    mockProducer.history.size mustBe 1
  }
}
