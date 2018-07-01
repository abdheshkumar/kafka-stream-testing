package kafka_producer

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

class SupplierDeserializer extends Deserializer[Supplier] {
  private val encoding = "UTF8"

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): Supplier = try {
    val buf = ByteBuffer.wrap(data)
    val id = buf.getInt
    val sizeOfName = buf.getInt
    val nameBytes = new Array[Byte](sizeOfName)
    buf.get(nameBytes)

    val deserializedName = new String(nameBytes, encoding)
    val sizeOfDate = buf.getInt
    val dateBytes = new Array[Byte](sizeOfDate)
    buf.get(dateBytes)

    val dateString = new String(dateBytes, encoding)
    val df = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
    new Supplier(id, deserializedName, df.parse(dateString))
  } catch {
    case e: Exception =>
      throw new SerializationException("Error when deserializing byte[] to kafka_java.Supplier")
  }
}
