package kafka_producer

import java.nio.ByteBuffer
import java.util

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

class SupplierSerializer extends Serializer[Supplier] {
  private val encoding = "UTF8"

  override def configure(configs: util.Map[String, _], isKey: Boolean) = {}

  override def serialize(topic: String, data: Supplier) = {
    try {
      val serializedName = data.getName.getBytes(encoding)
      val sizeOfName = serializedName.length
      val serializedDate = data.getStartDate.toString.getBytes(encoding)
      val sizeOfDate = serializedDate.length
      val buf = ByteBuffer.allocate(4 + 4 + sizeOfName + 4 + sizeOfDate)
      buf.putInt(data.getID)
      buf.putInt(sizeOfName)
      buf.put(serializedName)
      buf.putInt(sizeOfDate)
      buf.put(serializedDate)
      buf.array
    } catch {
      case e: Exception =>
        throw new SerializationException("Error when serializing kafka_java.Supplier to byte[]")
    }
  }

  override def close(): Unit = ()
}
