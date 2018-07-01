package kafka_producer

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.utils.Utils

class SensorPartitioner extends Partitioner {
  var speedSensorName: Option[String] = Option.empty

  override def partition(topic: String, key: scala.Any,
                         keyBytes: Array[Byte],
                         value: scala.Any,
                         valueBytes: Array[Byte], cluster: Cluster): Int = {

    val partitions = cluster.partitionsForTopic(topic)
    val numPartitions = partitions.size
    val sp = Math.abs(numPartitions * 0.3).toInt
    Option(keyBytes).map(f => Some(new String(f)) == speedSensorName) match {
      case Some(_) => Utils.toPositive(Utils.murmur2(valueBytes)) % sp
      case None => Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - sp) + sp
    }
  }

  override def close() = {}

  override def configure(configs: util.Map[String, _]) = {
    speedSensorName = Some(configs.get("speed.sensor.name").toString)
  }
}
