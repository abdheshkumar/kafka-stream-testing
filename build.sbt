val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}

val KAFKA_VERSION = "2.6.0"
name := "KafkaTest"
scalaVersion := "2.13.1"
libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % KAFKA_VERSION,
  "org.apache.kafka" % "kafka-clients" % KAFKA_VERSION,
  "org.apache.kafka" % "kafka-streams-test-utils" % KAFKA_VERSION % Test,
  "org.scalatest" %% "scalatest" % "3.2.0" % Test
)
