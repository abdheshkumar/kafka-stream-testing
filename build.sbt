name := "KafkaTest"
scalaVersion := "2.12.6"
libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.8.1",
  "io.confluent" % "kafka-avro-serializer" % "3.1.1",
  "org.apache.kafka" % "kafka-streams" % "1.1.0",
  "org.apache.kafka" % "kafka-clients" % "1.1.0",
  "org.apache.kafka" % "kafka-streams-test-utils" % "1.1.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)