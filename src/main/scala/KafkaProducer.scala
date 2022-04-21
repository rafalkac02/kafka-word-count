import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object KafkaProducer extends App {

  val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", 0)
    props.put("batch.size", 16384)
    props.put("linger.ms", 1)
    props.put("buffer.memory", 33554432)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  val producer = new KafkaProducer[String, String](kafkaProducerProps)

  // send messages for words occurrences counting
  producer.send(new ProducerRecord[String, String]("words", "Hello world", "foo"))
  producer.send(new ProducerRecord[String, String]("words", "Kafka is th best trust me", "foo"))
  producer.send(new ProducerRecord[String, String]("words", "Hello world world", "foo"))
  producer.send(new ProducerRecord[String, String]("words", "nice nice nice one", "foo"))

//  for (x <- 0 to 20) {
//    producer.send(new ProducerRecord[String, String]("foo", x.toString, x.toString))
//  }

  producer.close()
}