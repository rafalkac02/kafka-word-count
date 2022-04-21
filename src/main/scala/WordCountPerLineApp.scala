import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util
import java.util.Properties
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters.ScalaDurationOps

object WordCountPerLineApp extends App {


  def occurences(s: String): Map[String, Int] = s.split(" +").foldLeft(Map.empty[String, Int]){
    (words, w) => words + (w -> (words.getOrElse(w, 0) + 1))
  }

  val propsConsumer: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  val propsProducer: Properties = {
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

  val consumer = new KafkaConsumer[String, String](propsConsumer)
  consumer.subscribe(List("words").asJava)


  // produce records and send to topic "myOutput"
  val records /*: ConsumerRecords[String, String]*/ = consumer.poll(100).asScala
  val producer = new KafkaProducer[String, String](propsProducer)

//  while (true) {
    var counter = 0
    records.foreach { record =>
      producer.send(new ProducerRecord[String, String]("myOutput", s"record $counter", occurences(record.key()).toString))
      counter += 1
    }
//  }



  producer.close()
  consumer.close()
}