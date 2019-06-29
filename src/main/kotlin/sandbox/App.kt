package sandbox

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.ConsumerRecordFactory
import java.util.*

class App {
  val greeting: String
    get() {
      return "Hello world."
    }

  fun main() {
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "sandbox"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

    val builder = StreamsBuilder()
    val source = builder.stream<String, String>("input-topic")
    val filtered = source.filter { _, value -> value.startsWith("k") }
    filtered.to("output-topic")

    val topology = builder.build()
    val driver = TopologyTestDriver(topology, props)

    val strSerializer = Serdes.String().serializer()
    val strDeserializer = Serdes.String().deserializer()
    val factory = ConsumerRecordFactory(strSerializer, strSerializer)
    driver.pipeInput(factory.create("input-topic", "key", "kalue"))

    val record = driver.readOutput("output-topic", strDeserializer, strDeserializer)
    println(record)
  }

}

fun main(args: Array<String>) {
  println(App().greeting)
  App().main()
}
