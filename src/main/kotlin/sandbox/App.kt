package sandbox

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.giladam.kafka.jacksonserde.Jackson2Serde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.test.ConsumerRecordFactory
import java.util.*


data class User(
  val id: Long,
  val lastName: String
)

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

    val mapper = ObjectMapper()
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .registerModule(KotlinModule())
    val userSerdes = Jackson2Serde(mapper, User::class.java)
    val strSerializer = Serdes.String().serializer()
    val strDeserializer = Serdes.String().deserializer()

    val builder = StreamsBuilder()
    val usersStream = builder.stream<String, User>("users", Consumed.with(Serdes.String(), userSerdes))
    val filtered = usersStream.filter { _, value -> value.lastName.startsWith("k") }
    filtered.to("output-topic")

    val topology = builder.build()
    val driver = TopologyTestDriver(topology, props)

    val factory = ConsumerRecordFactory(strSerializer, strSerializer)
    driver.pipeInput(factory.create("users", "key", mapper.writeValueAsString(User(1, "kid"))))

    val record = driver.readOutput("output-topic", strDeserializer, userSerdes.deserializer())
    println(record)

    val users = builder.table("users", Consumed.with(Serdes.String(), userSerdes))
  }

}

fun main(args: Array<String>) {
  println(App().greeting)
  App().main()
}
