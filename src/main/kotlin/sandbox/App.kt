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

data class Org(
  val id: Long,
  val name: String
)

data class OrgUser(
  val id: Long,
  val userId: Long,
  val orgId: Long
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

    // Create the serializers/deserializers
    val mapper = ObjectMapper()
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .registerModule(KotlinModule())
    val userSerdes = Jackson2Serde(mapper, User::class.java)
    val orgSerdes = Jackson2Serde(mapper, Org::class.java)
    val orgUserSerdes = Jackson2Serde(mapper, OrgUser::class.java)
    val strSerializer = Serdes.String().serializer()

    // Create streams for each of the CDC topics
    val builder = StreamsBuilder()
    val usersStream = builder.stream("users", Consumed.with(Serdes.Long(), userSerdes))
    val orgStream = builder.stream("orgs", Consumed.with(Serdes.Long(), orgSerdes))
    val orgUserStream = builder.stream("org_users", Consumed.with(Serdes.Long(), orgUserSerdes))

    val filtered = usersStream.filter { _, value -> value.lastName.startsWith("k") }
    filtered.to("output-topic")

    val topology = builder.build()
    val driver = TopologyTestDriver(topology, props)

    val factory = ConsumerRecordFactory(Serdes.Long().serializer(), strSerializer)
    driver.pipeInput(factory.create("users", 1L, mapper.writeValueAsString(User(1, "kid"))))

    val record = driver.readOutput("output-topic", Serdes.Long().deserializer(), userSerdes.deserializer())
    println(record)

    val users = builder.table("users", Consumed.with(Serdes.Long(), userSerdes))

    val orgTable = builder.table("orgs", Consumed.with(Serdes.Long(), orgSerdes))

    val orgUserTable = builder.table("org_users", Consumed.with(Serdes.Long(), orgUserSerdes))

    

  }

}

fun main(args: Array<String>) {
  println(App().greeting)
  App().main()
}
