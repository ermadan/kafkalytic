package org.kafkalytic.plugin

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.util.*

fun getOffsets(connection: Properties, topic: String) : Collection<Pair<Int, Long>> {
    val connection = HashMap(connection)
    connection["group.id"] = "kafkalytic"
    connection["key.deserializer"] = ByteArrayDeserializer::class.java
    connection["value.deserializer"] = ByteArrayDeserializer::class.java

    LOG.info("Reading offsets:$connection")
    val consumer = KafkaConsumer<Any, Any>(connection as Map<String, Any>)
    consumer.subscribe(listOf(topic))
    consumer.poll(100)
    val partitions = consumer.assignment().map { Pair(it.partition(), consumer.position(it)) }
    consumer.unsubscribe()
    LOG.info("Reading offsets complete")
    return partitions
}