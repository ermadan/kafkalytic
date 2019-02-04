package org.kafkalytic.plugin

import com.google.gson.Gson
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer

fun main(argv: Array<String>) {
//    val cl = AdminClient.create(Cluster("scoreu01.uk.db.com:9093",
//            "c:/temp/scoreu01.uk.db.com.jks", "sdcSd/dU0ICbqdir+mI5UA",
//            "c:/temp/scoreu01.uk.db.com.jks", "sdcSd/dU0ICbqdir+mI5UA"
//            ).toProperties())
//    println (cl.describeCluster().brokers().get())
    val consumer = KafkaConsumer<Any, Any>(mapOf("bootstrap.servers" to "localhost:9092",
            "group.id" to "test",
            "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer"))
    consumer.subscribe(".*".toRegex().toPattern())
//    consumer.poll(100)

    println(consumer.assignment().map{ consumer.position(it)})
}