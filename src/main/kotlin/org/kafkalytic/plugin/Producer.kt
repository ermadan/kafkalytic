package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.progress.ProgressIndicator
import com.intellij.openapi.progress.Task
import com.intellij.openapi.project.Project
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer

class Producer(project: Project, val topic: String, val props: Properties, val key: String, val value: ByteArray)
    : Task.Backgroundable(project, "Consume from " + topic, true) {
    private val LOG = Logger.getInstance(this::class.java)

    override fun run(indicator: ProgressIndicator) {
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "none");
        props.put("key.serializer", StringSerializer::class.java)
        props.put("value.serializer", ByteArraySerializer::class.java)
        props.put("max.request.size", 15728640);

        val producer = KafkaProducer<String, ByteArray>(props)

        producer.send(ProducerRecord<String, ByteArray>(topic, key, value))
        LOG.info("sent:" + key)
        producer.close()
    }
}