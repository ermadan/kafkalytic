package org.kafkalytic.plugin

import com.intellij.notification.Notification
import com.intellij.notification.NotificationType
import com.intellij.notification.Notifications
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.progress.ProgressIndicator
import com.intellij.openapi.progress.Task
import com.intellij.openapi.project.Project
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.*

abstract class ConsumerDecrement(project: Project, val topic: String, val props: Properties,
                        val keyDeserializer: String, val valueDeserializer: String)
    : Task.Backgroundable(project, "Consume from " + topic, true) {
    private val LOG = Logger.getInstance(this::class.java)

    override fun run(indicator: ProgressIndicator) {
        val local = Properties(props)

        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", Class.forName(keyDeserializer));
        props.put("value.deserializer", Class.forName(valueDeserializer));
        props.put("max.poll.records",1);

        LOG.info(local.toString())
        LOG.info(props.toString())
        val consumer = KafkaConsumer<Any, Any>(props)
        prepare(consumer)

        LOG.info("background task complete:" + title)
        consumer.unsubscribe()
    }

    abstract fun prepare(consumer: KafkaConsumer<Any, Any>)

    protected fun consume(consumer: KafkaConsumer<Any, Any>, howMany : Int, polls: Int = 5) {
        var consumed = 0
        (0..polls).forEach {
            val records = consumer.poll(1000) as ConsumerRecords<String, ByteArray>
            // Handle new records
            LOG.info("polling:" + records.count())
            records.forEach {
                Notifications.Bus.notify(Notification("Kafkalytic", "topic:" + topic, "key:" + it.key()
                        + ", partition:" + it.partition() + ", offset:" + it.offset() + ", message:" + String(it.value()), NotificationType.INFORMATION));
                consumed++
                if (consumed == howMany) {
                    return
                }
                System.out.println(it.key());
            }
        }
    }

    override fun onCancel() {
        LOG.info("background task complete:" + title)
        super.onCancel()
    }
}

class WaitMessageConsumer(project: Project, topic: String, props: Properties,
                              keyDeserializer: String, valueDeserializer: String,
                              val howMany: Int, val polls: Int) :
        ConsumerDecrement(project, topic, props, keyDeserializer, valueDeserializer) {
    override fun prepare(consumer: KafkaConsumer<Any, Any>) {
        consumer.subscribe(listOf(topic))
        consume(consumer, howMany)
    }
}

class SpecificMessageConsumer(project: Project, topic: String, props: Properties,
                              keyDeserializer: String, valueDeserializer: String,
                              val partition: Int, val offset: Long) :
        ConsumerDecrement(project, topic, props, keyDeserializer, valueDeserializer) {
    override fun prepare(consumer: KafkaConsumer<Any, Any>) {
        val partitions = consumer.partitionsFor(topic)
        consumer.assign(partitions.filter { it.partition() == partition }
                .map { TopicPartition(topic, it.partition())})
        consumer.seek(TopicPartition(topic, partition), offset)
        consume(consumer, 1)
    }
}

class RecentMessageConsumer(project: Project, topic: String, props: Properties,
                              keyDeserializer: String, valueDeserializer: String,
                              val decrement: Int) :
        ConsumerDecrement(project, topic, props, keyDeserializer, valueDeserializer) {
    override fun prepare(consumer: KafkaConsumer<Any, Any>) {
        consumer.subscribe(listOf(topic))
        consumer.poll(0)
        val assignments = consumer.assignment()
        val endOffsets = consumer.endOffsets(assignments);
        endOffsets.forEach{ partition, offset ->
            val decrement = offset - decrement
            consumer.seek(partition, if (decrement < 0) 0 else decrement);
        }
        consume(consumer, decrement * endOffsets.size)
    }
}