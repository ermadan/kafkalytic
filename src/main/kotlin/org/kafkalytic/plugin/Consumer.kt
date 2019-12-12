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
import java.time.Duration
import java.util.*

fun consume(topic: String, props: Map<String, Any>, dialog: ConsumeDialog) {
    val local = props.toMutableMap()

    local.put("group.id", "kafkalytic")
    local.put("enable.auto.commit", "false")
    local.put("session.timeout.ms", "30000")
    local.put("key.deserializer", Class.forName(dialog.getKeyDeserializer()))
    local.put("value.deserializer", Class.forName(dialog.getValueDeserializer()))
    local.put("max.poll.records", 1)

    LOG.info(local.toString())
    LOG.info(props.toString())
    val consumer = KafkaConsumer<Any, Any>(local)
    when (dialog.getMode()) {
        0 -> {
            consumer.subscribe(listOf(topic))
            consume(consumer, topic, dialog.getWaitFor(), dialog.getPolls())
        }
        1 -> {
            consumer.subscribe(listOf(topic))
            consumer.poll(Duration.ofSeconds(10))
            val assignments = consumer.assignment()
            val endOffsets = consumer.endOffsets(assignments)
            LOG.info("Iterating partitions with offsets $endOffsets")
            endOffsets.forEach{ (partition, offset) ->
                consumer.seek(partition, if (dialog.getDecrement() > offset) 0 else offset - dialog.getDecrement())
            }
            consume(consumer, topic, dialog.getDecrement() * endOffsets.size)
        }
        2 -> {
            val partitions = consumer.partitionsFor(topic)
            consumer.assign(partitions.filter { it.partition() == dialog.getPartition() }
                    .map { TopicPartition(topic, it.partition())})
            consumer.seek(TopicPartition(topic, dialog.getPartition()), dialog.getOffset())
            consume(consumer, topic, 1)
        }
    }
    consumer.unsubscribe()
}

private fun consume(consumer: KafkaConsumer<Any, Any>, topic: String, howMany : Int, polls: Int = 5) {
    var consumed = 0
    repeat(polls) { _ ->
        val records = consumer.poll(Duration.ofSeconds(3)) as ConsumerRecords<Any, Any>
        // Handle new records
        LOG.info("polling:" + records.count())
        records.forEach {
            Notifications.Bus.notify(Notification("Kafkalytic", "topic:$topic",
                    "key:${it.key()}, partition:${it.partition()}, offset:${it.offset()}, message:${it.value()}",
                    NotificationType.INFORMATION))
            consumed++
            if (consumed == howMany) {
                return
            }
            LOG.info("Consumed:$it.key()")
        }
    }
}
