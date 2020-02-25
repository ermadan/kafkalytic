package org.kafkalytic.plugin

import com.intellij.openapi.progress.ProgressIndicator
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration


fun consume(topic: String, props: Map<String, Any>, dialog: ConsumeDialog, progress: ProgressIndicator, win: MainWindow) {
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
            consume(consumer, dialog.getWaitFor(), dialog.getPolls(), progress, win)
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
            consume(consumer, dialog.getDecrement() * endOffsets.size, 5,progress, win)
        }
        2 -> {
            val partitions = consumer.partitionsFor(topic)
            consumer.assign(partitions.filter { it.partition() == dialog.getPartition() }
                    .map { TopicPartition(topic, it.partition())})
            consumer.seek(TopicPartition(topic, dialog.getPartition()), dialog.getOffset())
            consume(consumer, 1, 5,  progress, win)
        }
    }
    consumer.unsubscribe()
}

private fun consume(consumer: KafkaConsumer<Any, Any>, howMany : Int, polls: Int, progress: ProgressIndicator, win: MainWindow) {
    var consumed = 0
    repeat(polls) { _ ->
        if (progress.isCanceled) {
            return
        }
        val records = consumer.poll(Duration.ofSeconds(3)) as ConsumerRecords<Any, Any>
        // Handle new records
        LOG.info("polling:" + records.count())
        records.forEach {record ->
            win.printMessage(record)
            consumed++
            if (consumed == howMany) {
                return
            }
            LOG.info("Consumed:${record.key()}")
        }
    }
}
