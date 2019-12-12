package org.kafkalytic.plugin

import com.intellij.notification.Notification
import com.intellij.notification.NotificationType
import com.intellij.notification.Notifications
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis

fun loadOffsets(connection: MutableMap<String, String>, topic: String) : Collection<Pair<Int, Long>> {
    return withConsumer(connection, topic) {consumer ->
        consumer.endOffsets(consumer.listTopics()[topic]?.map { TopicPartition(it.topic(), it.partition()) })
                .map {it.key.partition() to it.value}
    }
}

fun <T> withConsumer(connection: Map<String, String>, topic: String, consume : (KafkaConsumer<ByteArray, ByteArray>) -> T): T {
    val props = mutableMapOf<String, Any>()
    props.putAll(connection)

    props["group.id"] = "kafkalytic"
    props["key.deserializer"] = ByteArrayDeserializer::class.java
    props["value.deserializer"] = ByteArrayDeserializer::class.java
    props["message.max.bytes"] = 10_000_000

    LOG.info("Reading offsets:$connection")
    val consumer = KafkaConsumer<ByteArray, ByteArray>(props as Map<String, Any>)
    consumer.subscribe(listOf(topic))

    val result = consume(consumer)

    consumer.unsubscribe()
    consumer.close()
    return result
}

fun <K, V, R> readUpTo(consumer: KafkaConsumer<K, V>, endOffsets: Map<Int, Long>, process: (ConsumerRecord<K, V>) -> R) : Collection<R> {
    val mutableOffsets = endOffsets.toMutableMap()
    val result = mutableListOf<R>()
    while (mutableOffsets.isNotEmpty()) {
        val records = consumer.poll(Duration.ofSeconds(3))
        val time = measureTimeMillis {
            result.addAll(records.map {
                val endOffset = mutableOffsets[it.partition()]
                if (endOffset != null && endOffset <= it.offset()) {
                    mutableOffsets.remove(it.partition())
                }
                process(it)
            })
        }
        LOG.info("Processed :${records.count()}, time: ${time}")
    }
    return result
}

fun copy(source: MutableMap<String, String>, sourceTopic: String, dest: MutableMap<String, String>, destTopic: String, timestamp: Long, compression: String) {
    withConsumer(source, sourceTopic) {consumer ->
        consumer.poll(Duration.ofSeconds(10))
        val partitions = consumer.listTopics()[sourceTopic]?.map { TopicPartition(sourceTopic, it.partition()) }
        if (partitions != null) {
            val endOffsets = consumer.endOffsets(partitions).map { it.key.partition() to it.value - 1 }.filter { it.second > 0 }.toMap().toMutableMap()
            val targetOffsets = seekToTimestamp(consumer, partitions, timestamp)
            notify("Republishing messages from $sourceTopic to $destTopic\n" + endOffsets.map {
                "partition ${it.key} from offset ${targetOffsets.entries.find { it.key.partition() == it.key.partition() }?.value} to ${it.value}"
            }.joinToString("\n"))
            val futures = mutableListOf<Future<RecordMetadata>>()
            withProducer(dest, compression) {producer ->
                var processed = 0
                readUpTo(consumer, endOffsets) {
                    if (processed > 0 && processed % 1000 == 0) {
                        notify("$processed records republished")
                        val failed = flush(futures)
                        if (failed > 0) {
                            notify("$failed records failed to be republish, check logs...")
                        }
                        futures.clear()
                    }
                    futures.add(producer.send(ProducerRecord<ByteArray, ByteArray>(destTopic, it.key(), it.value())))
                    processed++
                }
                flush(futures)
                notify("Copy finished. Total $processed records republished")
            }
        }
    }
}

fun flush(futures: Collection<Future<RecordMetadata>>): Int {
    var failed = 0
    futures.forEach { f ->
        try {
            f.get()
        } catch (e: ExecutionException) {
            failed++
            LOG.info("publish failed:$e")
        }
    }
    return failed
}

fun <K, V> seekToTimestamp(consumer: KafkaConsumer<K, V>, partitions: List<TopicPartition>, timestamp: Long) =
    consumer.offsetsForTimes(partitions.map { it to timestamp }.toMap()).filter { it.value != null }.also {
        it.forEach {
            consumer.seek(it.key, it.value.offset())
            LOG.info("Partition ${it.key} reset to ${it.value}")
        }
    }

fun search(connection: MutableMap<String, String>, topic: String, pattern: String, timestamp: Long) {
    withConsumer(connection, topic) { consumer ->
        consumer.poll(Duration.ofSeconds(10))

        val partitions = consumer.listTopics()[topic]?.map { TopicPartition(topic, it.partition()) }
        val endOffsets = consumer.endOffsets(partitions).map { it.key.partition() to it.value - 1 }.filter { it.second > 0 }.toMap().toMutableMap()
        var found = false

        if (partitions != null) {
            val targetOffsets = seekToTimestamp(consumer, partitions, timestamp)
            notify("Searching in $topic\n" + endOffsets.map {
                "partition ${it.key} from offset ${targetOffsets.entries.find { it.key.partition() == it.key.partition() }?.value} to ${it.value}"
            }.joinToString("\n"))
            val regexp = Regex(".*$pattern.*")
            readUpTo(consumer, endOffsets) {
                if (regexp.matches(String(it.value()))) {
                    notify("key:${String(it.key())}, partition:${it.partition()}, offset:${it.offset()}, message:${String(it.value())}\n")
                    found = true
                }
                LOG.info("Consumed:${String(it.key())}, offset: ${it.offset()}")
            }
        }
        if (!found) {
            notify("No messages found for pattern $pattern")
        }
//victoriatest@outliersolutions.co.uk
    }
    LOG.info("Reading offsets complete")
}

fun withProducer(connection: Map<String, String>, compression: String, batchSize: Int = 16535, produce: (producer: KafkaProducer<ByteArray, ByteArray>) -> Unit) {
    val props = mutableMapOf<String, Any>()
    props.putAll(connection)
    props.put("acks", "all")
    props.put("retries", 0)
    props.put("batch.size", batchSize)
    props.put("linger.ms", 0)
    props.put("buffer.memory", 33554432)
    props.put("compression.type", compression)
    props.put("key.serializer", ByteArraySerializer::class.java)
    props.put("value.serializer", ByteArraySerializer::class.java)
    props.put("max.request.size", 15728640)
    props.put("request.timeout.ms", 30000)
    val producer = KafkaProducer<ByteArray, ByteArray>(props)
    produce(producer)
    producer.close()
}

fun KRootTreeNode.produce(compression: String, batchSize: Int = 16535, closure: (producer: KafkaProducer<String, ByteArray>) -> Unit) {
    val props = mutableMapOf<String, Any>()
    props.putAll(this.clusterProperties)
    props.put("acks", "all")
    props.put("retries", 0)
    props.put("batch.size", batchSize)
    props.put("linger.ms", 0)
    props.put("buffer.memory", 33554432)
    props.put("compression.type", compression)
    props.put("key.serializer", StringSerializer::class.java)
    props.put("value.serializer", ByteArraySerializer::class.java)
    props.put("max.request.size", 15728640)
    props.put("request.timeout.ms", 30000)
    val producer = KafkaProducer<String, ByteArray>(props)
    closure(producer)
    producer.close()
}

fun produceGeneratedMessages(producer: KafkaProducer<String, ByteArray>, topic: String, template: String, messageSize: Int, messageNumber: Int, delay: Long) {
    val futures = (1..messageNumber).map { current ->
        if (current > 0 && current % (messageNumber / 100) == 0) {
            notify("Produced $current messages for topic $topic")
        }
        if (delay > 0) {
            Thread.sleep(delay)
        }
        val value = messageSize.let { size ->
            StringBuilder().also { buffer ->
                repeat(size) { buffer.append((65 + (Math.random() * 60).roundToInt()).toChar()) }
            }.toString()
        }
        producer.send(ProducerRecord<String, ByteArray>(topic, "message$current", template.replace(RANDOM_PLACEHOLDER, value).toByteArray()))
    }
    notify("Produced total futures ${futures.size}")
    val failures = flush(futures)
    if (failures > 0) {
        notify("failures: $failures")
    }
    notify("Produced total $messageNumber messages for topic $topic")
}

fun produceSingleMessage(producer: KafkaProducer<ByteArray, ByteArray>, topic: String, key: String, value: ByteArray) {
    producer.send(ProducerRecord<ByteArray, ByteArray>(topic, key.toByteArray(), value)).get()
    notify("Published $key")
}

private fun notify(log: String) {
    LOG.info(log)
    Notifications.Bus.notify(Notification("Kafkalytic", "Kafkalytic", log, NotificationType.INFORMATION))
}

val KAFKA_COMPRESSION_TYPES = arrayOf("none", "gzip", "snappy", "lz4", "zstd")