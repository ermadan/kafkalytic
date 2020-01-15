package org.kafkalytic.plugin

import com.google.gson.GsonBuilder
import com.google.gson.JsonSyntaxException
import com.intellij.notification.Notification
import com.intellij.notification.NotificationType
import com.intellij.notification.Notifications
import com.intellij.openapi.progress.ProgressIndicator
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.lang.IllegalStateException
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import kotlin.math.roundToInt
import kotlin.system.measureTimeMillis

fun loadOffsets(connection: MutableMap<String, String>, topic: String) : Collection<Pair<Int, Long>>? {
    return withConsumer(connection, topic) {consumer ->
        consumer.endOffsets(consumer.listTopics()[topic]?.map { TopicPartition(it.topic(), it.partition()) })
                .map {it.key.partition() to it.value}
    }
}

inline fun <T> withConsumer(connection: Map<String, String>, topic: String, consume : (KafkaConsumer<ByteArray, ByteArray>) -> T): T? {
    val props = mutableMapOf<String, Any>()
    props.putAll(connection)

    props["group.id"] = "kafkalytic"
    props["key.deserializer"] = ByteArrayDeserializer::class.java
    props["value.deserializer"] = ByteArrayDeserializer::class.java
    props["message.max.bytes"] = 10_000_000

    LOG.info("Reading offsets:$connection")
    val consumer = KafkaConsumer<ByteArray, ByteArray>(props as Map<String, Any>)
    consumer.subscribe(listOf(topic))
    return try {
        consume(consumer)
    } catch (e: IllegalStateException) {
        info("Communication error $e")
        null
    } finally {
        consumer.unsubscribe()
        consumer.close()
    }
}

fun flush(futures: Collection<Future<RecordMetadata>>, progress: ProgressIndicator): Int {
    var failed = 0
    futures.forEach { f ->
        try {
            f.get()
        } catch (e: ExecutionException) {
            if (!progress.isCanceled) {
                progress.cancel()
            }
            notify("publish failed:$e")
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

inline fun <K, V, R> readUpTo(consumer: KafkaConsumer<K, V>, endOffsets: Map<Int, Long>, process: (ConsumerRecord<K, V>) -> R) : Collection<R> {
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


fun copy(source: MutableMap<String, String>, sourceTopic: String, dest: MutableMap<String, String>, destTopic: String,
         timestamp: Long, compression: String, progress: ProgressIndicator) {
    notify("Copy messages from $sourceTopic to $destTopic starting...")
    withConsumer(source, sourceTopic, timestamp) {consumer, endOffsets ->
        val futures = mutableListOf<Future<RecordMetadata>>()
        withProducer(dest, compression) {producer ->
            var processed = 0
            readUpTo(consumer, endOffsets) {
                if (progress.isCanceled) {
                    LOG.info("Task cancelled")
                    return
                }
                if (processed > 0 && processed % 1000 == 0) {
                    notify("$processed records republished")
                    val failed = flush(futures, progress)
                    if (failed > 0) {
                        notify("$failed records failed to be republish, check logs...")
                    }
                    futures.clear()
                }
                futures.add(producer.send(ProducerRecord<ByteArray, ByteArray>(destTopic, it.key(), it.value())))
                processed++
            }
            flush(futures, progress)
            notify("Copy finished. Total $processed records republished")
        }
    }
}

inline fun <T> withConsumer(connection: Map<String, String>, topic: String, timestamp: Long,
                            consume : (KafkaConsumer<ByteArray, ByteArray>, Map<Int, Long>) -> T) {
    withConsumer(connection, topic) { consumer ->
        consumer.poll(Duration.ofSeconds(100))
        LOG.info("consumer assignements:" + consumer.assignment())
        val partitions = consumer.listTopics()[topic]?.map { TopicPartition(topic, it.partition()) }
        if (partitions != null) {
            val endOffsets = consumer.endOffsets(partitions).map { it.key.partition() to it.value - 1 }.filter { it.second > 0 }.toMap().toMutableMap()
            val targetOffsets = seekToTimestamp(consumer, partitions, timestamp)
            if (targetOffsets.isEmpty()) {
                notify("No messages found for timestamp $timestamp")
            } else {
                notify("Reading messages from $topic " + endOffsets.map {
                    "partition ${it.key} from offset ${targetOffsets.entries.find { it.key.partition() == it.key.partition() }?.value} to ${it.value}"
                }.joinToString("\n"))
                targetOffsets.forEach {
                    if (it.value.offset() >= endOffsets[it.key.partition()] ?: 0) {
                        endOffsets.remove(it.key.partition())
                    }
                }
                consume(consumer, endOffsets)
            }
        }
    }
}

fun search(connection: MutableMap<String, String>, topic: String, keyPattern: String, valuePattern: String,
           timestamp: Long, cancelableTask: ProgressIndicator, config: KafkaStateComponent) {
    var found = false
    withConsumer(connection, topic, timestamp) { consumer, endOffsets ->
        val valueRegexp = Regex(".*$valuePattern.*")
        val keyRegexp = Regex(".*$keyPattern.*")
        readUpTo(consumer, endOffsets) {
            if (cancelableTask.isCanceled) {
                LOG.info("Task cancelled")
                return
            }
            if (valueRegexp.matches(String(it.value())) && keyRegexp.matches(String(it.key())) ) {
                printMessage(config, it)
                found = true
            }
        }
//victoriatest@outliersolutions.co.uk
    }
    if (!found) {
        notify("No messages found for patterns $keyPattern $valuePattern")
    }
    LOG.info("Reading offsets complete")
}

inline fun withProducer(connection: Map<String, String>, compression: String, batchSize: Int = 16535, produce: (producer: KafkaProducer<ByteArray, ByteArray>) -> Unit) {
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

fun produceGeneratedMessages(producer: KafkaProducer<ByteArray, ByteArray>, topic: String, template: String,
                             messageSize: Int, messageNumber: Int, delay: Long, progress: ProgressIndicator) {
    val channel = Channel<Future<RecordMetadata>>()
    GlobalScope.launch {
        // this might be heavy CPU-consuming computation or async logic, we'll just send five squares
        for (current in 1..messageNumber) {
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

            channel.send(producer.send(ProducerRecord<ByteArray, ByteArray>(topic, "message$current".toByteArray(), template.replace(RANDOM_PLACEHOLDER, value).toByteArray())))
            if (progress.isCanceled) {
                channel.close()
                return@launch
            }
        }
    }
    var failures = 0
    var futures = 0
    runBlocking {
        for (f in channel) {
            try {
                f.get()
                futures++
            } catch (e: ExecutionException) {
                notify("Cannot republish ${e}")
                progress.cancel()
                failures++
                LOG.info("publish failed:$e")
            }
        }
        notify("Produced total futures ${futures}")
    }
    if (failures > 0) {
        notify("failures: $failures")
    }
    notify("Produced total $futures messages for topic $topic")
}

fun produceSingleMessage(producer: KafkaProducer<ByteArray, ByteArray>, topic: String, key: String, value: ByteArray) {
    try {
        producer.send(ProducerRecord<ByteArray, ByteArray>(topic, key.toByteArray(), value)).get()
        notify("Published $key")
    } catch (e: ExecutionException) {
        notify("Publish failed: $e")
    }
}

fun notify(log: String) {
    LOG.info(log)
    foreground {
        Notifications.Bus.notify(Notification("Kafkalytic", "Kafkalytic", log, NotificationType.INFORMATION))
    }
}

fun <T> format(s: T) : String {
    val value = when (s) {
        is String -> s
        is ByteArray -> String(s)
        else -> s.toString()
    }
    return try {
        val gson = GsonBuilder().setPrettyPrinting().create()
        gson.toJson(gson.fromJson(value, Map::class.java))
    } catch (e: JsonSyntaxException) {
        value
    }
}

fun <K, V> printMessage(config: KafkaStateComponent, record: ConsumerRecord<K, V>) {
    if (config.config["printToFileSelected"]?.toBoolean() ?: false) {
        Files.write(Paths.get(config.config["printToFile"].toString()), format(record.value()).toByteArray(), StandardOpenOption.APPEND, StandardOpenOption.CREATE)
    }
    Notifications.Bus.notify(Notification("Kafkalytic", "topic:${record.topic()}",
            "key:${format(record.key())}, partition:${record.partition()}, offset:${record.offset()}" +
                    if (config.config["printToEventSelected"]?.toBoolean() ?: true) ", message:\n${format(record.value())}" else "",
            NotificationType.INFORMATION))
}

val KAFKA_COMPRESSION_TYPES = arrayOf("none", "gzip", "snappy", "lz4", "zstd")