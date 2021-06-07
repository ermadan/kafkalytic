package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import java.util.concurrent.ExecutionException
import javax.swing.tree.DefaultMutableTreeNode
import javax.swing.tree.MutableTreeNode

const val BROKERS = "Brokers"
const val TOPICS = "Topics"
const val CONSUMERS = "Consumers"


interface KafkaTableNode {
    fun headers() = emptyList<String>()
    fun rows() = emptyList<Array<String>>()
}

interface KafkaNode : MutableTreeNode, KafkaTableNode {
    fun refresh()
    fun expand()
}

abstract class KafkaTreeNode(userObject: Any) : DefaultMutableTreeNode(userObject), KafkaTableNode, KafkaNode {
    private val loadingNode = DefaultMutableTreeNode("loading...")

    init {
        add(loadingNode)
    }

    override fun refresh() {
        removeAllChildren()
        add(loadingNode)
        expand()
    }

    private fun getKafkaRoot(): KRootTreeNode {
        var ancestor: DefaultMutableTreeNode? = this
        while (ancestor !is KRootTreeNode) {
            ancestor = ancestor?.parent as DefaultMutableTreeNode?
        }
        return ancestor
    }

    override fun expand() {
        LOG.info("Expand $this")
        if (childCount > 0 && getChildAt(0) == loadingNode) {
            getKafkaRoot().client?.let {
                try {
                    readChildren(it).let {
                        removeAllChildren()
                        it.forEach { c -> add(c) }
                    }
                } catch (e: ExecutionException) {
                    error("" + e.message, e)
                }
            }
        }
        LOG.info("Expand complete $this")
    }

    abstract fun readChildren(client: AdminClient): List<DefaultMutableTreeNode>
}

class KRootTreeNode(val clusterProperties: MutableMap<String, String>) : KafkaTreeNode(clusterProperties) {
    var client: AdminClient? = null

    override fun expand() {
        if (client == null) {
            LOG.info("Creating client with $clusterProperties")
            val contextCL = Thread.currentThread().contextClassLoader
            try {
                Thread.currentThread().contextClassLoader = this::class.java.classLoader
                client = AdminClient.create(clusterProperties as Map<String, Any>)
            } catch (e: KafkaException) {
                error("Cannot connect to Kafka cluster ${clusterProperties["bootstrap.servers"]}", e)
            }
            Thread.currentThread().contextClassLoader = contextCL
        }
        super.expand()
    }

    fun createTopic(name: String, partitions: Int, replications: Short, config: Map<String, String>): CreateTopicsResult? {
        LOG.info("Creating topic $name")
        val newTopic = NewTopic(name, partitions, replications)
        newTopic.configs(config)
        val result = client?.createTopics(listOf(newTopic))
        LOG.info("Creating topic done.")
        return result
    }

    override fun headers() = listOf("Property", "Value")

    override fun rows() =
            (clusterProperties).filter { (k, _) -> k != "bootstrap.servers" }.map { (k, v) -> arrayOf(k, v) }
                    .toMutableList().also {
                        it.addAll(clusterProperties["bootstrap.servers"]
                                .toString()
                                .split(",")
                                .mapIndexed { index, broker -> arrayOf("Broker $index", broker) })
                    }

    override fun refresh() {
        if (client != null) {
            client!!.close()
        }
        client = null
        super.refresh()
    }

    override fun readChildren(client: AdminClient) = listOf(
            object : KafkaTreeNode(BROKERS) {
                override fun readChildren(client: AdminClient) =
                        client.describeCluster().nodes().get().map {
                            object : DefaultMutableTreeNode(it.idString() + " (" + it.host() + ":" + it.port() + ")"), KafkaTableNode {
                                override fun headers() = listOf("Property", "Value")
                                override fun rows() = client?.describeConfigs(listOf(ConfigResource(ConfigResource.Type.BROKER, it.idString())))
                                        ?.all()?.get()?.values?.first()?.entries()?.sortedBy { it.name() }
                                        ?.map { configEntry ->
                                            arrayOf(configEntry.name().toString(), configEntry.value() ?: "")
                                        }
                                        ?: emptyList()
                            }
                        }
            },
            object : KafkaTreeNode(CONSUMERS) {
                override fun readChildren(client: AdminClient) =
                        client.listConsumerGroups().all().get().map { it.groupId() }.map {
                            object : DefaultMutableTreeNode(it), KafkaTableNode {
                                override fun rows(): List<Array<String>> =
                                        client.listConsumerGroupOffsets(getUserObject() as String)
                                                .partitionsToOffsetAndMetadata().get().entries
                                                .map { (k, v) ->
                                                    arrayOf(k.toString(), v.offset().toString(), v.metadata())
                                                }

                                override fun headers() = listOf("Partition", "Offset", "Metadata")
                            }
                        }
            },
            object : KafkaTreeNode(TOPICS) {
                override fun readChildren(client: AdminClient) =
                        client.listTopics(ListTopicsOptions().listInternal(true)).listings().get().map { it.name() }.sorted()
                                .map {
                                    KTopicTreeNode(it, this@KRootTreeNode)
                                }
            }
    )

    fun getTopics() = children.map { it as KafkaTreeNode }.find { it.userObject == TOPICS }!!

    fun delete(names: Collection<String>) {
        client?.let { it.deleteTopics(names).all().get() }
        getTopics().refresh()
    }

    override fun toString() = (clusterProperties["name"] ?: clusterProperties["bootstrap.servers"]) as String
}

val LOG = Logger.getInstance("Kafkalytic")

class KTopicTreeNode(topicName: String, clusterNode: KRootTreeNode) : DefaultMutableTreeNode(topicName), KafkaNode {
    val cluster = clusterNode
    private var offsets: Collection<Pair<Int, Long>>? = null

    fun getTopicName() = userObject as String
    fun getPartitions() = cluster.client.let {
        if (it == null) emptyList() else it.describeTopics(listOf(getTopicName())).all().get().values.first().partitions()
    }

    fun deleteRecords(offset: Long) {
        val result = cluster.client?.deleteRecords(getPartitions().map {
            TopicPartition(getTopicName(), it.partition()) to RecordsToDelete.beforeOffset(offset)
        }.toMap())
        result?.lowWatermarks()?.entries?.forEach {
            try {
                notify("Partition ${it.key} messages deleted before ${it.value.get().lowWatermark()}")
            } catch (e: Exception) {
                notify("Partition ${it.key} error deleting messages $e")
            }
        }
        LOG.info("Removal records before $offset for topic ${getTopicName()} complete")
    }

    fun setPartitions(partitions: Int) {
        cluster.client?.createPartitions(mapOf(getTopicName() to NewPartitions.increaseTo(partitions)))
                ?.values()?.entries?.forEach {
                    try {
                        notify("Partition for topic ${it.key} created")
                    } catch (e: Exception) {
                        notify("Partition for topic ${it.key} not created: $e")
                    }
                }
        LOG.info("Partitions for topic ${getTopicName()} changed to $partitions")
    }

    override fun refresh() {
        LOG.info("topic ${getTopicName()} refresh")
        offsets = loadOffsets(cluster.clusterProperties.toMutableMap(), getTopicName())
    }

    override fun expand() {
        LOG.info("topic ${getTopicName()} expand")
        if (offsets == null) {
            refresh()
        }
    }

    override fun headers() = listOf("Partition Id", "ISR", "Leader", "Offset")

    override fun rows() = getPartitions().map { partition ->
        arrayOf(
                partition.partition().toString(),
                partition.isr().joinToString { it.id().toString() },
                partition.leader().id().toString(),
                if (offsets == null) {
                    "loading..."
                } else {
                    offsets?.find { p -> p.first == partition.partition() }?.second.toString()
                }
        )
    }

    fun zooPropValues() = cluster.client?.describeConfigs(listOf(ConfigResource(ConfigResource.Type.TOPIC,
            getTopicName())))?.all()?.get()?.values?.first()?.entries()?.sortedBy { it.name() }?.map {
        arrayOf(it.name().toString(), it.value() ?: "")
    }
}

