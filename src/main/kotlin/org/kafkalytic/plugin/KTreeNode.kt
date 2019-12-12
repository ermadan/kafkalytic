package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.RecordsToDelete
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
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
        if (childCount > 0 && getChildAt(0) == loadingNode ) {
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

    abstract fun readChildren(client: AdminClient) : List<DefaultMutableTreeNode>
}

class KRootTreeNode(val clusterProperties: MutableMap<String, String>) : KafkaTreeNode(clusterProperties) {
    var client: AdminClient? = null

    override fun expand() {
        if (client == null) {
            LOG.info("Creating client with $clusterProperties")
            try {
                client = AdminClient.create(clusterProperties as Map<String, Any>)
            } catch (e: KafkaException) {
                error("Cannot connect to Kafka cluster ${clusterProperties["bootstrap.servers"]}", e)
            }
        }
        super.expand()
    }

    fun createTopic(name: String, partitions: Int, replications: Short) {
        LOG.info("Creating topic $name")
        client?.createTopics(listOf(NewTopic(name, partitions, replications)))
        LOG.info("Creating topic done.")
    }

    override fun headers() = listOf("Property", "Value")

    override fun rows() =
            (clusterProperties).filter { (k, _) -> k != "bootstrap.servers"}.map { (k, v) -> arrayOf(k, v)}
                .toMutableList().also {
                    it.addAll(clusterProperties["bootstrap.servers"]
                        .toString()
                        .split(",")
                        .mapIndexed { index, broker -> arrayOf("Broker $index", broker) }) }

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
                    DefaultMutableTreeNode(it.idString() + " (" + it.host() + ":" + it.port() + ")")
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
                client.listTopics().listings().get().filter { !it.isInternal }.map { it.name() }.sorted()
                    .map {
                        KTopicTreeNode(it, this@KRootTreeNode)
                    }
        }
    )

    fun getTopics() = children.map {it as KafkaTreeNode}.find {it.userObject == TOPICS}!!

    fun delete(names: Collection<String>) {
        client?.let { it.deleteTopics(names).all().get() }
        getTopics()?.refresh()
    }

    override fun toString() = (clusterProperties["name"] ?: clusterProperties["bootstrap.servers"]) as String
}

val LOG = Logger.getInstance("Kafkalytic")
class KTopicTreeNode(topicName: String, clusterNode: KRootTreeNode) : DefaultMutableTreeNode(topicName), KafkaNode {
    val cluster = clusterNode
    private var offsets : Collection<Pair<Int, Long>>? = null

    fun getTopicName() = userObject as String
    fun getPartitions() = cluster.client.let {
        if (it == null) emptyList() else it.describeTopics(listOf(getTopicName())).all().get().values.first().partitions()
    }

    fun deleteRecords(offset: Long) {
        cluster.client?.deleteRecords(getPartitions().map {
            TopicPartition(getTopicName(), it.partition()) to RecordsToDelete.beforeOffset(offset)
        }.toMap())
        LOG.info("Removed records before $offset for topic ${getTopicName()}")
    }

    fun setPartitions(partitions: Int) {
        cluster.client?.createPartitions(mapOf(getTopicName() to NewPartitions.increaseTo(partitions)))
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

    override fun headers() =  listOf("Partition Id", "ISR", "Leader", "Offset")

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
}

class KPartitionTreeNode(id: Int, offset: Long) : DefaultMutableTreeNode ("partition $id offset $offset") {
}
