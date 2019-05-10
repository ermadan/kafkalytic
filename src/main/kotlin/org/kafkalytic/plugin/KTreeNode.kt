package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.util.containers.isNullOrEmpty
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.admin.NewTopic
import javax.swing.tree.DefaultMutableTreeNode
import javax.swing.tree.MutableTreeNode

const val BROKERS = "Brokers"
const val TOPICS = "Topics"
const val CONSUMERS = "Consumers"

interface KafkaTableNode {
    fun headers() : List<String> {
        return emptyList()
    }
    fun rows() : List<Array<String>> {
        return emptyList()
    }
}

interface KafkaNode : MutableTreeNode, KafkaTableNode {
    fun refresh()
    fun expand()
}

class KRootTreeNode(userObject: Map<String, String>) : DefaultMutableTreeNode(userObject), KafkaNode {
    private val brokers by lazy { object : DefaultMutableTreeNode(BROKERS), KafkaNode {
        override fun refresh() {
            removeAllChildren()
            expand()
        }

        override fun expand() {
            LOG.info("Expand brokers")
            if (childCount == 0) {
                client.describeCluster().nodes().get().forEach {
                    add(DefaultMutableTreeNode(it.idString() + " (" + it.host() + ":" + it.port() + ")"))
                    LOG.info("  broker found " + it.idString())
                }
                LOG.info("Expand brokers complete")
            } else {
                LOG.info("Brokers already expanded.")
            }
        }
    }}
    private val consumers by lazy { object : DefaultMutableTreeNode(CONSUMERS), KafkaNode {
        override fun refresh() {
            consumers_ = null
            removeAllChildren()
            expand()
        }

        var consumers_ : List<String>? = null

        fun getConsumers() : List<String> {
            if (consumers_.isNullOrEmpty()) {
                consumers_ = client.listConsumerGroups().all().get().map { it.groupId() }
            }
            return consumers_!!
        }

        override fun expand() {
            LOG.info("Expand consumers")
            if (childCount == 0) {
                getConsumers().forEach{
                    add(object : DefaultMutableTreeNode(it), KafkaTableNode {
                        override fun rows() : List<Array<String>> {
                            client.listConsumerGroupOffsets(getUserObject() as String)
                                    .partitionsToOffsetAndMetadata().get().entries
                                    .forEach {
                                        arrayOf(it.key.partition().toString(), it.value.offset().toString(), it.value.metadata())
                                    }
                            return emptyList()
                        }

                        override fun headers(): List<String> {
                            return listOf("Partition", "Offset", "Metadata")
                        }
                    })
                }
                LOG.info("Expand consumers complete")
            } else {
                LOG.info("consumers already expanded.")
            }
        }
        override fun headers() : List<String> {
            return emptyList()
        }
        override fun rows() : List<Array<String>> {
            return emptyList()
        }
    }}
    val topics : KafkaNode by lazy { object : DefaultMutableTreeNode(TOPICS), KafkaNode {
        override fun refresh() {
            removeAllChildren()
            expand()
        }

        override fun expand() {
            LOG.info("Expand topics")
            if (childCount == 0) {
                val names = client.listTopics().listings().get().filter { !it.isInternal }.map { it.name() }.sorted()
                names.forEach {
                    add(KTopicTreeNode(it, this@KRootTreeNode))
                    LOG.info("  topic found $it")
                }
                LOG.info("Expand topics complete" + client.describeTopics(names).all().get())
            }
        }
    }}
    init {
        add(brokers)
        add(topics)
        add(consumers)
    }
    val client by lazy{
        LOG.info("Creating client with " + getClusterProperties())
        AdminClient.create(getClusterProperties())
    }

    fun getClusterProperties() = (userObject as Map<String, String>).toProperties()

    fun createTopic(name: String, partitions: Int, replications: Short) {
        LOG.info("Creating topic " + name)
        client.createTopics(listOf(NewTopic(name, partitions, replications)))
        LOG.info("Creating topic done.")
    }

    override fun headers() : List<String> {
        return listOf("Host", "Port")
    }

    override fun rows() : List<Array<String>> {
        return getClusterProperties()["bootstrap.servers"]
                .toString()
                .split(";")
                .map { it.split(":").toTypedArray() }
    }

    override fun refresh() {
        brokers.refresh()
        topics.refresh()
    }

    override fun expand() {
        brokers.expand()
        topics.expand()
        consumers.expand()
    }

    fun delete(names: Collection<String>) {
        client.deleteTopics(names).all().get()
        topics.refresh()
    }

    override fun toString(): String {
        return getClusterProperties().get("bootstrap.servers") as String
    }
}

val LOG = Logger.getInstance("Kafkalytic")

class KTopicTreeNode(topicName: String, clusterNode: KRootTreeNode) : DefaultMutableTreeNode(topicName), KafkaNode {
    init {
        add(DefaultMutableTreeNode("loading..."))
    }

    val cluster = clusterNode
    var offsets_ : Collection<Pair<Int, Long>>? = null

    fun getOffsets() : Collection<Pair<Int, Long>> {
        if (offsets_ == null) {
            offsets_ = getOffsets(cluster.getClusterProperties(), getTopicName())
        }
        return offsets_!!
    }
    fun getTopicName() = userObject as String
    fun getPartitions() = cluster.client.describeTopics(listOf(getTopicName())).all().get().values.first().partitions()
    fun setPartitions(partitions: Int) {
        cluster.client.createPartitions(mapOf(getTopicName() to NewPartitions.increaseTo(partitions)))
        LOG.info("Partitions for topic " + getTopicName() + " changed to " + partitions)
    }

    override fun refresh() {
        LOG.info("Refresh partitions")
        offsets_ = null
        getOffsets()
        removeAllChildren()
        getOffsets().forEach{ add(KPartitionTreeNode(it.first, it.second)) }
    }

    override fun expand() {
        LOG.info("Expand partitions")
        if (!(getChildAt(0) is KPartitionTreeNode)) {
            removeAllChildren()
            getOffsets().forEach{ add(KPartitionTreeNode(it.first, it.second)) }
        }
    }

    override fun headers() : List<String> {
        return listOf("Partition Id", "ISR", "Leader", "Offset")
    }

    override fun rows() : List<Array<String>> {
        return getPartitions().map {
            arrayOf<String>(
                    it.partition().toString(),
                    it.isr().joinToString { it.id().toString() },
                    it.leader().id().toString(),
                    getOffsets().find { p -> p.first == it.partition() }?.second.toString())
        }
    }
}

class KPartitionTreeNode(id: Int, offset: Long) : DefaultMutableTreeNode ("partition $id offset $offset") {
}
