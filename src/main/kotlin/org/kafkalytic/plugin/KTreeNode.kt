package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.admin.NewTopic
import javax.swing.tree.DefaultMutableTreeNode

const val BROKERS = "Brokers"
const val TOPICS = "Topics"

class KRootTreeNode(userObject: Map<String, String>) : DefaultMutableTreeNode(userObject) {
    private val brokers by lazy {DefaultMutableTreeNode(BROKERS)}
    val topics by lazy {DefaultMutableTreeNode(TOPICS)}
    init {
        add(brokers)
        add(topics)
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
    fun refresh() {
        refreshBrokers()
        refreshTopics()
    }

    private fun expandBrokers() {
        LOG.info("Expand brokers")
        client.describeCluster().nodes().get().forEach {
            brokers.add(DefaultMutableTreeNode(it.idString() + " (" + it.host() + ":" + it.port() + ")"))
            LOG.info("  broker found " + it.idString())
        }
        LOG.info("Expand brokers complete")
    }

    private fun expandTopics() {
        LOG.info("Expand topics")
        val names = client.listTopics().listings().get().filter { !it.isInternal }.map{it.name()}.sorted()
        names.forEach {
            topics.add(KTopicTreeNode(it, this))
            LOG.info("  topic found $it")
        }

        LOG.info("Expand topics complete" + client.describeTopics(names).all().get())
    }

    fun delete(names: Collection<String>) {
        client.deleteTopics(names).all().get()
        refreshTopics()
    }

    fun refreshTopics() {
        topics.removeAllChildren()
        expandTopics()
    }

    fun refreshBrokers() {
        brokers.removeAllChildren()
        expandBrokers()
    }

    override fun toString(): String {
        return getClusterProperties().get("bootstrap.servers") as String
    }
}

val LOG = Logger.getInstance("Kafkalytic")

class KTopicTreeNode(topicName: String, clusterNode: KRootTreeNode) : DefaultMutableTreeNode(topicName) {
    init {
        add(DefaultMutableTreeNode("loading..."))
    }

    val cluster = clusterNode
    fun getTopicName() = userObject as String
    fun getPartitions() = cluster.client.describeTopics(listOf(getTopicName())).all().get().values.first().partitions()
    fun setPartitions(partitions: Int) {
        cluster.client.createPartitions(mapOf(getTopicName() to NewPartitions.increaseTo(partitions)))
        LOG.info("Partitions for topic " + getTopicName() + " changed to " + partitions)
    }

    fun expand() {
        if (!(getChildAt(0) is KPartitionTreeNode)) {
            removeAllChildren()
            getOffsets(cluster.getClusterProperties(), getTopicName()).forEach{ add(KPartitionTreeNode(it.first, it.second)) }
        }
    }
}

class KPartitionTreeNode(id: Int, offset: Long) : DefaultMutableTreeNode ("partition $id offset $offset") {
}
