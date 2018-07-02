package org.kafkalytic.plugin

import javax.swing.tree.DefaultMutableTreeNode
import org.apache.kafka.clients.admin.AdminClient

class KRootTreeNode(userObject: Map<String, String>) : DefaultMutableTreeNode(userObject) {
    val brokers by lazy {DefaultMutableTreeNode("Brokers")}
    val topics by lazy {DefaultMutableTreeNode("Topics")}
    init {
        add(brokers)
        add(topics)
    }
    val client by lazy{ AdminClient.create(getClusterProperties()) }

    fun getClusterProperties() = (userObject as Map<String, String>).toProperties()

    fun expand() {
        expandBrokers()
        expandTopics()
    }

    private fun expandBrokers() {
        client.describeCluster().nodes().get().forEach {
            brokers.add(DefaultMutableTreeNode(it.idString() + " (" + it.host() + ":" + it.port() + ")"))
        }
    }

    private fun expandTopics() {
        client.listTopics().listings().get().filter { !it.isInternal }.map{it.name()}.sorted().forEach {
            topics.add(KTopicTreeNode(it, this))
        }
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

class KTopicTreeNode(topicName: String, clusterNode: KRootTreeNode) : DefaultMutableTreeNode(topicName) {
    val cluster = clusterNode
    fun getTopicName() = userObject as String
    fun getPartitions() = cluster.client.describeTopics(listOf(getTopicName())).all().get().values.first().partitions()
}

class KBrokerTreeNode() : DefaultMutableTreeNode() {

}