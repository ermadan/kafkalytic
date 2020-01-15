package org.kafkalytic.plugin

import com.intellij.openapi.components.PersistentStateComponent
import com.intellij.openapi.components.State
import com.intellij.openapi.components.Storage
import com.intellij.openapi.diagnostic.Logger
import com.intellij.util.xmlb.XmlSerializerUtil

@State(name = "Kafkalytic", storages = [Storage("1kafkalytic2.xml")])
class KafkaStateComponent : PersistentStateComponent<KafkaStateComponent> {
    private val LOG = Logger.getInstance("KafkaStateComponent")

    var clusters : MutableMap<String, MutableMap<String, String>> = mutableMapOf()
    var config = mutableMapOf<String, String>()

    fun addCluster(c: Map<String, String>) {
        val cluster = c.toMutableMap()
        cluster.putIfAbsent("name", c["bootstrap.servers"]!!)
        clusters[cluster["name"] ?: error("Bootstrap servers cannot be null")] = cluster
    }

    fun removeCluster(cluster: Map<String, String>) {
        clusters.remove(cluster.get("name"))
    }

    override fun getState(): KafkaStateComponent {
        LOG.info("Save state:$this")
        return this
    }

    override fun loadState(state: KafkaStateComponent) {
        LOG.info("Load state:$state")
        //upgrade to configuration with name not present originally
        state.clusters = state.clusters.map { (k, m) -> m.putIfAbsent("name", m["bootstrap.servers"]!!); k to m}
                .toMap().toMutableMap()
        XmlSerializerUtil.copyBean(state, this);
        LOG.info("Load state:$this")
    }

    override fun toString(): String {
        return clusters.toString() + config.toString()
    }

//    fun getClusters() : List<Map<String, String>> {
//        Gson().toJson(clusters)
//    }
}

