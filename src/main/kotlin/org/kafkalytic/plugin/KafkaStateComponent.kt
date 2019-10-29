package org.kafkalytic.plugin

import com.intellij.openapi.components.PersistentStateComponent
import com.intellij.openapi.components.State
import com.intellij.openapi.components.Storage
import com.intellij.openapi.diagnostic.Logger
import com.intellij.util.xmlb.XmlSerializerUtil

@State(name = "Kafkalytic", storages = [Storage("1kafkalytic2.xml")])
class KafkaStateComponent : PersistentStateComponent<KafkaStateComponent> {
    private val LOG = Logger.getInstance("KafkaStateComponent")

    var clusters : MutableMap<String, Map<String, String>>? = null
    var cluster : Map<String, String>? = null

    fun addCluster(c: Map<String, String>) {
        if (clusters == null) {
            clusters = mutableMapOf()
        }
        clusters?.put(c.get("bootstrap.servers")!!, c)
        cluster = c
    }

    fun removeCluster(cluster: Map<String, String>) {
        clusters?.remove(cluster.get("bootstrap.servers"))
    }


    override fun getState(): KafkaStateComponent {
        LOG.info("Save state:" + clusters)
        return this
    }

    override fun loadState(state: KafkaStateComponent) {
        LOG.info("Load state:" + state)
        XmlSerializerUtil.copyBean(state, this);
        LOG.info("Load state:" + this)
    }

    override fun toString(): String {
        return clusters.toString()
    }

//    fun getClusters() : List<Map<String, String>> {
//        Gson().toJson(clusters)
//    }
}

