package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.ui.DialogWrapper
import java.awt.Dimension
import java.awt.GridLayout
import java.awt.event.ActionEvent
import java.awt.event.ActionListener
import java.awt.event.ItemEvent
import java.awt.event.ItemListener
import javax.swing.*
import javax.swing.tree.DefaultMutableTreeNode


class CopyDialog(val topic: String, private val cluster: KRootTreeNode, private val rootNodes: List<KRootTreeNode>) : DialogWrapper(false), ItemListener {
    private lateinit var clusters: ComboBox<KRootTreeNode>
    private lateinit var topics: ComboBox<KTopicTreeNode>
    private lateinit var compression: ComboBox<String>
    private lateinit var timestampPanel: SearchDialog.TimestampPanel

    init {
        setTitle("Choose cluster and topic to copy from $topic")
        init()
    }

    override fun createCenterPanel(): JPanel {
        clusters = ComboBox(rootNodes.toTypedArray())
        clusters.preferredSize = Dimension(200, 24)
        clusters.addItemListener(this)
        topics = ComboBox()
        topics.preferredSize = Dimension(200, 24)
        compression = ComboBox(KAFKA_COMPRESSION_TYPES)
        compression.preferredSize = Dimension(200, 24)
        timestampPanel = SearchDialog.TimestampPanel()

        clusters.selectedItem = cluster
        itemStateChanged(ItemEvent(clusters, 0, cluster, 0))

        val panel = JPanel(GridLayout(0, 2))
        panel.addLabelled("Destination cluster", clusters)
        panel.addLabelled("Destination topic", topics)
        panel.addLabelled("Compression", compression)

        return layoutUD(panel, timestampPanel)
    }

    fun getTimestamp() = timestampPanel.getTimestamp()
    fun getCompression() = compression.selectedItem.toString()
    fun getSelectedCluster()= clusters.selectedItem as KRootTreeNode
    fun getSelectedTopic()= topics.selectedItem as KTopicTreeNode

    override fun itemStateChanged(e: ItemEvent?) {
        if (e?.source == clusters) {
            val root = clusters.selectedItem as KRootTreeNode
            topics.removeAllItems()
            topics.addItem(KTopicTreeNode("loading...", root))
            root.expand()
            (clusters.selectedItem as KRootTreeNode).getTopics().expand()
            topics.removeAllItems()
            (clusters.selectedItem as KRootTreeNode).getTopics().children().toList().forEach {topics.addItem(it as KTopicTreeNode)}
        }
    }
}

