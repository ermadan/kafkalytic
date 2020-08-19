package org.kafkalytic.plugin

import com.intellij.openapi.Disposable
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.WindowWrapper
import com.intellij.openapi.ui.WindowWrapperBuilder
import com.intellij.openapi.util.Disposer
import com.intellij.ui.components.JBScrollPane
import com.intellij.ui.table.JBTable
import com.intellij.util.ImageLoader
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition
import java.awt.Dimension
import java.util.concurrent.TimeUnit
import javax.swing.JLabel
import javax.swing.JPanel
import javax.swing.JTextField
import javax.swing.table.DefaultTableModel


class ProgressDialog(val topic: String, client: AdminClient, private val mainWindow: MainWindow, private val project: Project) {
    private val LOG = Logger.getInstance("Kafkalytic")
    lateinit var updateFrequency: JTextField
    private lateinit var table: JBTable
    private lateinit var countdown: JLabel
    private val model = OffsetsTableModel(this, topic, client)
    private var cancelled = false

    private fun update() {
        ApplicationManager.getApplication().invokeLater {
            model.updateDetails()
            table.setShowColumns(true)
            LOG.info("updated")
        }
    }

    fun show() {
        val processor = Disposable { cancelled = true }

        val wrapper = WindowWrapperBuilder(WindowWrapper.Mode.FRAME, createCenterPanel())
                .setProject(project)
                .setParent(mainWindow)
                .setDimensionServiceKey("Kafkalytic")
                .setOnShowCallback { update() }
                .build()
        wrapper.setImages(listOf(ImageLoader.loadFromResource("/icons/kafka.png")))
        Disposer.register(wrapper, processor)
        wrapper.setTitle("Consumption progress for topic $topic")
        wrapper.show()
    }

    private fun createCenterPanel(): JPanel {
        table = JBTable(model)
        table.emptyText.text = "Loading offsets and consumer information"
        table.fillsViewportHeight = true
        table.setShowColumns(true)
        updateFrequency = JTextField()
        updateFrequency.text = "30"
        updateFrequency.preferredSize = Dimension(90, 24)
        updateFrequency.inputVerifier = INT_VERIFIER
        countdown = JLabel("0")
        Thread {
            LOG.info("start reading")
            while (!cancelled) {
                LOG.info("updating:$topic:$cancelled")
                update()
                var countdownValue = updateFrequency.text.toInt()
                repeat(countdownValue) {
                    Thread.sleep(1000)
                    countdown.text = countdownValue--.toString()
                    if (cancelled) {
                        return@repeat
                    }
                }
            }
            LOG.info("cancelled, exiting: $topic")
        }.start()
        return layoutUD(
                layoutLR(layoutLR(JLabel("Update every "), updateFrequency, JLabel(" seconds")), JLabel()),
                JBScrollPane(table),
                layoutLR(JLabel("Consumer columns displays current offset/remaining messages/messages per second"), JLabel(), layoutLR(JLabel("time remaining to update: "), countdown)))
    }
}

class OffsetsTableModel(val dialog: ProgressDialog, val topicName: String, private val client: AdminClient) : DefaultTableModel() {
    private val LOG = Logger.getInstance("Kafkalytic")
    private var consumerRemaining : List<MutableMap<Int, Long>>? = null
    fun updateDetails() {
        LOG.info("updating")
        val partitions = client.describeTopics(listOf(topicName)).all().get().values.first().partitions()
        LOG.info("updating2")
//        partitions.forEach { addColumn(it.partition()) }
        LOG.info("updating3")
        val listOffsets = client.listOffsets(partitions.map { TopicPartition(topicName, it.partition()) to OffsetSpec.latest() }.toMap())
        LOG.info("updating4")
        val offsets = listOffsets.all().get(5, TimeUnit.SECONDS)
        LOG.info("updating4.1")
        val sortedBy = offsets.entries.sortedBy { it.key.partition() }
        LOG.info("updating4.2")
        val topicOffsets = sortedBy.map { it.value.offset().toString() }
        LOG.info("updating4.3")
//        addRow(topicOffsets.toTypedArray())

        LOG.info("updating5")
        val consumers = client.listConsumerGroups().all().get().map { consumerGroup ->
            LOG.info("consumer0:" + consumerGroup.groupId())
            val consumerPartitions = client.listConsumerGroupOffsets(consumerGroup.groupId())
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS).entries
                    .filter { it.key.topic() == topicName }
                    .sortedBy { it.key.partition() }
                    .map { it.value.offset().toString() }
            LOG.info("partitions:$consumerPartitions")
            consumerPartitions
                    .toMutableList().also { it.add(0, consumerGroup.groupId()) }
        }.filter { it.size > 1 }
//        .forEach{LOG.info("found offsets:" + it); addRow(it.toTypedArray())}
        LOG.info("updating6")
        LOG.info("updating8")
        dataVector.clear()
        columnIdentifiers.clear()
        addColumn("Partition")
        addColumn("Topic latest offsets")

        LOG.info("updating6" + consumers.size)
        consumers.forEach {  LOG.info("updating6" + it[0]);addColumn(it[0]) }
        if (consumerRemaining == null || consumerRemaining!!.size < consumers.size) {
            consumerRemaining = consumers.map { mutableMapOf<Int, Long>() }
        }
        LOG.info("updating7")
        val rows = topicOffsets.mapIndexed { index, s -> mutableListOf(partitions[index], s).also { it.addAll(consumers.mapIndexed { consumerIndex, consumerOffsets ->
            val partitionIndex = index + 1
            val currentOffset = consumerOffsets[partitionIndex]
            val remaining = s.toLong() - currentOffset.toLong()
            val delta = (consumerRemaining!![consumerIndex][partitionIndex]?:0) - remaining
            val value = currentOffset + " / " + remaining + " / " + (if (delta > 0) delta/dialog.updateFrequency.text.toInt() else 0)
            consumerRemaining!![consumerIndex][partitionIndex] = remaining
            value
        }) } }
        LOG.info("updating7.1")
        rows.forEach {
            LOG.info("row added")
            LOG.info("row added: $it")
            addRow(it.toTypedArray()) }
        LOG.info("updating8")
        fireTableStructureChanged()
    }
}
