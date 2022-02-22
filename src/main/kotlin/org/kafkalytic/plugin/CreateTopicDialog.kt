package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.ui.InputValidator
import com.intellij.openapi.ui.Messages
import com.intellij.ui.table.JBTable
import java.awt.BorderLayout
import java.awt.Dimension
import java.awt.GridLayout
import java.util.*
import javax.swing.JButton
import javax.swing.JPanel
import javax.swing.JTextField
import javax.swing.table.DefaultTableModel

class CreateTopicDialog() : Messages.InputDialog(
        "Enter topic name",
        "New topic",
        Messages.getQuestionIcon(),
        null,
        object : InputValidator {
            override fun checkInput(inputString: String?) = true
            override fun canClose(inputString: String?) = inputString != null && inputString.isNotEmpty()
        }) {

    private val LOG = Logger.getInstance(this::class.java)
    private lateinit var partitions: JTextField
    private lateinit var replication: JTextField
    private lateinit var compression: ComboBox<String>
    private lateinit var options: OptionsPanel

    override fun createMessagePanel(): JPanel {
        val messagePanel = JPanel(BorderLayout())
        if (myMessage != null) {
            val textComponent = createTextComponent()
            messagePanel.add(textComponent, BorderLayout.NORTH)
        }
        myField = createTextFieldComponent()
        myField.preferredSize = Dimension(100, 24)
        myField.inputVerifier = TOPIC_VERIFIER
        messagePanel.add(createScrollableTextComponent(), BorderLayout.CENTER)

        partitions = JTextField("1")
        partitions.preferredSize = Dimension(50, 24)
        partitions.inputVerifier = INT_VERIFIER
        replication = JTextField("1")
        replication.preferredSize = Dimension(50, 24)
        replication.inputVerifier = INT_VERIFIER

        compression = ComboBox(KAFKA_COMPRESSION_TYPES)
        options = OptionsPanel(arrayOf(
                "cleanup.policy",
                "delete.retention.ms",
                "file.delete.delay.ms",
                "flush.messages",
                "flush.ms",
                "follower.replication.throttled.replicas",
                "index.interval.bytes",
                "leader.replication.throttled.replicas",
                "max.compaction.lag.ms",
                "max.message.bytes",
                "message.format.version",
                "message.timestamp.difference.max.ms",
                "message.timestamp.type",
                "min.cleanable.dirty.ratio",
                "min.compaction.lag.ms",
                "min.insync.replicas",
                "preallocate",
                "retention.bytes",
                "retention.ms",
                "segment.bytes",
                "segment.index.bytes",
                "segment.jitter.ms",
                "segment.ms",
                "unclean.leader.election.enable",
                "message.downconversion.enable"
        ))

        val certPanel = JPanel(GridLayout(0, 2))

        certPanel.addLabelled("Partitions", partitions)
        certPanel.addLabelled("Replication factor", replication)
        certPanel.addLabelled("Compression", compression)


        messagePanel.add(layoutUD(certPanel, options), BorderLayout.SOUTH)

        return messagePanel
    }

    class OptionsPanel(optionNames: Array<String>) : JPanel(BorderLayout()) {

        private var tableModel: DefaultTableModel
        private var options: ComboBox<String>
        private var value: JTextField

        init {
            options = ComboBox(optionNames)
            value = JTextField("")
            tableModel = object : DefaultTableModel() {
                override fun isCellEditable(row: Int, column: Int) = column == 1
            }
            tableModel.addColumn("Property")
            tableModel.addColumn("Value")
            tableModel.addTableModelListener { }


            val addButton = JButton("Add Property")
            addButton.addActionListener {
                if (value.text.isNotBlank()) {
                    tableModel.addRow(arrayOf(options.selectedItem, value.text.trim()))
                    options.removeItem(options.selectedItem)
                } else {
                    Messages.showInfoMessage("Property value cannot be blank", "Kafka")
                }
            }
            add(addButton, BorderLayout.NORTH)
            add(options, BorderLayout.WEST)
            add(value, BorderLayout.CENTER)
            add(JBTable(tableModel), BorderLayout.SOUTH)
        }

        fun getConfig() =
                tableModel.dataVector.elements().asSequence()
                        .map { val v = it as Vector<*>; v[0].toString() to v[1].toString() }.toMap()

    }

    fun getTopic() = inputString!!
    fun getPartitions() = partitions.text.toInt()
    fun getReplications() = replication.text.toShort()
    fun getConfig() = options.getConfig().toMutableMap().also {
        if (compression.selectedItem.toString() != "none")
            it.put("compression.type", compression.selectedItem.toString())
    }
}
