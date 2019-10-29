package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.fileChooser.FileChooser
import com.intellij.openapi.fileChooser.FileChooserDescriptor
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.InputValidator
import com.intellij.openapi.ui.Messages
import com.intellij.ui.table.JBTable
import java.awt.BorderLayout
import java.io.FileReader
import java.util.*
import javax.swing.*
import javax.swing.event.TableModelEvent
import javax.swing.event.TableModelListener
import javax.swing.table.DefaultTableModel

class CreateClusterDialog(val project: Project) : Messages.InputDialog(
        "Enter Kafka bootstrap server as host:port",
        "New cluster",
        Messages.getQuestionIcon(),
        null,
        object: InputValidator {
            //host:port,
            private val matcher = """([a-zA-Z0-9.-]+:[0-9]{1,5},)*([a-zA-Z0-9-]+\.)*([a-zA-Z0-9-])+:[0-9]{1,5}""".toRegex()
            override fun checkInput(inputString: String?) = inputString != null && matcher.matches(inputString)
            override fun canClose(inputString: String?) = checkInput(inputString)
        }) {

    private val LOG = Logger.getInstance(this::class.java)
    private lateinit var tableModel: DefaultTableModel

    override fun createMessagePanel(): JPanel {
        val messagePanel = JPanel(BorderLayout())
        if (myMessage != null) {
            val textComponent = createTextComponent()
            messagePanel.add(textComponent, BorderLayout.NORTH)
        }

        tableModel = DefaultTableModel()
        tableModel.addColumn("Property")
        tableModel.addColumn("Value")
        tableModel.addTableModelListener {  }
        myField = createTextFieldComponent()
        messagePanel.add(createScrollableTextComponent(), BorderLayout.CENTER)
        val browse = JButton("Load properties from file")
        browse.addActionListener {
            val fcd = FileChooserDescriptor(true, false, false, false, false, false)
            val props = Properties()
            props.load(FileReader(FileChooser.chooseFile(fcd, project, null)?.canonicalPath))
            props.entries.forEach { tableModel.addRow(arrayOf(it.key, it.value)) }
            if (props.containsKey("bootstrap.servers")) {
                myField.text = props.getProperty("bootstrap.servers")
            }
        }
        val subPanel = JPanel(BorderLayout())
        subPanel.add(browse, BorderLayout.NORTH)
        subPanel.add(JBTable(tableModel), BorderLayout.CENTER)
        messagePanel.add(subPanel, BorderLayout.SOUTH)
        return messagePanel
    }

    fun getCluster(): Map<String, String> {
        var props = tableModel.dataVector.elements().asSequence().map { val v = it as Vector<*>; v[0].toString() to v[1].toString() }.toMap()
        if (!inputString.isNullOrEmpty()) {
            props = props.toMutableMap()
            props.put("bootstrap.servers", inputString.toString())
        }
        LOG.info("coonection properties:" + props)
        return props
    }
}


