package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.fileChooser.FileChooser
import com.intellij.openapi.fileChooser.FileChooserDescriptor
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.InputValidator
import com.intellij.openapi.ui.Messages
import com.intellij.ui.table.JBTable
import java.awt.BorderLayout
import java.awt.GridLayout
import java.io.FileReader
import java.util.*
import javax.swing.*
import javax.swing.event.TableModelEvent
import javax.swing.event.TableModelListener
import javax.swing.table.DefaultTableModel

class CreateClusterDialog(val project: Project) : Messages.InputDialog(
        "Enter Kafka bootstrap servers as host:port,host2:port",
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
    private lateinit var name: JTextField
    private lateinit var tableModel: DefaultTableModel
    private lateinit var trustPath: JTextField
    private lateinit var keyPath: JTextField
    private lateinit var trustPassword: JTextField
    private lateinit var keyPassword: JTextField
    private lateinit var requestTimeout: JTextField


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
            val file = FileChooser.chooseFile(fcd, project, null)
            if (file != null) {
                props.load(FileReader(file.canonicalPath))
                props.entries.forEach { tableModel.addRow(arrayOf(it.key, it.value)) }
                props.getProperty("bootstrap.servers")?.let { myField.text = it }
                props.getProperty("ssl.truststore.location")?.let { trustPath.text = it }
                props.getProperty("ssl.truststore.password")?.let { trustPassword.text = it }
                props.getProperty("ssl.keystore.location")?.let { keyPath.text = it }
                props.getProperty("ssl.keystore.password")?.let { keyPassword.text = it }
            }
        }
        name = JTextField()
        val subPanel = JPanel(BorderLayout())
        subPanel.add(layoutLR(JLabel("Cluster name (optional)"), name), BorderLayout.NORTH)
        val certSubPanel = JPanel(GridLayout(0, 2))
        trustPath = JTextField()
        keyPath = JTextField()
        trustPassword = JTextField()
        keyPassword = JTextField()
        requestTimeout = JTextField("5000")
        certSubPanel.add(JLabel("Truststore path"))
        certSubPanel.add(trustPath)
        certSubPanel.add(JLabel("Truststore password"))
        certSubPanel.add(trustPassword)
        certSubPanel.add(JLabel("Keystore path"))
        certSubPanel.add(keyPath)
        certSubPanel.add(JLabel("Keystore password"))
        certSubPanel.add(keyPassword)
        certSubPanel.add(JLabel("Request timeout, ms"))
        certSubPanel.add(requestTimeout)
        subPanel.add(certSubPanel, BorderLayout.CENTER)
        subPanel.add(layoutUD(browse, JBTable(tableModel)), BorderLayout.SOUTH)

        messagePanel.add(subPanel, BorderLayout.SOUTH)
        return messagePanel
    }

    fun getCluster(): MutableMap<String, String> {
        var props = tableModel.dataVector.elements().asSequence()
                .map { val v = it as Vector<*>; v[0].toString() to v[1].toString() }.toMap().toMutableMap()
        if (!inputString.isNullOrEmpty()) {
            props.put("bootstrap.servers", inputString.toString())
        }
        props.put("name", name.text.ifBlank { props["bootstrap.servers"]!! })
        if (requestTimeout.text.isNotBlank()) {
            props.put("request.timeout.ms", requestTimeout.text)
        }
        if (trustPath.text.isNotBlank()) {
            props.putAll(mapOf(
                    "security.protocol" to "SSL",
                    "ssl.truststore.location" to trustPath.text,
                    "ssl.truststore.password" to trustPassword.text,
                    "ssl.keystore.location" to keyPath.text,
                    "ssl.keystore.password" to keyPassword.text))
        }

        LOG.info("coonection properties:$props")
        return props
    }
}


