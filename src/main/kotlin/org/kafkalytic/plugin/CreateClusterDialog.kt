package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.fileChooser.FileChooser
import com.intellij.openapi.fileChooser.FileChooserDescriptor
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.InputValidator
import com.intellij.openapi.ui.Messages
import com.intellij.ui.table.JBTable
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.KafkaException
import java.awt.*
import java.io.FileReader
import java.io.IOException
import java.util.*
import javax.swing.JButton
import javax.swing.JLabel
import javax.swing.JPanel
import javax.swing.JTextField
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
    private lateinit var zoo: JTextField
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
        myField = HintTextField("host1:port,host2:port")
        messagePanel.add(createScrollableTextComponent(), BorderLayout.CENTER)
        val browse = JButton("Load properties from file")
        val testConnection = JButton("Test connection")
        testConnection.addActionListener {
            try {
                AdminClient.create(getCluster() as Map<String, Any>).close()
                if (zoo.text.isNullOrBlank()) {
                    info("Connection successful")
                }
            } catch (e: KafkaException) {
                info("Cannot connect to Kafka cluster. $e")
            }
            if (!zoo.text.isNullOrBlank()) {
                val split = zoo.text.trim().split("/")
                try {
                    val zoo = ZkUtils.getZk(split[0])
                    val zoopath = "/" + split.subList(1, split.size).joinToString ("/")
                    if (zoo.exists(zoopath, false) == null) {
                        info("Path $zoopath not found.")
                    } else {
                        info("Connection successful")
                    }
                    zoo.close()
                } catch (e: IOException) {
                    info("Cannot connect to Zookeeper cluster. $e")
                }
            }
        }
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
        zoo = HintTextField("host1:port/path_to_kafka_namespace")
        trustPath = HintTextField("local path")
        keyPath = HintTextField("local path")
        trustPassword = JTextField()
        keyPassword = JTextField()
        requestTimeout = JTextField("5000")
        requestTimeout.inputVerifier = INT_VERIFIER
        certSubPanel.addLabelled("zookeeper (enables topic configs, optional)", zoo)
        certSubPanel.addLabelled("Truststore path", trustPath)
        certSubPanel.addLabelled("Truststore password", trustPassword)
        certSubPanel.addLabelled("Keystore path", keyPath)
        certSubPanel.addLabelled("Keystore password", keyPassword)
        certSubPanel.addLabelled("Request timeout, ms", requestTimeout)
        subPanel.add(certSubPanel, BorderLayout.CENTER)
        subPanel.add(layoutUD(browse, JBTable(tableModel), testConnection), BorderLayout.SOUTH)

        messagePanel.add(subPanel, BorderLayout.SOUTH)
        return messagePanel
    }

    fun getCluster(): MutableMap<String, String> {
        var props = tableModel.dataVector.elements().asSequence()
                .map { val v = it as Vector<*>; v[0].toString() to v[1].toString() }.toMap().toMutableMap()
        if (!myField.text.trim().isNullOrEmpty()) {
            props.put("bootstrap.servers", myField.text.trim())
        }
        props.put("name", name.text.ifBlank { props["bootstrap.servers"]!! })
        if (requestTimeout.text.isNotBlank()) {
            props.put("request.timeout.ms", requestTimeout.text)
        }
        props.put(ZOOKEEPER_PROPERTY, zoo.text.trim())
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


