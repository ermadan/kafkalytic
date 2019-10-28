package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.ui.InputValidator
import com.intellij.openapi.ui.Messages
import java.awt.BorderLayout
import java.awt.Color
import java.awt.GridLayout
import javax.swing.*
import javax.swing.event.ChangeEvent
import javax.swing.event.ChangeListener

class CreateClusterDialog() : Messages.InputDialog(
        "Enter Kafka bootstrap server as host:port",
        "New cluster",
        Messages.getQuestionIcon(),
        null,
        object: InputValidator {
            //host:port,
            private val matcher = """([a-zA-Z0-9.-]+:[0-9]{1,5},)*([a-zA-Z0-9-]+\.)*([a-zA-Z0-9-])+:[0-9]{1,5}""".toRegex()
            override fun checkInput(inputString: String?) = inputString != null && matcher.matches(inputString)
            override fun canClose(inputString: String?) = checkInput(inputString)
        }), ChangeListener {

    private val LOG = Logger.getInstance(this::class.java)
    private lateinit var trustPath: JTextField
    private lateinit var keyPath: JTextField
    private lateinit var trustPassword: JTextField
    private lateinit var keyPassword: JTextField
    private lateinit var saslUser: JTextField
    private lateinit var saslPassword: JTextField
    private lateinit var radioNoAuth: JRadioButton
    private lateinit var radioCertAuth: JRadioButton
    private lateinit var radioSASLAuth: JRadioButton

    override fun stateChanged(e: ChangeEvent?) {
        saslUser.isEnabled = radioSASLAuth.isSelected
        saslPassword.isEnabled = radioSASLAuth.isSelected

        trustPath.isEnabled = radioCertAuth.isSelected
        keyPath.isEnabled = radioCertAuth.isSelected
        trustPassword.isEnabled = radioCertAuth.isSelected
        keyPassword.isEnabled = radioCertAuth.isSelected

        LOG.info("radios:" + radioCertAuth.isSelected + ":" + radioSASLAuth.isSelected)
    }

    override fun createMessagePanel(): JPanel {
        val messagePanel = JPanel(BorderLayout())
        if (myMessage != null) {
            val textComponent = createTextComponent()
            messagePanel.add(textComponent, BorderLayout.NORTH)
        }

        myField = createTextFieldComponent()
        messagePanel.add(createScrollableTextComponent(), BorderLayout.CENTER)

        val certPanel = JPanel(BorderLayout())

        trustPath = JTextField()
        keyPath = JTextField()
        trustPassword = JTextField()
        keyPassword = JTextField()
        saslUser = JTextField()
        saslPassword = JTextField()
        radioNoAuth = JRadioButton("No Auth")
        radioCertAuth = JRadioButton("Certificate Auth")
        radioSASLAuth = JRadioButton("SASL Auth")
        val radioGroup = ButtonGroup()
        radioGroup.add(radioNoAuth)
        radioGroup.add(radioCertAuth)
        radioGroup.add(radioSASLAuth)
        radioNoAuth.addChangeListener(this)
        radioCertAuth.addChangeListener(this)
        radioSASLAuth.addChangeListener(this)

        val certSubPanel = JPanel(GridLayout(0, 2))
        certSubPanel.add(radioNoAuth)
        certSubPanel.add(JLabel(""))
        certSubPanel.add(radioCertAuth)
        certSubPanel.add(JLabel(""))
        certSubPanel.add(JLabel("Truststore path"))
        certSubPanel.add(trustPath)
        certSubPanel.add(JLabel("Truststore password"))
        certSubPanel.add(trustPassword)
        certSubPanel.add(JLabel("Keystore path"))
        certSubPanel.add(keyPath)
        certSubPanel.add(JLabel("Keystore password"))
        certSubPanel.add(keyPassword)
        certPanel.add(certSubPanel, BorderLayout.CENTER)

        val saslSubPanel = JPanel(GridLayout(0, 2))
        saslSubPanel.add(radioSASLAuth)
        saslSubPanel.add(JLabel(""))
        saslSubPanel.add(JLabel("SASL username"))
        saslSubPanel.add(saslUser)
        saslSubPanel.add(JLabel("SASL password"))
        saslSubPanel.add(saslPassword)
        certPanel.add(saslSubPanel, BorderLayout.SOUTH)

        messagePanel.add(certPanel, BorderLayout.SOUTH)
        radioNoAuth.isSelected = true

        return messagePanel
    }

    fun getCluster(): MutableMap<String, String> {
        val props = hashMapOf("bootstrap.servers" to inputString!!)
        if (radioCertAuth.isSelected) {
            props.putAll(mapOf(
                    "security.protocol" to "SSL",
                    "ssl.truststore.location" to trustPath.text,
                    "ssl.truststore.password" to trustPassword.text,
                    "ssl.keystore.location" to keyPath.text,
                    "ssl.keystore.password" to keyPassword.text))
        }
        if (radioCertAuth.isSelected) {
            props.putAll(mapOf(
                    "sasl.mechanism" to "PLAIN",
                    "security.protocol" to "SASL_SSL",
                    "sasl.jaas.config" to "org.apache.kafka.common.security.plain.PlainLoginModule required  username=\""
                            + saslUser + "\" password=\"" + saslPassword + "\""
            ))
        }
        LOG.info("coonection properties:" + props)
        return props
    }
}

