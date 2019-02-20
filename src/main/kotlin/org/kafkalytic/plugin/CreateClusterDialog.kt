package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.ui.InputValidator
import com.intellij.openapi.ui.Messages
import java.awt.BorderLayout
import java.awt.Color
import java.awt.GridLayout
import javax.swing.*
import javax.swing.event.ChangeEvent

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
        }) {

    private val LOG = Logger.getInstance(this::class.java)
    private lateinit var trustPath: JTextField
    private lateinit var keyPath: JTextField
    private lateinit var trustPassword: JTextField
    private lateinit var keyPassword: JTextField
    private lateinit var certCheckbox: JCheckBox

    override fun createMessagePanel(): JPanel {
        val messagePanel = JPanel(BorderLayout())
        if (myMessage != null) {
            val textComponent = createTextComponent()
            messagePanel.add(textComponent, BorderLayout.NORTH)
        }

        myField = createTextFieldComponent()
        messagePanel.add(createScrollableTextComponent(), BorderLayout.CENTER)

        val certPanel = JPanel(BorderLayout())

        val certSubPanel = JPanel(GridLayout(0, 2))
        trustPath = JTextField()
        keyPath = JTextField()
        trustPassword = JTextField()
        keyPassword = JTextField()
        certSubPanel.add(JLabel("Truststore path"))
        certSubPanel.add(trustPath)
        certSubPanel.add(JLabel("Truststore password"))
        certSubPanel.add(trustPassword)
        certSubPanel.add(JLabel("Keystore path"))
        certSubPanel.add(keyPath)
        certSubPanel.add(JLabel("Keystore password"))
        certSubPanel.add(keyPassword)
        certPanel.add(certSubPanel, BorderLayout.CENTER)

        certCheckbox = JCheckBox("Use certificates to connect:")
        certCheckbox.isSelected = false
        certCheckbox.addChangeListener{
            certSubPanel.components.forEach { it.isEnabled =  certCheckbox.isSelected}
        }
        certPanel.add(certCheckbox, BorderLayout.NORTH)
        certSubPanel.components.forEach { it.isEnabled =  certCheckbox.isSelected}

        messagePanel.add(certPanel, BorderLayout.SOUTH)

        return messagePanel
    }

    fun getCluster(): MutableMap<String, String> {
        LOG.info("is enabled:" + certCheckbox.isSelected)
        if (certCheckbox.isSelected) {
            return hashMapOf("bootstrap.servers" to inputString!!,
                    "security.protocol" to "SSL",
                    "ssl.truststore.location" to trustPath.text,
                    "ssl.truststore.password" to trustPassword.text,
                    "ssl.keystore.location" to keyPath.text,
                    "ssl.keystore.password" to keyPassword.text)
        }
        return hashMapOf("bootstrap.servers" to inputString!!)
    }
}

