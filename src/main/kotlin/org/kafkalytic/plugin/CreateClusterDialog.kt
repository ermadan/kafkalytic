package org.kafkalytic.plugin

import com.intellij.openapi.ui.InputValidator
import com.intellij.openapi.ui.Messages
import java.awt.BorderLayout
import java.awt.Color
import java.awt.GridLayout
import javax.swing.*
import javax.swing.event.ChangeEvent

class CreateClusterDialog() : Messages.InputDialog(
        "Enter Kafka bootstrap server",
        "New cluster",
        Messages.getQuestionIcon(),
        null,
        object: InputValidator {
            override fun checkInput(inputString: String?) = true
            override fun canClose(inputString: String?) = inputString != null && inputString.length > 0
        }) {

    var trustPath: JTextField? = null
    var keyPath: JTextField? = null
    var trustPassword: JTextField? = null
    var keyPassword: JTextField? = null
    var certCheckbox: JCheckBox? = null

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
        certSubPanel.border = BorderFactory.createLineBorder(Color.GRAY)
        certPanel.add(certSubPanel, BorderLayout.CENTER)

        certCheckbox = JCheckBox("User certificate")
        certCheckbox!!.addChangeListener({event: ChangeEvent ->
            certSubPanel.components.forEach { it.isEnabled =  certCheckbox!!.isSelected}
        })
        certPanel.add(certCheckbox, BorderLayout.NORTH)
        certSubPanel.components.forEach { it.isEnabled =  certCheckbox!!.isSelected}

        messagePanel.add(certPanel, BorderLayout.SOUTH)

        return messagePanel
    }

    fun getCluster(): MutableMap<String, String> {
        if (certCheckbox!!.isEnabled) {
            return hashMapOf("bootstrap.servers" to inputString!!,
                    "security.protocol" to "SSL",
                    "ssl.truststore.location" to trustPath!!.text,
                    "ssl.truststore.password" to trustPassword!!.text,
                    "ssl.keystore.location" to keyPath!!.text,
                    "ssl.keystore.password" to keyPassword!!.text)
        }
        return return hashMapOf("bootstrap.servers" to inputString!!)
    }
}

