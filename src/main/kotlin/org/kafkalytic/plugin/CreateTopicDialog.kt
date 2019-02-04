package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.ui.InputValidator
import com.intellij.openapi.ui.Messages
import java.awt.BorderLayout
import java.awt.Color
import java.awt.Dimension
import java.awt.GridLayout
import javax.swing.*
import javax.swing.event.ChangeEvent

class CreateTopicDialog() : Messages.InputDialog(
        "Enter topic name",
        "New topic",
        Messages.getQuestionIcon(),
        null,
        object: InputValidator {
            override fun checkInput(inputString: String?) = true
            override fun canClose(inputString: String?) = inputString != null && inputString.length > 0
        }) {

    private val LOG = Logger.getInstance(this::class.java)
    var partitions: JTextField? = null
    var replication: JTextField? = null

    override fun createMessagePanel(): JPanel {
        val messagePanel = JPanel(BorderLayout())
        if (myMessage != null) {
            val textComponent = createTextComponent()
            messagePanel.add(textComponent, BorderLayout.NORTH)
        }
        myField = createTextFieldComponent()
        myField.preferredSize = Dimension(100, 24)
        messagePanel.add(createScrollableTextComponent(), BorderLayout.CENTER)

        val certPanel = JPanel(GridLayout(2, 2))
        partitions = JTextField()
        partitions?.preferredSize = Dimension(50, 24)
        replication = JTextField()
        replication?.preferredSize = Dimension(50, 24)
        certPanel.add(JLabel("Partitions"))
        certPanel.add(partitions)
        certPanel.add(JLabel("Replication factor"))
        certPanel.add(replication)

        messagePanel.add(certPanel, BorderLayout.SOUTH)

        return messagePanel
    }

    fun getTopic() = inputString!!
    fun getPartitions() = partitions!!.text.toInt()
    fun getReplications() = replication!!.text.toShort()
}
