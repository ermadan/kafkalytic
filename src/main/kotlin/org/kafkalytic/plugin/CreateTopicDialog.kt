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
            override fun canClose(inputString: String?) = inputString != null && inputString.isNotEmpty()
        }) {

    private val LOG = Logger.getInstance(this::class.java)
    private lateinit var partitions: JTextField
    private lateinit var replication: JTextField

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
        val certPanel = JPanel(GridLayout(2, 2))
        certPanel.addLabelled("Partitions", partitions)
        certPanel.addLabelled("Replication factor", replication)

        messagePanel.add(certPanel, BorderLayout.SOUTH)

        return messagePanel
    }

    fun getTopic() = inputString!!
    fun getPartitions() = partitions!!.text.toInt()
    fun getReplications() = replication!!.text.toShort()
}
