package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.fileChooser.FileChooser
import com.intellij.openapi.fileChooser.FileChooserDescriptor
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.ui.DialogWrapper
import com.intellij.util.ui.UIUtil
import java.awt.Color
import java.awt.Dimension
import java.awt.GridLayout
import java.awt.event.FocusEvent
import java.awt.event.FocusListener
import java.nio.file.Files
import java.nio.file.Paths
import javax.swing.JButton
import javax.swing.JPanel
import javax.swing.JTextArea
import javax.swing.JTextField

val RANDOM_PLACEHOLDER = """<random>"""

class GeneratorDialog(val project: Project, topic: String) : DialogWrapper(false)/*, ChangeListener*/ {

    private val LOG = Logger.getInstance(this::class.java)

    public final var PLACE_HOLDER: String = "key:value;";
    private lateinit var header: HintTextField
    private lateinit var numberOfMessages: JTextField
    private lateinit var delay: JTextField
    private lateinit var batchSize: JTextField
    private lateinit var messageSize: JTextField
    private lateinit var template: JTextArea
    private lateinit var compression: ComboBox<String>

    init {
        setTitle("Configure Kafka message generator for topic $topic")
        init()
    }

    override fun createCenterPanel(): JPanel {
        delay = JTextField("10")
        delay.setInputVerifier(LONG_VERIFIER)
        delay.preferredSize = Dimension(200, 24)

        header = HintTextField(PLACE_HOLDER)
        header.preferredSize = Dimension(200, 24)
        header.setForeground(Color.GRAY)

        template = JTextArea(10, 43)
        template.text = """
            example template with $RANDOM_PLACEHOLDER that 
            will be replaced with randomized content
        """.trimIndent()
        template.lineWrap = true

        numberOfMessages = JTextField("100")
        numberOfMessages.setInputVerifier(INT_NON_ZERO_VERIFIER)
        numberOfMessages.preferredSize = Dimension(200, 24)

        messageSize = JTextField("100")
        messageSize.setInputVerifier(INT_VERIFIER)
        messageSize.preferredSize = Dimension(200, 24)

        batchSize = JTextField("16535")
        batchSize.setInputVerifier(INT_VERIFIER)
        batchSize.preferredSize = Dimension(200, 24)
        val fileButton = JButton("Load template from file")
        fileButton.preferredSize = Dimension(200, 24)
        fileButton.addActionListener {
            val fcd = FileChooserDescriptor(true, false, false, false, false, false)
            val file = FileChooser.chooseFile(fcd, project, null)
            if (file != null) {
                template.text = String(Files.readAllBytes(Paths.get(file.canonicalPath)))
            }
        }

        compression = ComboBox(KAFKA_COMPRESSION_TYPES)

        val panel = JPanel(GridLayout(0, 2))
        panel.addLabelled("Header", header)
        panel.addLabelled("Number of messages", numberOfMessages)
        panel.addLabelled("Delay ms", delay)
        panel.addLabelled("Batch size", batchSize)
        panel.addLabelled("Compression", compression)
        panel.addLabelled("Random part size", messageSize)
        return layoutUD(panel, layoutUD(template, fileButton))
    }

    fun getDelay() = delay.text.toLong()
    fun getHeader() = header.text.toString()
    fun getNumberOfMessages() = numberOfMessages.text.toInt()
    fun getMessageSize() = messageSize.text.toInt()
    fun getBatchSize() = batchSize.text.toInt()
    fun getCompression() = compression.selectedItem.toString()
    fun getTemplate() = template.text.toString()
}
