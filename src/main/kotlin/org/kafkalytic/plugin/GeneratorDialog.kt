package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.fileChooser.FileChooser
import com.intellij.openapi.fileChooser.FileChooserDescriptor
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.ui.DialogWrapper
import java.awt.*
import java.nio.file.Files
import java.nio.file.Paths
import javax.swing.*

val RANDOM_PLACEHOLDER = """<random>"""

class GeneratorDialog(val project: Project, topic: String) : DialogWrapper(false)/*, ChangeListener*/ {

    private val LOG = Logger.getInstance(this::class.java)

    private lateinit var numberOfMessages: JTextField
    private lateinit var delay: JTextField
    private lateinit var batchSize: JTextField
    private lateinit var messageSize: JTextField
    private lateinit var template: JTextArea
    private lateinit var compression: ComboBox<String>
//    private lateinit var radios: List<JRadioButton>
//    override fun stateChanged(e: ChangeEvent?) {
//        messageSize.isEnabled = radios[0].isSelected
//        fileButton.isEnabled = radios[1].isSelected
//        template.isEnabled = radios[1].isSelected
//    }

    init {
        setTitle("Configure Kafka message generator for topic $topic")
        init()
    }

    override fun createCenterPanel(): JPanel {
        delay = JTextField("10")
        delay.setInputVerifier(LONG_VERIFIER)
        delay.preferredSize = Dimension(200, 24)

//        radios = arrayOf("Generate random content ", "Use template ").map { JRadioButton(it) }

        template = JTextArea(10, 43)
        template.text = """
            example template with $RANDOM_PLACEHOLDER that 
            will be replaced with randomized content
        """.trimIndent()
        template.lineWrap = true

        numberOfMessages = JTextField("100")
        numberOfMessages.setInputVerifier(INT_VERIFIER)
        numberOfMessages.preferredSize = Dimension(200, 24)

        messageSize = JTextField("100")
        messageSize.setInputVerifier(INT_VERIFIER)
        messageSize.preferredSize = Dimension(200, 24)

        batchSize = JTextField("16535")
        batchSize.setInputVerifier(INT_VERIFIER)
        batchSize.preferredSize = Dimension(200, 24)
        val fileButton = JButton("Load template from file")
        fileButton.preferredSize = Dimension(200, 24)
        fileButton.addActionListener{
            val fcd = FileChooserDescriptor(true, false, false, false, false, false)
            val file = FileChooser.chooseFile(fcd, project, null)
            if (file != null) {
                template.text = String(Files.readAllBytes(Paths.get(file.canonicalPath)))
            }
        }

        compression = ComboBox(KAFKA_COMPRESSION_TYPES)

        val panel = JPanel(GridLayout(0, 2))
        panel.addLabelled("Number of message", numberOfMessages)
        panel.addLabelled("Delay", delay)
        panel.addLabelled("Batch size", batchSize)
        panel.addLabelled("Compression", compression)
        panel.addLabelled("Random part size", messageSize)
        return layoutUD(panel, layoutUD(template, fileButton))
//        panel.add(layoutUD(
//            layoutLR(radios[1], JBScrollPane(value)),
//            layoutLR(radios[0], layoutLR(numberOfMessages, browse)),
//            layoutLR(JBLabel("Compression"), compression)), BorderLayout.SOUTH)

//        val radioGroup = ButtonGroup()
//        radios.forEach {
//            radioGroup.add(it)
//            it.addChangeListener(this)
//        }
//        radios[1].isSelected = true
//        stateChanged(null)

//        return panel
    }

    fun getDelay() = delay.text.toLong()
    fun getNumberOfMessages() = numberOfMessages.text.toInt()
    fun getMessageSize() = messageSize.text.toInt()
    fun getBatchSize() = batchSize.text.toInt()
    fun getCompression() = compression.selectedItem.toString()
    fun getTemplate() = template.text.toString()
}
