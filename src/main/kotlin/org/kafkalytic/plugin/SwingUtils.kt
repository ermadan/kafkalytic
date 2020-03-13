package org.kafkalytic.plugin

import com.intellij.openapi.fileChooser.FileChooser
import com.intellij.openapi.fileChooser.FileChooserDescriptor
import com.intellij.openapi.ui.InputValidator
import com.intellij.ui.DocumentAdapter
import com.intellij.ui.components.JBLabel
import java.awt.*
import java.nio.file.Files
import java.nio.file.Paths
import java.text.ParseException
import java.text.SimpleDateFormat
import javax.swing.*
import javax.swing.event.DocumentEvent

fun layoutLR(left: JComponent, center: JComponent? = null, right: JComponent? = null) : JPanel {
    val panel = JPanel(BorderLayout())
    panel.add(left, BorderLayout.WEST)
    center?.let { panel.add(it, BorderLayout.CENTER) }
    right?.let { panel.add(it, BorderLayout.EAST) }
    return panel
}

fun layoutUD(top: JComponent, center: JComponent? = null, bottom: JComponent? = null) : JPanel {
    val panel = JPanel(BorderLayout())
    panel.add(top, BorderLayout.NORTH)
    center?.let { panel.add(it, BorderLayout.CENTER) }
    bottom?.let { panel.add(it, BorderLayout.SOUTH) }
    return panel
}

fun layoutUD2(vararg components: JComponent) = JPanel(GridLayout(0, 1)).also { panel ->
    components.forEach {
        panel.add(it)
    }
}

fun JPanel.addLabelled(label: String, field: Component) {
    add(JBLabel("$label "))
    add(field)
}

val INT_VERIFIER = object : InputVerifier() {
    override fun verify(input: JComponent) : Boolean {
        try {
            with(Integer.parseInt((input as JTextField).text)) {
                return this <= Int.MAX_VALUE && this >= 0
            }
        } catch (e: NumberFormatException) {
            return false
        }
    }
}
val LONG_VALIDATOR = object: InputValidator {
    override fun checkInput(inputString: String?) = true
    override fun canClose(inputString: String?) =
        try {
            with(java.lang.Long.parseLong(inputString)) {
                this <= Long.MAX_VALUE && this >= 0
            }
        } catch (e: NumberFormatException) {
            false
        }

}
val DATE_FORMAT = SimpleDateFormat("dd/MM/yy hh:mm:ss")
val DATE_VERIFIER = object : InputVerifier() {
    override fun verify(input: JComponent) =
        try {
            DATE_FORMAT.parse((input as JTextField).text)
            true
        } catch (e: ParseException) {
            false
        }
}

val TOPIC_VERIFIER = object : InputVerifier() {
    override fun verify(input: JComponent) = Regex("""[a-zA-Z0-9.\-_]+""").matches((input as JTextField).text)
}

val LONG_VERIFIER = object : InputVerifier() {
    override fun verify(input: JComponent) = LONG_VALIDATOR.canClose((input as JTextField).text)
}

class HintTextField(private val _hint: String) : JTextField() {
    override fun paint(g: Graphics) {
        super.paint(g)
        if (text.length == 0) {
            val h = height
            (g as Graphics2D).setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON)
            val ins: Insets = insets
            val fm: FontMetrics = g.getFontMetrics()
            val c0 = background.rgb
            val c1 = foreground.rgb
            val m = -0x1010102
            val c2 = (c0 and m ushr 1) + (c1 and m ushr 1)
            g.setColor(Color(c2, true))
            g.drawString(_hint, ins.left, h / 2 + fm.getAscent() / 2 - 2)
        }
    }
}

class PrintOptionsPanel(config: KafkaStateComponent) : JPanel(BorderLayout()) {
    val printToEvent: JCheckBox
    val printToFile: JCheckBox
    val printToEditor: JCheckBox
    val file = JTextField(config.config["printToFile"] ?: Paths.get(System.getProperty("user.dir"), "kafkalytic-messages.txt").toString())
    init {
        printToEditor = JCheckBox("Print message payload to Editor window", null, config.config["printToEditorSelected"]?.toBoolean() ?: true)
        printToEvent = JCheckBox("Print message payload to EventLog window", null, config.config["printToEventSelected"]?.toBoolean() ?: true)
        val printToFileSelected = config.config["printToFileSelected"]?.toBoolean() ?: false
        printToFile = JCheckBox("Print message payload to file", null, printToFileSelected)
        printToEditor.addActionListener {
            config.config["printToEditorSelected"] = printToEditor.isSelected.toString()
            LOG.info("printToEditorSelected: " + config.config["printToEditorSelected"])
        }
        printToFile.addActionListener {
            file.isEnabled = printToFile.isSelected
            config.config["printToFileSelected"] = printToFile.isSelected.toString()
            LOG.info("printToFileSelected: " + config.config["printToFileSelected"])
        }
        printToEvent.addActionListener {
            config.config["printToEventSelected"] = printToEvent.isSelected.toString()
            LOG.info("printToEventSelected: " + config.config["printToEventSelected"])
        }
        file.isEnabled = printToFileSelected
        file.preferredSize = Dimension(200, 24)
        file.document.addDocumentListener(object: DocumentAdapter() {
            override fun textChanged(e: DocumentEvent?) {
                config.config["printToFile"] = file.text
                LOG.info("path changed: " + config.config["printToFile"])
            }
        })
        val browse = JButton("Browse")
        browse.addActionListener{
            val fcd = FileChooserDescriptor(true, true, false, false, false, false)
            val fileName = FileChooser.chooseFile(fcd, null, null)?.canonicalPath
            file.text = if (Files.isDirectory(Paths.get(fileName))) Paths.get(fileName, "kafkaMessages.txt").toString() else fileName
        }
        add(layoutUD(layoutUD(printToEditor, printToEvent), printToFile, layoutLR(JLabel("File to print"), file, browse)), BorderLayout.SOUTH)
    }
}

