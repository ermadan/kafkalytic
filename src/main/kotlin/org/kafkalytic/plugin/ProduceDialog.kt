package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.fileChooser.FileChooser
import com.intellij.openapi.fileChooser.FileChooserDescriptor
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.ui.DialogWrapper
import com.intellij.ui.components.JBLabel
import com.intellij.ui.components.JBScrollPane
import java.awt.*
import javax.swing.*
import javax.swing.event.ChangeEvent
import javax.swing.event.ChangeListener


class ProduceDialog(val project: Project, topic: String) : DialogWrapper(false), ChangeListener {
    private val LOG = Logger.getInstance(this::class.java)

    private lateinit var file: JTextField
    private lateinit var value: JTextArea
    private lateinit var key: JTextField
    private lateinit var compression: ComboBox<String>
    private lateinit var radios: List<JRadioButton>
    override fun stateChanged(e: ChangeEvent?) {
        file.isEnabled = radios[0].isSelected
        value.isEnabled = radios[1].isSelected
    }

    init {
        setTitle("Configure Kafka producer for topic $topic")
        init()
    }

    override fun createCenterPanel(): JPanel {
        key = JTextField()
        key.preferredSize = Dimension(200, 24)

        radios = arrayOf("Load from file ", "Text ").map { JRadioButton(it) }

        value = JTextArea(10, 43)
        value.lineWrap = true

        file = JTextField()
        file.preferredSize = Dimension(200, 24)
        val browse = JButton("Browse")
        browse.addActionListener{
            val fcd = FileChooserDescriptor(true, false, false, false, false, false)
            file.text = FileChooser.chooseFile(fcd, project, null)?.canonicalPath
        }

        compression = ComboBox(arrayOf("none", "gzip", "snappy", "lz4", "zstd"))

        val panel = JPanel(BorderLayout())
        panel.add(layoutLR(JBLabel("Key "), key), BorderLayout.NORTH)
        panel.add(JBLabel("Value"), BorderLayout.CENTER)
        panel.add(layoutUD(
            layoutLR(radios[1], JBScrollPane(value)),
            layoutLR(radios[0], layoutLR(file, browse)),
            layoutLR(JBLabel("Compression"), compression)), BorderLayout.SOUTH)

        val radioGroup = ButtonGroup()
        radios.forEach {
            radioGroup.add(it)
            it.addChangeListener(this)
        }
        radios[1].isSelected = true
        stateChanged(null)

        return panel
    }

    private fun layoutLR(left: JComponent, right: JComponent) : JPanel {
        val panel = JPanel(BorderLayout())
        panel.add(left, BorderLayout.WEST)
        if (right != null) {
            panel.add(right, BorderLayout.CENTER)
        }
        return panel
    }

    private fun layoutUD(top: JComponent, center: JComponent, bottom: JComponent) : JPanel {
        val panel = JPanel(BorderLayout())
        panel.add(top, BorderLayout.NORTH)
        panel.add(center, BorderLayout.CENTER)
        panel.add(bottom, BorderLayout.SOUTH)
        return panel
    }

    fun getKey() = key.text
    fun getFile() = file.text
    fun getText() = value.text
    fun getMode() = radios[0].isSelected
    fun getCompression() = compression.selectedItem.toString()
}
