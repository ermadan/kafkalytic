package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.fileChooser.FileChooser
import com.intellij.openapi.fileChooser.FileChooserDescriptor
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.ui.DialogWrapper
import com.intellij.ui.components.JBLabel
import com.intellij.ui.components.JBScrollPane
import org.apache.kafka.common.serialization.*
import java.awt.*
import java.awt.event.ActionEvent
import java.io.File
import java.util.concurrent.CompletableFuture
import javax.swing.*
import javax.swing.event.ChangeEvent
import javax.swing.event.ChangeListener


class ProduceDialog(val project: Project, topic: String) : DialogWrapper(false), ChangeListener {
    private val LOG = Logger.getInstance(this::class.java)

    private var file: JTextField? =  null
    private var value: JTextArea? = null
    private var key: JTextField? = null
    private var keySerializer: JComboBox<String>? = null
    private var valueSerializer: JComboBox<String>? = null
    private var radios: List<JRadioButton>? = null
    override fun stateChanged(e: ChangeEvent?) {
        file?.isEnabled = radios!![0].isSelected
        value?.isEnabled = radios!![1].isSelected
    }

    init {
        setTitle("Configure Kafka producer for topic " + topic)
        init()
    }

    override fun createCenterPanel(): JPanel {
        val certPanel = JPanel(BorderLayout())

        val gridLayout = GridLayout(0, 2)
        gridLayout.hgap = 2
        gridLayout.vgap = 2
//        val deserizalizerSubPanel = JPanel(gridLayout)
//        val deserializers = arrayOf(StringDeserializer::class.java,
//                ByteArrayDeserializer::class.java,
//                IntegerDeserializer::class.java,
//                LongDeserializer::class.java,
//                DoubleDeserializer::class.java).map{it.getSimpleName()}.toTypedArray()
//        keySerializer = ComboBox(deserializers)
//        keySerializer?.preferredSize = Dimension(40, 24)
//        valueSerializer = ComboBox(deserializers)
//        valueSerializer?.preferredSize = Dimension(40, 24)
//        valueSerializer?.selectedIndex = 1
//        deserizalizerSubPanel.add(JLabel("Key serializer"))
//        deserizalizerSubPanel.add(keySerializer)
//        deserizalizerSubPanel.add(JLabel("Value serializer"))
//        deserizalizerSubPanel.add(valueSerializer)
//        deserizalizerSubPanel.border = BorderFactory.createLineBorder(Color.GRAY, 1, true)
//        certPanel.add(deserizalizerSubPanel, BorderLayout.NORTH)

        val keySubPanel = JPanel(FlowLayout(FlowLayout.LEADING))
        key = JTextField()
        key?.preferredSize = Dimension(200, 24)
        keySubPanel.add(JBLabel("Key "))
        keySubPanel.add(key)
        val loadSubPanel = JPanel(FlowLayout(FlowLayout.LEADING))
        val textSubPanel = JPanel(FlowLayout(FlowLayout.LEADING))
        radios = arrayOf("Load from file ", "Text ").map{JRadioButton(it)}
        loadSubPanel.add(radios!![0])
        file = JTextField()
        file?.preferredSize = Dimension(200, 24)
        loadSubPanel.add(file)
        val browse = JButton("Browse")
        browse.addActionListener({
            val fcd = FileChooserDescriptor(true, false, false, false, false, false)
            file?.text = FileChooser.chooseFile(fcd, project, null)?.canonicalPath
        })

        loadSubPanel.add(browse)
        textSubPanel.add(radios!![1])
        value = JTextArea(10, 43)
        value?.setLineWrap(true)
        val jScrollPane1 = JBScrollPane(value)
        textSubPanel.add(jScrollPane1)
        certPanel.add(keySubPanel, BorderLayout.NORTH)
        certPanel.add(loadSubPanel, BorderLayout.CENTER)
        certPanel.add(textSubPanel, BorderLayout.SOUTH)
        val radioGroup = ButtonGroup()
        radios!!.forEach {
            radioGroup.add(it)
            it.addChangeListener(this)
        }
        radios!![1].isSelected = true
        stateChanged(null)

        return certPanel
    }

    fun getKey() = key!!.text
    fun getFile() = file!!.text
    fun getText() = value!!.text

    fun getMode() = radios!![0].isSelected
}
