package org.kafkalytic.plugin

import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.ui.DialogWrapper
import org.apache.kafka.common.serialization.*
import java.awt.Color
import java.awt.Dimension
import java.awt.GridLayout
import javax.swing.*
import javax.swing.event.ChangeEvent
import javax.swing.event.ChangeListener


class ConsumeDialog(topic: String, val config: KafkaStateComponent) : DialogWrapper(false), ChangeListener {
    private val LOG = Logger.getInstance(this::class.java)

    private lateinit var waitFor: JTextField
    private lateinit var polls: JTextField
    private lateinit var decrement: JTextField
    private lateinit var partition: JTextField
    private lateinit var offset: JTextField
    private lateinit var keyDeserializer: JComboBox<String>
    private lateinit var valueDeserializer: JComboBox<String>
    private lateinit var radios: List<JRadioButton>
    private lateinit var printOptionsPanel: PrintOptionsPanel
    override fun stateChanged(e: ChangeEvent?) {
        waitFor.isEnabled = radios[0].isSelected
        polls.isEnabled = radios[0].isSelected
        decrement.isEnabled = radios[1].isSelected
        partition.isEnabled = radios[2].isSelected
        offset.isEnabled = radios[2].isSelected
        LOG.info("radios:" + radios[2].isSelected + ":" + radios[1].isSelected)
    }

    init {
        setTitle("Configure Kafka consumer for topic $topic")
        init()
    }

    override fun createCenterPanel(): JPanel {
        val deserializers = arrayOf(StringDeserializer::class.java,
                ByteArrayDeserializer::class.java,
                IntegerDeserializer::class.java,
                LongDeserializer::class.java,
                DoubleDeserializer::class.java).map { it.getSimpleName() }.toTypedArray()
        keyDeserializer = ComboBox(deserializers)
        keyDeserializer.preferredSize = Dimension(40, 24)
        valueDeserializer = ComboBox(deserializers)
        valueDeserializer.preferredSize = Dimension(40, 24)
        val gridLayout = GridLayout(0, 2)
        val deserizalizerSubPanel = JPanel(gridLayout)
        gridLayout.hgap = 2
        gridLayout.vgap = 2
        deserizalizerSubPanel.addLabelled("Key deserializer", keyDeserializer)
        deserizalizerSubPanel.addLabelled("Value deserializer", valueDeserializer)
        deserizalizerSubPanel.border = BorderFactory.createLineBorder(Color.GRAY, 1, true)

        val methodSubPanel = JPanel(GridLayout(3, 5))
        radios = arrayOf("Wait for ", "Recent ", "Specific message at ").map { JRadioButton(it) }
        methodSubPanel.add(radios[0])
        waitFor = JTextField(1.toString())
        methodSubPanel.add(waitFor)
        methodSubPanel.add(JLabel(" messages"))
        polls = JTextField(10.toString())
        methodSubPanel.add(polls)
        methodSubPanel.add(JLabel(" 1s polls"))
        methodSubPanel.add(JLabel(""))

        methodSubPanel.add(radios[1])
        decrement = JTextField(1.toString())
        methodSubPanel.add(decrement)
        methodSubPanel.add(JLabel(" messages"))
        repeat(3) { methodSubPanel.add(JLabel("")) }
        methodSubPanel.add(radios[2])
        partition = JTextField()
        methodSubPanel.add(partition)
        methodSubPanel.add(JLabel(" partition"))
        offset = JTextField()
        methodSubPanel.add(offset)
        methodSubPanel.add(JLabel(" offset"))
        val radioGroup = ButtonGroup()
        radios.forEach {
            radioGroup.add(it)
            it.addChangeListener(this)
        }
        radios[1].isSelected = true
        stateChanged(null)
        printOptionsPanel = PrintOptionsPanel(config)

        return layoutUD(deserizalizerSubPanel, methodSubPanel, printOptionsPanel)
    }

    fun getKeyDeserializer() = "org.apache.kafka.common.serialization." + keyDeserializer.selectedItem
    fun getValueDeserializer() = "org.apache.kafka.common.serialization." + valueDeserializer.selectedItem
    fun getDecrement() = if (radios[1].isSelected) decrement.text.toInt() else 0
    fun getPartition() = if (radios[2].isSelected) partition.text.toInt() else 0
    fun getOffset() = if (radios[2].isSelected) offset.text.toLong() else 0
    fun getWaitFor() = if (radios[0].isSelected) waitFor.text.toInt() else 0
    fun getPolls() = if (radios[0].isSelected) polls.text.toInt() else 0
    fun getMode() = if (radios[0].isSelected) 0 else if (radios[1].isSelected) 1 else 2
    fun getPrintToEvent() = printOptionsPanel.printToEvent.isSelected
    fun getPrintToFile() = printOptionsPanel.printToFile.isSelected
    fun getPrintToFileName() = printOptionsPanel.file.text
}

