package org.kafkalytic.plugin

import com.intellij.openapi.ui.DialogWrapper
import com.intellij.openapi.ui.ValidationInfo
import java.awt.*
import java.util.*
import javax.swing.*
import javax.swing.event.ChangeEvent
import javax.swing.event.ChangeListener

class SearchDialog(topic: String, val config: KafkaStateComponent) : DialogWrapper(false) {
    private lateinit var valuePattern: JTextField
    private lateinit var keyPattern: JTextField
    private lateinit var timestampPanel: TimestampPanel
    private lateinit var printOptionsPanel: PrintOptionsPanel

    init {
        title = "Configure Kafka search for topic $topic"
        init()
    }

    override fun doValidate(): ValidationInfo? = if (valuePattern.text.isBlank() && keyPattern.text.isBlank()) {
        ValidationInfo("One of pattern fields should not be blank", null)
    } else {
        null
    }


    override fun createCenterPanel(): JPanel {
        valuePattern = HintTextField("regexp")
        valuePattern.preferredSize = Dimension(200, 24)
        keyPattern = HintTextField("regexp")
        keyPattern.preferredSize = Dimension(200, 24)

        timestampPanel = TimestampPanel()

        printOptionsPanel = PrintOptionsPanel(config)

        return layoutUD(timestampPanel,
                layoutUD(layoutLR(JLabel("Value search pattern"), valuePattern),
                layoutLR(JLabel("Key search pattern"), keyPattern)),
                printOptionsPanel)
    }

    class TimestampPanel : JPanel(), ChangeListener {
        private var timestamp: JTextField = JTextField("0")
        private var datetime: JTextField
        private var radios: List<JRadioButton>

        init {
            timestamp.setInputVerifier(INT_VERIFIER)
            timestamp.preferredSize = Dimension(200, 24)

            datetime = JTextField(DATE_FORMAT.format(Date()))
            datetime.setInputVerifier(DATE_VERIFIER)
            datetime.preferredSize = Dimension(200, 24)

            val radioGroup = ButtonGroup()
            radios = arrayOf("From beginning ", "Specific datetime ", "From specific timestamp ").map {
                JRadioButton(it).also {
                    radioGroup.add(it)
                    it.addChangeListener(this)
                }
            }
            radios[0].isSelected = true

            val panel = JPanel(GridLayout(0, 2))
            panel.addLabelled("Start timestamp", timestamp)
            panel.addLabelled("Start date", datetime)
            add(layoutUD(layoutUD(radios[0], radios[1], radios[2]), panel))
        }

        override fun stateChanged(e: ChangeEvent?) {
            timestamp.isEnabled = radios[2].isSelected
            datetime.isEnabled = radios[1].isSelected
            if (radios[0].isSelected) {
                timestamp.text = "0"
            }
        }

        fun getTimestamp() = if (radios[1].isSelected) {
            DATE_FORMAT.parse(datetime.text).time
        } else {
            timestamp.text.toLong()
        }
    }

    fun getKeyPattern() = keyPattern.text
    fun getValuePattern() = valuePattern.text
    fun getTimestamp() = timestampPanel.getTimestamp()
    fun getPrintToEvent() = printOptionsPanel.printToEvent.isSelected
    fun getPrintToFile() = printOptionsPanel.printToFile.isSelected
    fun getPrintToFileName() = printOptionsPanel.file.text

}
