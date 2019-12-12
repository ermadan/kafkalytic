package org.kafkalytic.plugin

import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.DialogWrapper
import java.awt.*
import java.util.*
import javax.swing.*
import javax.swing.event.ChangeEvent
import javax.swing.event.ChangeListener

class SearchDialog(topic: String) : DialogWrapper(false) {
    private lateinit var pattern: JTextField
    private lateinit var timestampPanel: TimestampPanel

    init {
        title = "Configure Kafka search for topic $topic"
        init()
//        preferredSize = Dimension(500, 150)
    }

    override fun createCenterPanel(): JPanel {
        pattern = JTextField("")
        pattern.preferredSize = Dimension(200, 24)

        timestampPanel = TimestampPanel()

        return layoutUD(timestampPanel, layoutLR(JLabel("Search pattern (regexp)"), pattern))
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

    fun getPattern() = pattern.text
    fun getTimestamp() = timestampPanel.getTimestamp()
}
