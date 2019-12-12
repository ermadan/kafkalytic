package org.kafkalytic.plugin

import com.intellij.ui.components.JBLabel
import java.awt.BorderLayout
import java.awt.Component
import java.awt.GridLayout
import java.text.ParseException
import java.text.SimpleDateFormat
import javax.swing.InputVerifier
import javax.swing.JComponent
import javax.swing.JPanel
import javax.swing.JTextField

fun layoutLR(left: JComponent, right: JComponent?) : JPanel {
    val panel = JPanel(BorderLayout())
    panel.add(left, BorderLayout.WEST)
    right?.let { panel.add(it, BorderLayout.CENTER) }
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
    override fun verify(input: JComponent) : Boolean {
        try {
            with(java.lang.Long.parseLong((input as JTextField).text)) {
                return this <= Long.MAX_VALUE && this >= 0
            }
        } catch (e: NumberFormatException) {
            return false
        }
    }
}

