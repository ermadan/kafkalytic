package org.kafkalytic.plugin

import java.awt.BorderLayout
import javax.swing.JComponent
import javax.swing.JPanel

fun layoutLR(left: JComponent, right: JComponent?) : JPanel {
    val panel = JPanel(BorderLayout())
    panel.add(left, BorderLayout.WEST)
    if (right != null) {
        panel.add(right, BorderLayout.CENTER)
    }
    return panel
}

fun layoutUD(top: JComponent, center: JComponent, bottom: JComponent) : JPanel {
    val panel = JPanel(BorderLayout())
    panel.add(top, BorderLayout.NORTH)
    panel.add(center, BorderLayout.CENTER)
    panel.add(bottom, BorderLayout.SOUTH)
    return panel
}

