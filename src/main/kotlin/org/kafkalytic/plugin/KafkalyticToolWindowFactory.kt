package org.kafkalytic.plugin

import com.intellij.openapi.components.ServiceManager
import com.intellij.openapi.project.Project
import com.intellij.openapi.wm.ToolWindow
import com.intellij.openapi.wm.ToolWindowFactory
import com.intellij.ui.content.ContentFactory
import java.text.DecimalFormat
import java.text.NumberFormat
import java.util.*

class KafkalyticToolWindowFactory : ToolWindowFactory {

    private val config: KafkaStateComponent = ServiceManager.getService(KafkaStateComponent::class.java)

    companion object Formatter {
        private val formatter = NumberFormat.getInstance(Locale.US) as DecimalFormat

        init {
            val symbols = formatter.decimalFormatSymbols
            symbols.groupingSeparator = ' '
            formatter.decimalFormatSymbols = symbols
        }
    }

    override fun createToolWindowContent(project: Project, toolWindow: ToolWindow) {
        val contentFactory = ContentFactory.SERVICE.getInstance()
        val content = contentFactory!!.createContent(MainWindow(config, project), "", false)
        toolWindow.contentManager!!.addContent(content)
    }
}

