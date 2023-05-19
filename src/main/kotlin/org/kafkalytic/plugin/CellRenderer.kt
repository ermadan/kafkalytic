package org.kafkalytic.plugin

import com.intellij.openapi.util.IconLoader
import com.intellij.ui.ColoredTreeCellRenderer
import com.intellij.ui.SimpleTextAttributes
import javax.swing.JTree
import javax.swing.tree.DefaultMutableTreeNode

class CellRenderer : ColoredTreeCellRenderer() {
    private val leafIcon = IconLoader.getIcon("/icons/add.png", CellRenderer::class.java)
    private val folderIcon = IconLoader.getIcon("/icons/webFolder.png", CellRenderer::class.java)
    private val topicIcon = IconLoader.getIcon("/icons/read-access.png", CellRenderer::class.java)
    private val consumerIcon = IconLoader.getIcon("/icons/weblistener.png", CellRenderer::class.java)
    private val brokerIcon = IconLoader.getIcon("/icons/write-access.png", CellRenderer::class.java)

    override fun customizeCellRenderer(tree: JTree, node: Any?, selected: Boolean, expanded: Boolean,
                                       leaf: Boolean, row: Int, hasFocus: Boolean) {
        icon = when {
            node == null -> leafIcon
            node is KRootTreeNode -> folderIcon
            node is DefaultMutableTreeNode && node.parent is KRootTreeNode -> folderIcon
            node is DefaultMutableTreeNode -> when (node.parent?.toString()) {
                BROKERS -> brokerIcon
                CONSUMERS -> consumerIcon
                else -> topicIcon
            }
            else -> leafIcon
        }
        append(node.toString(), SimpleTextAttributes.REGULAR_ATTRIBUTES)
    }
}