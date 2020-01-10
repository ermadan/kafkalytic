package org.kafkalytic.plugin

import com.intellij.ui.ColoredTreeCellRenderer
import com.intellij.ui.SimpleTextAttributes
import sun.swing.DefaultLookup
import javax.swing.JTree
import javax.swing.tree.DefaultMutableTreeNode

class KTreeCellRenderer() : ColoredTreeCellRenderer() {
    val leafIcon = DefaultLookup.getIcon(this, ui, "Tree.leafIcon")
    val openIcon = DefaultLookup.getIcon(this, ui, "Tree.openIcon")
    val closedIcon = DefaultLookup.getIcon(this, ui, "Tree.closedIcon")

    override fun customizeCellRenderer(tree: JTree, node: Any?, selected: Boolean, expanded: Boolean, leaf: Boolean, row: Int, hasFocus: Boolean) {
        if (leaf) {
            icon = leafIcon
        } else if (expanded) {
            icon = openIcon
        } else {
            icon = closedIcon
        }
        if (node is DefaultMutableTreeNode
                && (node.userObject == BROKERS || node.userObject == TOPICS) ) {
            append(node.toString(), SimpleTextAttributes.REGULAR_ATTRIBUTES)
            if (node.childCount == 0) {
                append(" loading...", SimpleTextAttributes.GRAY_ITALIC_ATTRIBUTES)
            }
        } else {
            append(node.toString(), SimpleTextAttributes.REGULAR_ATTRIBUTES)
        }
    }
}