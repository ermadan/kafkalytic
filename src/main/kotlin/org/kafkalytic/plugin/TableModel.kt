package org.kafkalytic.plugin

import java.util.*
import javax.swing.table.DefaultTableModel
import javax.swing.tree.DefaultMutableTreeNode
import javax.swing.tree.TreeNode

class TableModel : DefaultTableModel() {
    init {
        addTableModelListener { e ->
            if (!mute) {
                val value = getValueAt(e.firstRow, e.column) as String
                val key = getValueAt(e.firstRow, 0) as String
                listener?.invoke(key, value)
            }
        }
    }

    var mute = false
    var currentNode: TreeNode? = null
    var listener: ((key: String, value: String) -> Unit)? = null

    fun updateDetails(node: TreeNode?) {
        mute = true
        currentNode = node
        dataVector.clear()
        columnIdentifiers.clear()
        if (node != null) {
            if (node is KafkaTableNode) {
                node.headers().forEach { addColumn(it) }
                node.rows().forEach { addRow(it) }
            }
        }
        mute = false
    }

    override fun isCellEditable(row: Int, column: Int) = currentNode is KRootTreeNode && column == 1

    fun addEditListener( listener: (key: String, value: String) -> Unit) {
        this.listener = listener
    }
}