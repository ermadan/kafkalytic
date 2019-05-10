package org.kafkalytic.plugin

import java.util.*
import javax.swing.table.DefaultTableModel
import javax.swing.tree.DefaultMutableTreeNode

class TableModel() : DefaultTableModel() {
    fun updateDetails(node: DefaultMutableTreeNode) {
        dataVector.clear()
        columnIdentifiers.clear()
        if (node is KafkaTableNode) {
            node.headers().forEach{ addColumn(it) }
            node.rows().forEach { addRow(it) }
        }
    }
}