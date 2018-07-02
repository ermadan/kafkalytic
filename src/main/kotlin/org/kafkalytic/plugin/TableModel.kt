package org.kafkalytic.plugin

import java.util.*
import javax.swing.table.DefaultTableModel
import javax.swing.tree.DefaultMutableTreeNode

class TableModel() : DefaultTableModel() {
    fun updateDetails(node: DefaultMutableTreeNode) {
        dataVector.clear()
        columnIdentifiers.clear()
        if (node is KTopicTreeNode) {
            addColumn("Partition Id")
            addColumn("ISR")
            addColumn("Leader")
            node.getPartitions().forEach{
                addRow(arrayOf(
                        it.partition(),
                        it.isr().joinToString { it.id().toString()},
                        it.leader().id().toString()
                ))
            }
        }
    }
}