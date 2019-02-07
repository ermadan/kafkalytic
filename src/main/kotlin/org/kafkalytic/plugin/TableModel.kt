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
            addColumn("Offset")
            background(null, "reading offsets") {
                val offsets = getOffsets(node.cluster.getClusterProperties(), node.getTopicName())
                node.getPartitions().forEach {
                    addRow(arrayOf(
                            it.partition(),
                            it.isr().joinToString { it.id().toString() },
                            it.leader().id().toString(),
                            offsets.find{ p -> p.first == it.partition()}?.second
                    ))
                }
            }
        } else if (node is KRootTreeNode) {
            addColumn("Host")
            addColumn("Port")
            val map = node.userObject
            if (map is Map<*,*>) {
                val servers = map["bootstrap.servers"].toString().split(";")
                servers.forEach {addRow(it.split(":").toTypedArray())}
            }
        }
    }
}