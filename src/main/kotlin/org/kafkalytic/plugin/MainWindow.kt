package org.kafkalytic.plugin

import com.intellij.openapi.actionSystem.*
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.progress.BackgroundTaskQueue
import com.intellij.openapi.progress.ProgressIndicator
import com.intellij.openapi.progress.ProgressManager
import com.intellij.openapi.progress.Task
import com.intellij.openapi.progress.impl.BackgroundableProcessIndicator
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.Messages
import com.intellij.openapi.util.IconLoader
import com.intellij.ui.DocumentAdapter
import com.intellij.ui.SearchTextField
import com.intellij.ui.components.JBScrollPane
import com.intellij.ui.table.JBTable
import com.intellij.ui.treeStructure.Tree
import java.awt.BorderLayout
import java.awt.event.ActionEvent
import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import java.io.File
import java.util.*
import java.util.regex.Pattern
import javax.swing.*
import javax.swing.event.*
import javax.swing.tree.*

class MainWindow(stateComponent: KafkaStateComponent, project: Project) : JPanel(BorderLayout()) {
    private val LOG = Logger.getInstance("Kafkalytic")
    private val ADD_ICON by lazy { IconLoader.getIcon("/general/add.png")}
    private val REMOVE_ICON by lazy { IconLoader.getIcon("/general/remove.png")}
    private val REFRESH_ICON by lazy { IconLoader.getIcon("/actions/refresh.png")}
    private val zRoot by lazy { DefaultMutableTreeNode("Kafka") }
    private val treeModel by lazy { DefaultTreeModel(zRoot) }
    private val tree by lazy { Tree(treeModel) }
    private val tableModel by lazy {TableModel()}
    private val addButton by lazy {AddAction()}
    private val removeButton by lazy {RemoveAction()}
    private val refreshButton by lazy {RefreshAction()}
    private val config = stateComponent
    private val project = project
    private val taskQueue = BackgroundTaskQueue(project, "Kafkalytic queue")

    init {
        LOG.info("Main window created")
        add(getToolbar(), BorderLayout.PAGE_START)


        val panel = JSplitPane(JSplitPane.VERTICAL_SPLIT)

        config.clusters?.forEach { LOG.info("init node:$it"); zRoot.add(KRootTreeNode(it.value)) }
        tree.expandPath(TreePath(zRoot))

        tree.addTreeSelectionListener {
            val node = it?.newLeadSelectionPath?.lastPathComponent
            LOG.info("selection changed:$node")
            if (node != null) {
                if (node is KRootTreeNode) {
                    tableModel.updateDetails(node as DefaultMutableTreeNode)
                }
                background("Loading properties") {
                    if (node is KafkaNode) {
                        node.expand()
                    }
                    foreground {
                        tableModel.updateDetails(node as DefaultMutableTreeNode)
                    }
                }
            }
        }

        tree.addMouseListener(object : MouseAdapter() {
            override fun mousePressed(e: MouseEvent) {
                if (SwingUtilities.isRightMouseButton(e)) {
                    val paths = tree.selectionPaths
                    if (paths.isEmpty()) {
                        return
                    }

                    val menu = object: JPopupMenu() {
                        fun add(name: String, task: () -> Unit) {
                            add(JMenuItem(object: AbstractAction(name) {
                                override fun actionPerformed(e: ActionEvent) {
                                    task()
                                }
                            }))
                        }
                    }
                    menu.add("Select topics") {
                        val pattern = Messages.showInputDialog("Enter selection regexp",
                                "Select topics", Messages.getQuestionIcon())
                        if (!pattern.isNullOrEmpty()) {
                            val parent = (paths.first().path[1] as KRootTreeNode).topics
                            tree.selectionModel.selectionPaths = parent.children().asSequence()
                                    .filter { Pattern.matches(pattern, (it as DefaultMutableTreeNode).userObject.toString()) }
                                    .map { TreePath((tree.model as DefaultTreeModel).getPathToRoot(it as TreeNode)) }
                                    .toList()
                                    .toTypedArray()
                            info(tree.selectionModel.selectionPaths.size.toString() + " topics were selected.")
                        }
                    }
                    val topicNodes = paths.filter {it.lastPathComponent is KTopicTreeNode}.map{it.lastPathComponent as KTopicTreeNode}
                    val topics = topicNodes.map {it.getTopicName()}
                    if (topics.isNotEmpty()) {
                        menu.add("Delete topic(s)") {
                            if (Messages.OK == Messages.showOkCancelDialog(
                                            "You are about to delete following topics " + topics.joinToString {"\n"},
                                            "Kafka", Messages.getQuestionIcon())) {
                                background("Deleting kafka topics") {
                                    val cluster = topicNodes.first().cluster
                                    cluster.delete(topics)
                                    cluster.topics.refresh()
                                    treeModel.reload(cluster.topics)
                                    info("" + topics.size + " topics were deleted.")
                                }
                            }
                        }
                    }
                    if (paths.size == 1) {
                        val path = paths.first()
                        val last = path.lastPathComponent
                        val clusterNode = path.path[1] as KRootTreeNode
                        if (last is KTopicTreeNode) {
                            menu.add("Consume from " + last.getTopicName()) {
                                val dialog = ConsumeDialog(last.getTopicName())
                                if (dialog.showAndGet()) {
                                    LOG.info("progress:" + ProgressManager.getInstance().progressIndicator)
                                    ApplicationManager.getApplication().invokeLater {
                                        val props = clusterNode.getClusterProperties()
                                        val consumer = Consumer(project, last.getTopicName(), props, dialog)
                                        ProgressManager.getInstance().runProcessWithProgressAsynchronously(
                                                consumer, BackgroundableProcessIndicator(consumer))

                                    }
                                }
                            }
                            menu.add("Publish to " + last.getTopicName()) {
                                val dialog = ProduceDialog(project, last.getTopicName())
                                if (dialog.showAndGet()) {
                                    val value = if (dialog.getMode()) {
                                        File(dialog.getFile()).inputStream().readBytes()
                                    } else {
                                        dialog.getText().toByteArray()
                                    }
                                    val producer = Producer(project, last.getTopicName(),
                                            last.cluster.getClusterProperties(),
                                            dialog.getKey(), value, dialog.getCompression())
                                    background("Publish") {
                                        producer.run()
                                        last.refresh()
                                        foreground { treeModel.reload(last) }
                                    }
                                }
                            }
                            menu.add("Change partitions number ") {
                                val partitions = Messages.showInputDialog("Enter partitions number",
                                        "Change partitions number for topic " + last.getTopicName() + " to ", Messages.getQuestionIcon())
                                if (partitions != null) {
                                    background("Change partitions number") {
                                        last.setPartitions(partitions.toInt())
                                        last.refresh()
                                        foreground { treeModel.reload(last) }
                                    }
                                }
                            }
                        }
                        menu.add("Refresh") {
                            if (last is KafkaNode) {
                                refreshCluster(last)
                            }
                        }
                        if (last is KRootTreeNode || last == clusterNode.topics) {
                            menu.add("Create topic") {
                                val dialog = CreateTopicDialog()
                                if (dialog.showAndGet()) {
                                    background("Changing partitions number") {
                                        clusterNode.createTopic(dialog.getTopic(), dialog.getPartitions(), dialog.getReplications())
                                        info("topic " + dialog.getTopic() + "was created.")
                                        clusterNode.topics.refresh()
                                        foreground { treeModel.reload(clusterNode.topics) }
                                    }
                                }
                            }
                        }
                        if (last is KRootTreeNode) {
                            menu.add("Remove cluster $last") { removeCluster() }
                        }
                    }
                    menu.show(tree, e.x, e.y)
                }
            }
        })

        tree.isRootVisible = false
        tree.selectionModel.selectionMode = TreeSelectionModel.DISCONTIGUOUS_TREE_SELECTION
        tree.addTreeExpansionListener(object : TreeExpansionListener {
            override fun treeExpanded(event: TreeExpansionEvent?) {
                val node = event!!.path.lastPathComponent
                if (node is KafkaNode) {
                    background("Expanding node") {
                        node.expand()
                        treeModel.reload(node)
                    }
                }
            }

            override fun treeCollapsed(event: TreeExpansionEvent?) {
            }
        })
        val details = JBTable(tableModel)
        details.fillsViewportHeight = false
        details.setShowColumns(true)
        tableModel.addEditListener { key, value ->
            if (tableModel.currentNode is KRootTreeNode) {
                val clusterConfig = (tableModel.currentNode as KRootTreeNode).getClusterProperties()
                val clusterName = clusterConfig["name"] as String
                if (key == "name") {
                    config.clusters.remove(clusterName)
                    config.clusters[value] = clusterConfig
                }
                with (clusterConfig) {
                    if (key.startsWith("Broker ")) {
                        val bootstrap = tableModel.dataVector.elements().asSequence()
                                .mapNotNull { val v = it as Vector<*>; if (v[0].toString().startsWith("Broker ")) v[1].toString() else null }.joinToString (",")
                        this.put("bootstrap.servers", bootstrap)
                        (tableModel.currentNode as KRootTreeNode).resetConnection()
                    } else {
                        this.put(key, value)
                    }
                }
                treeModel.reload(tableModel.currentNode)
            }
        }

        panel.topComponent = JBScrollPane(tree)
        panel.bottomComponent = JBScrollPane(details)
        panel.resizeWeight = 0.7
        panel.dividerSize = 2
        add(panel, BorderLayout.CENTER)
    }

    private fun refreshCluster(node: KafkaNode) {
        if (node is DefaultMutableTreeNode) {
            if (!(node is KRootTreeNode)) {
                    node.removeAllChildren()
                    node.add(DefaultMutableTreeNode("loading..."))
                    treeModel.reload(node)
            }
            background("refreshing $node") {
                try {
                    node.refresh()
                    foreground {
                        treeModel.reload(node)
                    }
                } catch (e: Exception) {
                    error("Unable to expand $node", e.cause)
                }
            }
        }
    }

    private fun removeCluster() {
        val clusterNode = tree.selectionPaths[0].lastPathComponent as KRootTreeNode
        if (Messages.OK == Messages.showOkCancelDialog(
                        "You are about to delete Kafka cluster " + clusterNode.toString(),
                        "Kafka", Messages.getQuestionIcon())) {
            zRoot.remove(clusterNode)
            treeModel.reload(zRoot)
            tableModel.updateDetails(null)
            config.removeCluster(clusterNode.userObject as Map<String, String>)
        }
    }

    fun updateTree(text: String) {
    }

    private fun getToolbar(): JComponent {
        val panel = JPanel()

        panel.layout = BoxLayout(panel, BoxLayout.X_AXIS)

        val group = DefaultActionGroup()
        group.add(addButton)
        group.add(removeButton)
        group.add(refreshButton)
        removeButton.templatePresentation.isEnabled = false

        panel.add(ActionManager.getInstance().createActionToolbar("Kafka Tool", group, true).component)


        val searchTextField = SearchTextField()
        searchTextField.addDocumentListener(object : DocumentAdapter() {
            override fun textChanged(e: DocumentEvent?) {
                if (e != null) {
                    val pattern = e.document.getText(0, e.document.length).toLowerCase()
                    tree.selectionModel.selectionPaths = findNodes(zRoot, pattern).map { leaf ->
                        generateSequence(leaf) { it.parent }.toList().reversed().toTypedArray()
                    }.map {
                        TreePath(it)
                    }.toTypedArray()
                    LOG.info("Selected topics ${tree.selectionModel.selectionPaths.size}")
                }
            }
        })


        panel.add(searchTextField)
        return panel
    }

    private fun findNodes(parent: TreeNode, text: String) : Collection<TreeNode> {
        val children = (parent.children() as Enumeration<TreeNode>).toList()
        return children
                .mapNotNull { if (it.toString().toLowerCase().indexOf(text) >= 0) listOf(it) else findNodes(it, text)}
                .flatten()
    }

    private fun addCluster() {
        val dialog = CreateClusterDialog(project)
        dialog.show()
        if (dialog.exitCode == Messages.OK) {
            background("Adding Kafka cluster " + dialog.inputString) {
                LOG.info("added:" + dialog.getCluster())
                zRoot.add(KRootTreeNode(dialog.getCluster()))
                treeModel.reload(zRoot)
                config.addCluster(dialog.getCluster())
            }
        }
    }

    inner class AddAction : AnAction("Add", "Add Kafka cluster node", ADD_ICON) {
        override fun actionPerformed(e: AnActionEvent?) {
            addCluster()
        }
    }

    inner class RemoveAction : AnAction("Remove","Remove Kafka cluster node", REMOVE_ICON), AnAction.TransparentUpdate {
        override fun actionPerformed(e: AnActionEvent?) {
            removeCluster()
        }

        override fun update (e: AnActionEvent) {
            e.presentation.setEnabled(isRootNodeSelected())
        }
    }

    inner class RefreshAction : AnAction("Refresh", "Refresh Kafka cluster node", REFRESH_ICON), AnAction.TransparentUpdate {
        override fun actionPerformed(e: AnActionEvent?) {
            val node = tree.selectionPaths[0].path[1]
            if (node is KafkaNode) {
                refreshCluster(node)
            }
        }

        override fun update (e: AnActionEvent) {
            e.presentation.setEnabled(isRootNodeSelected())
        }
    }

    private fun isRootNodeSelected(): Boolean {
        if (tree.selectionPaths == null) {
            return false
        }
        return tree.selectionPaths.fold(true) { a, v ->
            val path = v.lastPathComponent
            a && (path is KRootTreeNode) && (path.isLeaf || path is KRootTreeNode)
        }
    }



    fun background(title: String, task: () -> Unit) {
        background(project, title, task)
    }

    fun background(project: Project?, title: String, task: () -> Unit) {
        taskQueue.run(object: Task.Backgroundable(project, title, false) {
            override fun run(indicator: ProgressIndicator) {
                LOG.info("background task started:$title")
                task()
                LOG.info("background task complete:$title")
            }
        })
    }
}

fun foreground(task: () -> Unit) {
    ApplicationManager.getApplication().invokeLater(task)
}

fun error(message: String, e: Throwable?) {
    LOG.error(message, e)
    foreground { Messages.showErrorDialog(message + e.toString(), "Kafka") }
}

fun info(message: String) {
    LOG.info(message)
    foreground { Messages.showInfoMessage(message, "Kafka") }
}


