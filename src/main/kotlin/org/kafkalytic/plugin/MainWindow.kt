package org.kafkalytic.plugin

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonSyntaxException
import com.intellij.lang.Language
import com.intellij.notification.Notification
import com.intellij.notification.NotificationType
import com.intellij.notification.Notifications
import com.intellij.openapi.actionSystem.ActionManager
import com.intellij.openapi.actionSystem.AnAction
import com.intellij.openapi.actionSystem.AnActionEvent
import com.intellij.openapi.actionSystem.DefaultActionGroup
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.fileChooser.FileChooser
import com.intellij.openapi.fileChooser.FileChooserDescriptor
import com.intellij.openapi.fileEditor.FileEditorManager
import com.intellij.openapi.progress.BackgroundTaskQueue
import com.intellij.openapi.progress.ProgressIndicator
import com.intellij.openapi.progress.Task
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.Messages
import com.intellij.openapi.util.IconLoader
import com.intellij.psi.PsiFile
import com.intellij.psi.PsiFileFactory
import com.intellij.ui.DocumentAdapter
import com.intellij.ui.SearchTextField
import com.intellij.ui.components.JBScrollPane
import com.intellij.ui.table.JBTable
import com.intellij.ui.treeStructure.Tree
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.awt.BorderLayout
import java.awt.event.ActionEvent
import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import java.io.*
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.regex.Pattern
import javax.swing.*
import javax.swing.event.DocumentEvent
import javax.swing.event.TreeExpansionEvent
import javax.swing.event.TreeExpansionListener
import javax.swing.tree.*
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.OutputKeys
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult

class MainWindow(stateComponent: KafkaStateComponent, private val project: Project) : JPanel(BorderLayout()) {
    private val LOG = Logger.getInstance("Kafkalytic")
    private val ADD_ICON by lazy { IconLoader.getIcon("/general/add.png")}
    private val REMOVE_ICON by lazy { IconLoader.getIcon("/general/remove.png")}
    private val REFRESH_ICON by lazy { IconLoader.getIcon("/actions/refresh.png")}
    private val EXPORT_ICON by lazy { IconLoader.getIcon("/actions/install.png")}
    private val IMPORT_ICON by lazy { IconLoader.getIcon("/actions/uninstall.png")}
    val KAFKA_ICON = IconLoader.getIcon("/icons/kafka.png")
    private val zRoot by lazy { DefaultMutableTreeNode("Kafka") }
    private val treeModel by lazy { DefaultTreeModel(zRoot) }
    private val tree by lazy { Tree(treeModel) }
    private val tableModel by lazy {TableModel()}
    private val topicTableModel by lazy {TopicTableModel()}
    private val topicTable by lazy {JBTable(topicTableModel)}
    private val addButton by lazy {AddAction()}
    private val removeButton by lazy {RemoveAction()}
    private val refreshButton by lazy {RefreshAction()}
    private val config = stateComponent
    private val taskQueue = BackgroundTaskQueue(project, "Kafkalytic queue")
    private val longTaskQueue = BackgroundTaskQueue(project, "Kafkalytic long task queue")

    init {
        LOG.info("Main window created")
        add(getToolbar(), BorderLayout.PAGE_START)


        tree.isRootVisible = false
        tree.selectionModel.selectionMode = TreeSelectionModel.DISCONTIGUOUS_TREE_SELECTION
        tree.cellRenderer = CellRenderer()

        val details = JBTable(tableModel)
        details.fillsViewportHeight = false
        details.setShowColumns(true)

        topicTable.fillsViewportHeight = false
        topicTable.setShowColumns(true)

        val propsSplit = JSplitPane(JSplitPane.VERTICAL_SPLIT)
        val jbScrollPane = JBScrollPane(details)
        propsSplit.topComponent = jbScrollPane
        propsSplit.resizeWeight = 0.7
        propsSplit.dividerSize = 2

        val mainSplit = JSplitPane(JSplitPane.VERTICAL_SPLIT)
        mainSplit.topComponent = JBScrollPane(tree)
        mainSplit.bottomComponent = propsSplit
        mainSplit.resizeWeight = 0.7
        mainSplit.dividerSize = 2


        add(mainSplit, BorderLayout.CENTER)

        config.clusters.forEach { LOG.info("init node:$it"); zRoot.add(KRootTreeNode(it.value)) }
        tree.expandPath(TreePath(zRoot))

        tree.addTreeSelectionListener {
            it?.newLeadSelectionPath?.lastPathComponent?.let {node ->
                LOG.info("selection changed:$node")
                if (node is KRootTreeNode) {
                    if (config.config["progress_tip"] == null) {
                        info("""
                            |Did you know that you can now export and import clusters configuration, 
                            |import/export buttons are on toolbar
                            """.trimMargin())
                        config.config["progress_tip"] = "shown"
                    }
                }
                propsSplit.bottomComponent = if (node is KTopicTreeNode) {
                    JBScrollPane(topicTable)
                } else {
                    null
                }
                topicTableModel.updateDetails(node as TreeNode)
                tableModel.updateDetails(node)
                background("Loading properties $node") {
                    if (node is KafkaNode) {
                        node.expand()
                    }
                    foreground {
                        tableModel.updateDetails(node)
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
                        fun add(name: String, task: (title: String) -> Unit) {
                            add(JMenuItem(object: AbstractAction(name) {
                                override fun actionPerformed(e: ActionEvent) {
                                    task(name)
                                }
                            }))
                        }
                    }
                    menu.add("Select topics") {
                        val pattern = Messages.showInputDialog("Enter selection regexp",
                                "Select topics", Messages.getQuestionIcon())
                        if (!pattern.isNullOrEmpty()) {
                            val parent = (paths.first().path[1] as KRootTreeNode).getTopics()
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
                                            "You are about to delete following topics " + topics.joinToString("\n"),
                                            "Kafka", Messages.getQuestionIcon())) {
                                background("Deleting kafka topics") {
                                    val cluster = topicNodes.first().cluster
                                    cluster.delete(topics)
                                    refreshNode(cluster.getTopics())
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
                            val connection = last.cluster.clusterProperties
                            val topic = last.getTopicName()
                            menu.add("Change partitions number") {
                                Messages.showInputDialog(it, it, Messages.getQuestionIcon(), last.getPartitions().size.toString(), LONG_VALIDATOR)?.run {
                                    background(it) {
                                        last.setPartitions(this.toInt())
                                        refreshNode(last)
                                    }
                                }
                            }
                            menu.add("Remove messages before offset") {
                                Messages.showInputDialog("Enter offset",
                                        it, Messages.getQuestionIcon(), "0", LONG_VALIDATOR)?.run {
                                    background(it) {
                                        last.deleteRecords(this.toLong())
                                        refreshNode(last)
                                    }
                                }
                            }
                            menu.add("Consume ") {
                                val dialog = ConsumeDialog(topic, config)
                                if (dialog.showAndGet()) {
                                    background (it, longTaskQueue) { progress ->
                                        consume(topic, connection, dialog,  progress, this@MainWindow)
                                    }
                                }
                            }
                            menu.add("Search topic for the message ") {
                                val dialog = SearchDialog(topic, config)
                                if (dialog.showAndGet()) {
                                    background (it + dialog.getValuePattern(), longTaskQueue) { progress ->
                                        search(connection, topic,
                                                dialog.getKeyPattern(), dialog.getValuePattern(), dialog.getTimestamp(),
                                                { progress.isCanceled }) { printMessage(it) }
                                    }
                                }
                            }
                            menu.add("Publish single message ") {
                                val dialog = ProduceDialog(project, topic)
                                if (dialog.showAndGet()) {
                                    val value = if (dialog.getMode()) {
                                        File(dialog.getFile()).inputStream().readBytes()
                                    } else {
                                        dialog.getText().toByteArray()
                                    }
                                    background(it) {
                                        withProducer(connection, dialog.getCompression()) { producer ->
                                            produceSingleMessage(producer, topic, dialog.getKey(), value)
                                        }
                                        refreshNode(last)
                                    }
                                }
                            }
                            menu.add("Message bulk generator") {
                                val dialog = GeneratorDialog(project, topic)
                                if (dialog.showAndGet()) {
                                    background(it, longTaskQueue) { progress ->
                                        try {
                                            withProducer(last.cluster.clusterProperties, dialog.getCompression(), dialog.getBatchSize()) { producer ->
                                                produceGeneratedMessages(producer, topic,
                                                        dialog.getTemplate(), dialog.getMessageSize(),
                                                        dialog.getNumberOfMessages(), dialog.getDelay()) { progress.isCanceled }
                                            }
                                        } catch (e: ExecutionException) {
                                            if (!progress.isCanceled) {
                                                progress.cancel()
                                            }
                                        }
                                        refreshNode(last)
                                    }
                                }
                            }
                            menu.add("Copy topic messages to another") {
                                val dialog = CopyDialog(topic, last.cluster, zRoot.children().toList() as List<KRootTreeNode>)
                                if (dialog.showAndGet()) {
                                    background (it, longTaskQueue) { progress ->
                                        try {
                                            copy(connection, topic,
                                                    dialog.getSelectedCluster().clusterProperties, dialog.getSelectedTopic().getTopicName(),
                                                    dialog.getTimestamp(), dialog.getCompression())  { progress.isCanceled }
                                        } catch (e: ExecutionException) {
                                            if (!progress.isCanceled) {
                                                progress.cancel()
                                            }
                                        }
                                    }
                                }
                            }
                            menu.add("Show consumption progress") {
                                val progressDialog = ProgressDialog(topic, last.cluster.client!!, this@MainWindow, project)
                                progressDialog.show()
                            }
                        }
                        menu.add("Refresh") {
                            if (last is KafkaNode) {
                                refreshNode(last)
                            }
                        }
                        if (last is KRootTreeNode || last == clusterNode.getTopics()) {
                            menu.add("Create topic") {
                                val dialog = CreateTopicDialog()
                                if (dialog.showAndGet()) {
                                    background(it) {
                                        val result = clusterNode.createTopic(dialog.getTopic(), dialog.getPartitions(),
                                                dialog.getReplications(), dialog.getConfig())
                                        val future = result?.values()?.get(dialog.getTopic())
                                        try {
                                            future?.get()
                                            info("topic ${dialog.getTopic()} was created.")
                                            refreshNode(clusterNode.getTopics())
                                        } catch (e : Exception) {
                                            info("$future\ntopic ${dialog.getTopic()} was not created")
                                        }
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

        tree.addTreeExpansionListener(object : TreeExpansionListener {
            override fun treeExpanded(event: TreeExpansionEvent?) {
                val node = event!!.path.lastPathComponent
                if (node is KafkaNode) {
                    background("Expanding node $node") {
                        node.expand()
                        treeModel.reload(node)
                    }
                }
            }

            override fun treeCollapsed(event: TreeExpansionEvent?) {
            }
        })

        tableModel.addEditListener { key, value ->
            if (tableModel.currentNode is KRootTreeNode) {
                val clusterConfig = (tableModel.currentNode as KRootTreeNode).clusterProperties
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
                        (tableModel.currentNode as KRootTreeNode).refresh()
                    } else {
                        this.put(key, value)
                    }
                }
                treeModel.reload(tableModel.currentNode)
            }
        }
    }

    private fun refreshNode(node: KafkaNode) {
        background("refreshing $node") {
            try {
                node.refresh()
                foreground {
                    treeModel.reload(node)
                    tableModel.updateDetails(node)
                }
            } catch (e: Exception) {
                error("Unable to expand $node", e.cause)
            }
        }
    }

    private fun removeCluster() {
        val clusterNode = tree.selectionPaths[0].lastPathComponent as KRootTreeNode
        if (Messages.OK == Messages.showOkCancelDialog(
                        "You are about to delete Kafka cluster $clusterNode",
                        "Kafka", Messages.getQuestionIcon())) {
            zRoot.remove(clusterNode)
            treeModel.reload(zRoot)
            tableModel.updateDetails(null)
            config.removeCluster(clusterNode.userObject as Map<String, String>)
        }
    }

    private fun getToolbar(): JComponent {
        val panel = JPanel()

        panel.layout = BoxLayout(panel, BoxLayout.X_AXIS)

        val group = DefaultActionGroup()
        group.add(addButton)
        group.add(removeButton)
        group.add(refreshButton)
        group.add(ImportAction())
        group.add(ExportAction())
        removeButton.templatePresentation.isEnabled = false

        panel.add(ActionManager.getInstance().createActionToolbar("Kafka Tool", group, true).component)


        val searchTextField = SearchTextField()
        searchTextField.addDocumentListener(object : DocumentAdapter() {
            override fun textChanged(e: DocumentEvent) {
                if (e != null) {
                    val pattern = e.document.getText(0, e.document.length).toLowerCase()
                    tree.selectionModel.selectionPaths = findNodes(zRoot, pattern).map { leaf ->
                        generateSequence(leaf) { it.parent }.toList().reversed().toTypedArray()
                    }.map {
                        TreePath(it)
                    }.toTypedArray()
                    if (tree.selectionModel.selectionPaths.isNotEmpty()) {
                        tree.scrollPathToVisible(tree.selectionModel.selectionPaths[0])
                    }
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
                .filter {it is KTopicTreeNode}
    }

    inner class AddAction : AnAction("Add", "Add Kafka cluster node", ADD_ICON) {
        override fun actionPerformed(e: AnActionEvent) {
            val dialog = CreateClusterDialog(project)
            dialog.show()
            if (dialog.exitCode == Messages.OK) {
                addCluster(dialog.getCluster())
            }
        }
    }

    private fun addCluster(cluster: MutableMap<String, String>) {
        background("Adding Kafka cluster ${cluster["name"]}") {
            if (config.clusters[cluster["name"]] != null) {
                cluster["name"] = cluster["name"] + "1"
            }
            LOG.info("added:$cluster")
            zRoot.add(KRootTreeNode(cluster))
            treeModel.reload(zRoot)
            config.addCluster(cluster)
        }
    }

    inner class RemoveAction : AnAction("Remove","Remove Kafka cluster node", REMOVE_ICON) {
        override fun actionPerformed(e: AnActionEvent) {
            removeCluster()
        }

        override fun update (e: AnActionEvent) {
            e.presentation.isEnabled = isRootNodeSelected()
        }
    }

    inner class RefreshAction : AnAction("Refresh", "Refresh Kafka cluster node", REFRESH_ICON) {
        override fun actionPerformed(e: AnActionEvent) {
            val node = tree.selectionPaths[0].path[1]
            if (node is KafkaNode) {
                refreshNode(node)
            }
        }

        override fun update (e: AnActionEvent) {
            e.presentation.isEnabled = isRootNodeSelected()
        }
    }

    inner class ImportAction : AnAction("Import clusters", "Import clusters from file", IMPORT_ICON), AnAction.TransparentUpdate {
        override fun actionPerformed(e: AnActionEvent) {
            val fcd = FileChooserDescriptor(true, false, false, false, false, false)
            val file = FileChooser.chooseFile(fcd, project, null)
            if (file != null) {
                val clusters = Gson().fromJson(String(Files.readAllBytes(Paths.get(file.canonicalPath))), Map::class.java)
                clusters.forEach {addCluster((it.value as Map<String, String>).toMutableMap())}
            }
        }
    }

    inner class ExportAction : AnAction("Export clusters", "Export clusters to file", EXPORT_ICON), AnAction.TransparentUpdate {
        override fun actionPerformed(e: AnActionEvent) {
            val fcd = FileChooserDescriptor(false, true, false, false, false, false)
            val file = FileChooser.chooseFile(fcd, project, null)
            if (file != null) {
                background("Save clusters") {
                    val clusters = GsonBuilder().setPrettyPrinting().create().toJson(config.clusters).toString();
                    val get = Paths.get(file.canonicalPath + File.separator + "clusters.json")
                    Files.write(get, clusters.toByteArray())
                    file.fileSystem.refresh(true)
                }
            }
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

    fun background(title: String, queue: BackgroundTaskQueue = taskQueue, task: (ProgressIndicator) -> Unit) {
        background(project, title, queue, task)
    }

    class CancelableTask(title: String, project: Project?, val task: (ProgressIndicator) -> Unit) : Task.Backgroundable(project, title, true) {
        var cancel = false
        override fun run(indicator: ProgressIndicator) {
            LOG.info("background task started:$title")
            task(indicator)
            LOG.info("background task complete:$title")
        }

        override fun onCancel() {
            cancel = true
            super.onCancel()
        }
    }

    fun background(project: Project?, title: String, queue: BackgroundTaskQueue, task: (ProgressIndicator) -> Unit) {
        queue.run(CancelableTask(title, project, task))
    }

    fun openEditor(name: String, content: String, language: String) {
        val psiFileFromText: PsiFile = PsiFileFactory.getInstance(project).createFileFromText(name,
                Language.findLanguageByID(language)!!, content, true, false)
        FileEditorManager.getInstance(project).openFile(psiFileFromText.virtualFile, true)
    }

    private fun <T> format(s: T) : Pair<String, String> {
        val value = when (s) {
            is String -> s
            is ByteArray -> String(s)
            else -> s.toString()
        }.trim()
        return if (value.startsWith("{")) {
                try {
                    val gson = GsonBuilder().setPrettyPrinting().create()
                    "JSON" to gson.toJson(gson.fromJson(value, Map::class.java))
                } catch (e: JsonSyntaxException) {
                    "TEXT" to value
                }
            } else if (value.startsWith("<")) {
                try {
                    val dbf = DocumentBuilderFactory.newInstance()
                    val db = dbf.newDocumentBuilder()

                    val transformer = TransformerFactory.newInstance().newTransformer()
                    transformer.setOutputProperty(OutputKeys.INDENT, "yes")
                    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2")
                    val result = StreamResult(StringWriter())
                    val source = DOMSource(db.parse(ByteArrayInputStream(value.toByteArray())))
                    transformer.transform(source, result)
                    "XML" to result.getWriter().toString()
                } catch (e: Exception) {
                    "TEXT" to value
                }
            } else "TEXT" to value
    }

    fun <K, V> printMessage(record: ConsumerRecord<K, V>) {
        foreground {
            val (_, key) = format(record.key())
            val (lang, value) =  format(record.value())

            if (config.config["printToEditorSelected"]?.toBoolean() != false) {
                    openEditor(key, value, lang)
            }
            if (config.config["printToFileSelected"]?.toBoolean() == true) {
                Files.write(Paths.get(config.config["printToFile"]), value.toByteArray(), StandardOpenOption.APPEND, StandardOpenOption.CREATE)
            }
            Notifications.Bus.notify(Notification("Kafkalytic", "topic:${record.topic()}",
                    "key:$key, partition:${record.partition()}, offset:${record.offset()}" +
                            if (config.config["printToEventSelected"]?.toBoolean() != false) ", message:\n$value" else "",
                    NotificationType.INFORMATION))
        }
    }

}

fun foreground(task: () -> Unit) {
    ApplicationManager.getApplication().invokeLater(task)
}

fun error(message: String, e: Throwable?) {
    LOG.info(message, e)
    foreground { Messages.showErrorDialog(message, "Kafka") }
}

fun info(message: String) {
    LOG.info(message)
    foreground { Messages.showInfoMessage(message, "Kafka") }
}




