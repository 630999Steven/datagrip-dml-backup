package com.github.dmlbackup.ui

import com.github.dmlbackup.model.BackupRecord
import com.github.dmlbackup.service.RollbackService
import com.github.dmlbackup.settings.DmlBackupConfigurable
import com.github.dmlbackup.storage.BackupStorage
import com.intellij.database.dataSource.DatabaseConnectionManager
import com.intellij.icons.AllIcons
import com.intellij.openapi.actionSystem.*
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.options.ShowSettingsUtil
import com.intellij.openapi.project.DumbAwareAction
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.Messages
import com.intellij.openapi.ui.SimpleToolWindowPanel
import com.intellij.openapi.ui.popup.JBPopupFactory
import com.intellij.openapi.wm.ToolWindow
import com.intellij.openapi.wm.ToolWindowFactory
import com.intellij.openapi.wm.ex.ToolWindowManagerListener
import com.intellij.ui.ScrollPaneFactory
import com.intellij.ui.content.ContentFactory
import com.intellij.ui.table.JBTable
import com.intellij.util.ui.JBUI
import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import java.time.format.DateTimeFormatter
import javax.swing.*
import javax.swing.table.DefaultTableModel

class BackupHistoryToolWindowFactory : ToolWindowFactory {

    override fun createToolWindowContent(project: Project, toolWindow: ToolWindow) {
        val panel = BackupHistoryPanel(project)
        val content = ContentFactory.getInstance().createContent(panel, "", false)
        toolWindow.contentManager.addContent(content)

        // 每次点击插件图标时自动刷新
        project.messageBus.connect().subscribe(ToolWindowManagerListener.TOPIC, object : ToolWindowManagerListener {
            override fun toolWindowShown(tw: ToolWindow) {
                if (tw.id == "DML Backup") panel.loadRecords()
            }
        })
    }
}

class BackupHistoryPanel(private val project: Project) : SimpleToolWindowPanel(true, false) {

    private val log = Logger.getInstance(BackupHistoryPanel::class.java)
    private val timeFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    private val columnNames = arrayOf("", "Time", "Type", "Table", "Database", "DataSource", "Rows", "Status")
    private val tableModel = object : DefaultTableModel(columnNames, 0) {
        override fun isCellEditable(row: Int, column: Int) = false
    }

    private val table = JBTable(tableModel).apply {
        setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION)
        setCellSelectionEnabled(true)
        setStriped(true)
        setShowGrid(true)
        gridColor = JBUI.CurrentTheme.CustomFrameDecorations.separatorForeground()
        rowHeight = JBUI.scale(24)
        tableHeader.reorderingAllowed = false
        emptyText.text = "No DML backup records"

        // ID 列：窄宽度 + 暗色字体
        columnModel.getColumn(0).preferredWidth = JBUI.scale(20)
        columnModel.getColumn(0).maxWidth = JBUI.scale(40)
        columnModel.getColumn(0).cellRenderer = javax.swing.table.DefaultTableCellRenderer().apply {
            foreground = java.awt.Color.GRAY
            horizontalAlignment = javax.swing.SwingConstants.CENTER
        }
        columnModel.getColumn(1).preferredWidth = JBUI.scale(120)
        columnModel.getColumn(2).preferredWidth = JBUI.scale(50)
        columnModel.getColumn(3).preferredWidth = JBUI.scale(90)
        columnModel.getColumn(4).preferredWidth = JBUI.scale(90)
        columnModel.getColumn(5).preferredWidth = JBUI.scale(80)
        columnModel.getColumn(6).preferredWidth = JBUI.scale(35)
        columnModel.getColumn(7).preferredWidth = JBUI.scale(70)
    }

    private var allRecords: List<BackupRecord> = emptyList()
    private var records: List<BackupRecord> = emptyList()
    private var selectedDataSource: String = "All"
    private var selectedDatabase: String = "All"

    init {
        // ActionToolbar
        val actionGroup = DefaultActionGroup().apply {
            add(RollbackAction())
            add(RefreshAction())
            addSeparator()
            add(DataSourceFilterAction())
            add(DatabaseFilterAction())
            addSeparator()
            add(ClearAction())
            add(SettingsAction())
        }
        val toolbar = ActionManager.getInstance().createActionToolbar("DmlBackupToolbar", actionGroup, true)
        toolbar.targetComponent = this
        setToolbar(toolbar.component)

        // 表格
        setContent(ScrollPaneFactory.createScrollPane(table, 0))

        // 点击 ID 列选中整行 + 右键菜单 + 双击查看
        table.addMouseListener(object : MouseAdapter() {
            override fun mousePressed(e: MouseEvent) {
                // 点击第 0 列（ID）时选中整行
                val col = table.columnAtPoint(e.point)
                val row = table.rowAtPoint(e.point)
                if (col == 0 && row >= 0) {
                    table.setRowSelectionInterval(row, row)
                    table.setColumnSelectionInterval(0, table.columnCount - 1)
                }
                this@BackupHistoryPanel.handlePopup(e)
            }
            override fun mouseReleased(e: MouseEvent) { this@BackupHistoryPanel.handlePopup(e) }
            override fun mouseClicked(e: MouseEvent) {
                if (e.clickCount == 2) this@BackupHistoryPanel.showCellPopup(e)
            }
        })

        this.loadRecords()
    }

    // ==================== Actions ====================

    private inner class RollbackAction : DumbAwareAction("Rollback", "Rollback selected record", AllIcons.Actions.Rollback) {
        override fun getActionUpdateThread() = ActionUpdateThread.EDT
        override fun update(e: AnActionEvent) { e.presentation.isEnabled = table.selectedRow >= 0 }
        override fun actionPerformed(e: AnActionEvent) { this@BackupHistoryPanel.doRollback() }
    }

    private inner class RefreshAction : DumbAwareAction("Refresh", "Refresh backup list", AllIcons.Actions.Refresh) {
        override fun getActionUpdateThread() = ActionUpdateThread.BGT
        override fun actionPerformed(e: AnActionEvent) { this@BackupHistoryPanel.loadRecords() }
    }

    private inner class ClearAction : DumbAwareAction("Clear", "Clear filtered records", AllIcons.Actions.GC) {
        override fun getActionUpdateThread() = ActionUpdateThread.BGT
        override fun actionPerformed(e: AnActionEvent) { this@BackupHistoryPanel.doClear() }
    }

    private inner class SettingsAction : DumbAwareAction("Settings", "Open DML Backup settings", AllIcons.General.Settings) {
        override fun getActionUpdateThread() = ActionUpdateThread.BGT
        override fun actionPerformed(e: AnActionEvent) {
            ShowSettingsUtil.getInstance().showSettingsDialog(project, DmlBackupConfigurable::class.java)
        }
    }

    private inner class DataSourceFilterAction : DumbAwareAction("DS: All", "Filter by data source", AllIcons.General.Filter) {
        override fun getActionUpdateThread() = ActionUpdateThread.EDT
        override fun update(e: AnActionEvent) { e.presentation.text = "DS: $selectedDataSource" }
        override fun actionPerformed(e: AnActionEvent) {
            val group = DefaultActionGroup()
            group.add(object : DumbAwareAction("All") {
                override fun actionPerformed(e: AnActionEvent) {
                    selectedDataSource = "All"; selectedDatabase = "All"
                    this@BackupHistoryPanel.filterRecords()
                }
            })
            allRecords.map { extractDataSourceName(it.connectionInfo) }.distinct().forEach { ds ->
                group.add(object : DumbAwareAction(ds) {
                    override fun actionPerformed(e: AnActionEvent) {
                        selectedDataSource = ds; selectedDatabase = "All"
                        this@BackupHistoryPanel.filterRecords()
                    }
                })
            }
            val popup = JBPopupFactory.getInstance().createActionGroupPopup(
                "Select DataSource", group, e.dataContext, JBPopupFactory.ActionSelectionAid.SPEEDSEARCH, false
            )
            popup.showUnderneathOf(e.inputEvent?.component ?: this@BackupHistoryPanel)
        }
    }

    private inner class DatabaseFilterAction : DumbAwareAction("DB: All", "Filter by database", AllIcons.Nodes.DataSchema) {
        override fun getActionUpdateThread() = ActionUpdateThread.EDT
        override fun update(e: AnActionEvent) {
            e.presentation.text = "DB: $selectedDatabase"
            e.presentation.isEnabled = selectedDataSource != "All"
        }
        override fun actionPerformed(e: AnActionEvent) {
            val group = DefaultActionGroup()
            group.add(object : DumbAwareAction("All") {
                override fun actionPerformed(e: AnActionEvent) {
                    selectedDatabase = "All"
                    this@BackupHistoryPanel.filterRecords()
                }
            })
            val filteredByDs = allRecords.filter { extractDataSourceName(it.connectionInfo) == selectedDataSource }
            filteredByDs.map { extractDatabaseName(it.tableName) }.filter { it.isNotEmpty() }.distinct().forEach { db ->
                group.add(object : DumbAwareAction(db) {
                    override fun actionPerformed(e: AnActionEvent) {
                        selectedDatabase = db
                        this@BackupHistoryPanel.filterRecords()
                    }
                })
            }
            val popup = JBPopupFactory.getInstance().createActionGroupPopup(
                "Select Database", group, e.dataContext, JBPopupFactory.ActionSelectionAid.SPEEDSEARCH, false
            )
            popup.showUnderneathOf(e.inputEvent?.component ?: this@BackupHistoryPanel)
        }
    }

    // ==================== Data ====================

    fun loadRecords() {
        try {
            allRecords = BackupStorage.findAll()
            this.filterRecords()
        } catch (e: Exception) {
            log.error("Failed to load backup records", e)
        }
    }

    private fun filterRecords() {
        records = allRecords
        if (selectedDataSource != "All") {
            records = records.filter { this.extractDataSourceName(it.connectionInfo) == selectedDataSource }
        }
        if (selectedDatabase != "All") {
            records = records.filter { this.extractDatabaseName(it.tableName) == selectedDatabase }
        }

        tableModel.rowCount = 0
        for (r in records) {
            tableModel.addRow(arrayOf(
                r.id,
                r.createdAt.format(timeFmt),
                r.operationType,
                this.extractTableName(r.tableName),
                this.extractDatabaseName(r.tableName),
                this.extractDataSourceName(r.connectionInfo),
                r.rowCount,
                r.status
            ))
        }
    }

    // ==================== Helpers ====================

    private fun extractTableName(tableName: String): String = tableName.substringAfterLast(".")
    private fun extractDatabaseName(tableName: String): String =
        if (tableName.contains(".")) tableName.substringBeforeLast(".") else ""
    private fun extractDataSourceName(connectionInfo: String): String =
        connectionInfo.substringBeforeLast(" (").ifEmpty { connectionInfo }

    private fun getSelectedRecord(): BackupRecord? {
        val row = table.selectedRow
        if (row < 0) {
            Messages.showWarningDialog(project, "Please select a record first", "DML Backup")
            return null
        }
        return records[row]
    }

    // ==================== UI Events ====================

    private fun handlePopup(e: MouseEvent) {
        if (!e.isPopupTrigger) return
        val row = table.rowAtPoint(e.point)
        if (row < 0) return
        table.setRowSelectionInterval(row, row)
        table.setColumnSelectionInterval(0, table.columnCount - 1)
        val record = records[row]

        val menu = JPopupMenu()
        val rollbackItem = JMenuItem("Rollback", AllIcons.Actions.Rollback)
        rollbackItem.isEnabled = record.status != "ROLLED_BACK"
        rollbackItem.addActionListener { this.doRollback() }
        menu.add(rollbackItem)

        val deleteItem = JMenuItem("Delete", AllIcons.General.Delete)
        deleteItem.addActionListener { this.doDeleteSelected() }
        menu.add(deleteItem)

        menu.show(table, e.x, e.y)
    }

    /** 双击弹出浮层显示完整单元格值 */
    private fun showCellPopup(e: MouseEvent) {
        val row = table.rowAtPoint(e.point)
        val col = table.columnAtPoint(e.point)
        if (row < 0 || col < 0) return
        val value = table.getValueAt(row, col)?.toString() ?: ""
        if (value.isEmpty()) return

        val textArea = JTextArea(value).apply {
            isEditable = false
            lineWrap = true
            wrapStyleWord = true
            font = table.font
            rows = minOf(value.length / 30 + 1, 8)
            columns = 30
        }
        val popup = JBPopupFactory.getInstance()
            .createComponentPopupBuilder(javax.swing.JScrollPane(textArea), textArea)
            .setRequestFocus(true)
            .setCancelOnClickOutside(true)
            .setCancelOnOtherWindowOpen(true)
            .createPopup()

        val cellRect = table.getCellRect(row, col, true)
        val point = java.awt.Point(cellRect.x, cellRect.y + cellRect.height)
        SwingUtilities.convertPointToScreen(point, table)
        popup.showInScreenCoordinates(table, point)
    }

    // ==================== Operations ====================

    private fun doClear() {
        if (records.isEmpty()) return
        val confirm = Messages.showYesNoDialog(
            project,
            "Delete all ${records.size} backup record(s) under current filter?\n\nThis cannot be undone.",
            "Clear Backup Records",
            Messages.getWarningIcon()
        )
        if (confirm != Messages.YES) return
        BackupStorage.deleteByIds(records.map { it.id })
        this.loadRecords()
    }

    private fun doDeleteSelected() {
        val record = this.getSelectedRecord() ?: return
        val confirm = Messages.showYesNoDialog(
            project,
            "Delete backup record #${record.id} (${record.operationType} on '${this.extractTableName(record.tableName)}')?\n\nThis cannot be undone.",
            "Delete Record",
            Messages.getWarningIcon()
        )
        if (confirm != Messages.YES) return
        BackupStorage.deleteById(record.id)
        this.loadRecords()
    }

    private fun doRollback() {
        val record = this.getSelectedRecord() ?: return

        if (record.status == "ROLLED_BACK") {
            Messages.showWarningDialog(project, "This record has already been rolled back", "DML Backup")
            return
        }

        if (record.partialColumns && record.operationType == "INSERT") {
            val proceed = Messages.showYesNoDialog(
                project,
                "This INSERT did not specify all columns. Rollback may not be precise " +
                    "(rows with default values in unspecified columns may be incorrectly matched).\n\nContinue?",
                "Partial Columns Warning",
                Messages.getWarningIcon()
            )
            if (proceed != Messages.YES) return
        }

        val confirm = Messages.showYesNoDialog(
            project,
            "Rollback ${record.operationType} on '${this.extractTableName(record.tableName)}' (${record.rowCount} rows)?\n\n${record.originalSql}",
            "Confirm Rollback",
            Messages.getQuestionIcon()
        )
        if (confirm != Messages.YES) return

        try {
            val sqls = RollbackService.generateRollbackSqls(record)
            if (sqls.isEmpty()) {
                Messages.showWarningDialog(project, "No rollback SQL generated (empty backup data)", "DML Backup")
                return
            }

            val targetUrl = record.connectionInfo.substringAfterLast("(").removeSuffix(")")
            val allConns = DatabaseConnectionManager.getInstance().activeConnections
            val isMySqlUrl = targetUrl.startsWith("jdbc:mysql://") || targetUrl.startsWith("jdbc:mariadb://")
            val conn = (if (isMySqlUrl) allConns.firstOrNull { it.connectionPoint.dataSource?.url == targetUrl } else null)
                ?: allConns.firstOrNull {
                    val url = it.connectionPoint.dataSource?.url ?: ""
                    url.startsWith("jdbc:mysql://") || url.startsWith("jdbc:mariadb://")
                }
                ?: throw IllegalStateException("No active MySQL connection found. Please connect to the database first.")

            val remoteConn = conn.remoteConnection
            remoteConn.setAutoCommit(false)
            try {
                val stmt = remoteConn.createStatement()
                for (sql in sqls) {
                    log.info("Executing rollback SQL: $sql")
                    stmt.executeUpdate(sql)
                }
                stmt.close()
                remoteConn.commit()
            } catch (ex: Exception) {
                remoteConn.rollback()
                throw ex
            } finally {
                remoteConn.setAutoCommit(true)
            }

            BackupStorage.updateStatus(record.id, "ROLLED_BACK")
            Messages.showInfoMessage(project, "Rollback completed! (${sqls.size} statements)", "DML Backup")
            this.loadRecords()
        } catch (e: Exception) {
            log.error("Rollback failed", e)
            Messages.showErrorDialog(project, "Rollback failed: ${e.message}", "DML Backup")
        }
    }
}
