package com.github.dmlbackup.ui

import com.github.dmlbackup.model.BackupRecord
import com.github.dmlbackup.service.RollbackService
import com.github.dmlbackup.settings.DmlBackupConfigurable
import com.github.dmlbackup.settings.DmlBackupSettings
import com.github.dmlbackup.storage.BackupStorage
import com.intellij.database.dataSource.DatabaseConnectionManager
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.options.ShowSettingsUtil
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.Messages
import com.intellij.openapi.wm.ToolWindow
import com.intellij.openapi.wm.ToolWindowFactory
import com.intellij.openapi.wm.ex.ToolWindowManagerListener
import com.intellij.ui.components.JBScrollPane
import com.intellij.ui.content.ContentFactory
import java.awt.BorderLayout
import java.awt.FlowLayout
import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import javax.swing.SwingUtilities
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

class BackupHistoryPanel(private val project: Project) : JPanel(BorderLayout()) {

    private val log = Logger.getInstance(BackupHistoryPanel::class.java)
    private val timeFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    private val columnNames = arrayOf("ID", "Time", "Type", "Table", "Database", "DataSource", "Rows", "Status")
    private val tableModel = object : DefaultTableModel(columnNames, 0) {
        override fun isCellEditable(row: Int, column: Int) = false
    }
    private val table = JTable(tableModel)
    private var allRecords: List<BackupRecord> = emptyList()
    private var records: List<BackupRecord> = emptyList()
    private val dataSourceComboBox = JComboBox<String>()
    private val databaseComboBox = JComboBox<String>()

    init {
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION)

        table.columnModel.getColumn(0).preferredWidth = 35   // ID
        table.columnModel.getColumn(1).preferredWidth = 120  // Time
        table.columnModel.getColumn(2).preferredWidth = 50   // Type
        table.columnModel.getColumn(3).preferredWidth = 90   // Table
        table.columnModel.getColumn(4).preferredWidth = 90   // Database
        table.columnModel.getColumn(5).preferredWidth = 80   // DataSource
        table.columnModel.getColumn(6).preferredWidth = 35   // Rows
        table.columnModel.getColumn(7).preferredWidth = 70   // Status

        add(JBScrollPane(table), BorderLayout.CENTER)

        // 工具栏：筛选行 + 操作行，WrapLayout 自动换行
        dataSourceComboBox.addActionListener { this.updateDatabaseComboBox(); this.filterRecords() }
        databaseComboBox.addActionListener { this.filterRecords() }

        val filterRow = JPanel(WrapLayout(FlowLayout.LEFT, 4, 2))
        filterRow.add(JLabel("DataSource:"))
        filterRow.add(dataSourceComboBox)
        filterRow.add(JLabel("Database:"))
        filterRow.add(databaseComboBox)

        val actionRow = JPanel(WrapLayout(FlowLayout.LEFT, 4, 2))
        actionRow.add(this.createButton("Rollback") { this.doRollback() })
        actionRow.add(this.createButton("Refresh") { this.loadRecords() })
        actionRow.add(this.createButton("Clear") { this.doClear() })
        actionRow.add(this.createButton("Settings") { ShowSettingsUtil.getInstance().showSettingsDialog(project, DmlBackupConfigurable::class.java) })

        val toolbar = JPanel()
        toolbar.layout = BoxLayout(toolbar, BoxLayout.Y_AXIS)
        toolbar.add(filterRow)
        toolbar.add(actionRow)
        add(toolbar, BorderLayout.NORTH)

        // 右键菜单 + 双击查看单元格值
        table.addMouseListener(object : MouseAdapter() {
            override fun mousePressed(e: MouseEvent) { this@BackupHistoryPanel.handlePopup(e) }
            override fun mouseReleased(e: MouseEvent) { this@BackupHistoryPanel.handlePopup(e) }
            override fun mouseClicked(e: MouseEvent) {
                if (e.clickCount == 2) this@BackupHistoryPanel.showCellValue(e)
            }
        })

        this.loadRecords()
    }

    private fun handlePopup(e: MouseEvent) {
        if (!e.isPopupTrigger) return
        val row = table.rowAtPoint(e.point)
        if (row < 0) return
        table.setRowSelectionInterval(row, row)
        val record = records[row]

        val menu = JPopupMenu()
        val rollbackItem = JMenuItem("Rollback")
        rollbackItem.isEnabled = record.status != "ROLLED_BACK"
        rollbackItem.addActionListener { this.doRollback() }
        menu.add(rollbackItem)

        val deleteItem = JMenuItem("Delete")
        deleteItem.addActionListener { this.doDeleteSelected() }
        menu.add(deleteItem)

        menu.show(table, e.x, e.y)
    }

    private fun createButton(text: String, action: () -> Unit): JButton {
        val btn = JButton(text)
        btn.addActionListener { action() }
        return btn
    }

    fun loadRecords() {
        try {
            allRecords = BackupStorage.findAll()

            // 更新数据源下拉框
            val selectedDs = dataSourceComboBox.selectedItem as? String
            val dataSources = allRecords.map { this.extractDataSourceName(it.connectionInfo) }.distinct()
            dataSourceComboBox.removeAllItems()
            dataSourceComboBox.addItem("All")
            dataSources.forEach { dataSourceComboBox.addItem(it) }
            if (selectedDs != null && selectedDs in dataSources) dataSourceComboBox.selectedItem = selectedDs

            this.updateDatabaseComboBox()
            this.filterRecords()
        } catch (e: Exception) {
            log.error("Failed to load backup records", e)
        }
    }

    /** 根据当前选中的数据源，更新库下拉框。DataSource 为 All 时禁用库筛选 */
    private fun updateDatabaseComboBox() {
        val selectedDs = dataSourceComboBox.selectedItem as? String

        databaseComboBox.removeAllItems()
        databaseComboBox.addItem("All")

        if (selectedDs == null || selectedDs == "All") {
            databaseComboBox.isEnabled = false
            return
        }

        databaseComboBox.isEnabled = true
        val selectedDb = databaseComboBox.selectedItem as? String
        val filteredByDs = allRecords.filter { this.extractDataSourceName(it.connectionInfo) == selectedDs }
        val databases = filteredByDs.map { this.extractDatabaseName(it.tableName) }.filter { it.isNotEmpty() }.distinct()
        databases.forEach { databaseComboBox.addItem(it) }
        if (selectedDb != null && selectedDb in databases) databaseComboBox.selectedItem = selectedDb
    }

    private fun filterRecords() {
        val selectedDs = dataSourceComboBox.selectedItem as? String
        val selectedDb = databaseComboBox.selectedItem as? String

        records = allRecords
        if (selectedDs != null && selectedDs != "All") {
            records = records.filter { this.extractDataSourceName(it.connectionInfo) == selectedDs }
        }
        if (selectedDb != null && selectedDb != "All") {
            records = records.filter { this.extractDatabaseName(it.tableName) == selectedDb }
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

    /** 双击单元格，在单元格位置弹出浮层显示完整值 */
    private fun showCellValue(e: MouseEvent) {
        val row = table.rowAtPoint(e.point)
        val col = table.columnAtPoint(e.point)
        if (row < 0 || col < 0) return
        val value = table.getValueAt(row, col)?.toString() ?: ""
        if (value.isEmpty()) return

        val textArea = JTextArea(value)
        textArea.isEditable = false
        textArea.lineWrap = true
        textArea.wrapStyleWord = true
        textArea.font = table.font
        textArea.rows = minOf(value.length / 30 + 1, 8)
        textArea.columns = 30

        val popup = com.intellij.openapi.ui.popup.JBPopupFactory.getInstance()
            .createComponentPopupBuilder(JBScrollPane(textArea), textArea)
            .setRequestFocus(true)
            .setCancelOnClickOutside(true)
            .setCancelOnOtherWindowOpen(true)
            .createPopup()

        val cellRect = table.getCellRect(row, col, true)
        val point = java.awt.Point(cellRect.x, cellRect.y + cellRect.height)
        SwingUtilities.convertPointToScreen(point, table)
        popup.showInScreenCoordinates(table, point)
    }

    private fun extractTableName(tableName: String): String = tableName.substringAfterLast(".")

    private fun extractDatabaseName(tableName: String): String =
        if (tableName.contains(".")) tableName.substringBeforeLast(".") else ""

    private fun extractDataSourceName(connectionInfo: String): String {
        return connectionInfo.substringBeforeLast(" (").ifEmpty { connectionInfo }
    }

    private fun getSelectedRecord(): BackupRecord? {
        val row = table.selectedRow
        if (row < 0) {
            Messages.showWarningDialog(project, "Please select a record first", "DML Backup")
            return null
        }
        return records[row]
    }

    /** 清空当前筛选条件下的所有记录 */
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

    /** 删除选中的单条记录 */
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

/**
 * 自动换行的 FlowLayout：空间不够时组件换到下一行，而非隐藏
 */
class WrapLayout(align: Int = FlowLayout.LEFT, hgap: Int = 5, vgap: Int = 5) : FlowLayout(align, hgap, vgap) {
    override fun preferredLayoutSize(target: java.awt.Container): java.awt.Dimension {
        return this.computeSize(target, true)
    }

    override fun minimumLayoutSize(target: java.awt.Container): java.awt.Dimension {
        return this.computeSize(target, false)
    }

    private fun computeSize(target: java.awt.Container, preferred: Boolean): java.awt.Dimension {
        synchronized(target.treeLock) {
            val targetWidth = if (target.width > 0) target.width else Int.MAX_VALUE
            val insets = target.insets
            val maxWidth = targetWidth - insets.left - insets.right - hgap * 2
            var x = 0
            var y = 0
            var rowHeight = 0

            for (comp in target.components) {
                if (!comp.isVisible) continue
                val d = if (preferred) comp.preferredSize else comp.minimumSize
                if (x > 0 && x + d.width > maxWidth) {
                    y += rowHeight + vgap
                    x = 0
                    rowHeight = 0
                }
                x += d.width + hgap
                rowHeight = maxOf(rowHeight, d.height)
            }
            y += rowHeight + insets.top + insets.bottom + vgap * 2
            return java.awt.Dimension(targetWidth, y)
        }
    }
}
