package com.github.dmlbackup.ui

import com.github.dmlbackup.model.BackupRecord
import com.github.dmlbackup.service.RollbackService
import com.github.dmlbackup.storage.BackupStorage
import com.intellij.database.dataSource.DatabaseConnectionManager
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.Messages
import com.intellij.openapi.wm.ToolWindow
import com.intellij.openapi.wm.ToolWindowFactory
import com.intellij.ui.components.JBScrollPane
import com.intellij.ui.content.ContentFactory
import java.awt.BorderLayout
import java.awt.FlowLayout
import java.time.format.DateTimeFormatter
import javax.swing.*
import javax.swing.table.DefaultTableModel

class BackupHistoryToolWindowFactory : ToolWindowFactory {

    override fun createToolWindowContent(project: Project, toolWindow: ToolWindow) {
        val panel = BackupHistoryPanel(project)
        val content = ContentFactory.getInstance().createContent(panel, "", false)
        toolWindow.contentManager.addContent(content)
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
    /** 当前过滤后展示的记录 */
    private var records: List<BackupRecord> = emptyList()
    private val dataSourceComboBox = JComboBox<String>()

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

        // 工具栏
        val toolbar = JPanel(FlowLayout(FlowLayout.LEFT, 4, 2))
        toolbar.add(JLabel("DataSource:"))
        dataSourceComboBox.addActionListener { this.filterRecords() }
        toolbar.add(dataSourceComboBox)
        toolbar.add(this.createButton("Refresh") { this.loadRecords() })
        toolbar.add(this.createButton("Rollback") { this.doRollback() })
        toolbar.add(this.createButton("Detail") { this.showDetail() })
        add(toolbar, BorderLayout.NORTH)

        this.loadRecords()
    }

    private fun createButton(text: String, action: () -> Unit): JButton {
        val btn = JButton(text)
        btn.addActionListener { action() }
        return btn
    }

    private fun loadRecords() {
        try {
            allRecords = BackupStorage.findAll()

            // 更新数据源下拉框
            val selected = dataSourceComboBox.selectedItem as? String
            val dataSources = allRecords.map { this.extractDataSourceName(it.connectionInfo) }.distinct()
            dataSourceComboBox.removeAllItems()
            dataSourceComboBox.addItem("All")
            dataSources.forEach { dataSourceComboBox.addItem(it) }
            if (selected != null && selected in dataSources) dataSourceComboBox.selectedItem = selected

            this.filterRecords()
        } catch (e: Exception) {
            log.error("Failed to load backup records", e)
        }
    }

    private fun filterRecords() {
        val selected = dataSourceComboBox.selectedItem as? String
        records = if (selected == null || selected == "All") allRecords
        else allRecords.filter { this.extractDataSourceName(it.connectionInfo) == selected }

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

    /** 从 tableName 提取纯表名，格式 "schema.table" → "table" */
    private fun extractTableName(tableName: String): String = tableName.substringAfterLast(".")

    /** 从 tableName 提取库名，格式 "schema.table" → "schema" */
    private fun extractDatabaseName(tableName: String): String =
        if (tableName.contains(".")) tableName.substringBeforeLast(".") else ""

    /** 从 connectionInfo 提取数据源名称，格式为 "name (url)" → "name" */
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

    private fun doRollback() {
        val record = this.getSelectedRecord() ?: return

        if (record.status == "ROLLED_BACK") {
            Messages.showWarningDialog(project, "This record has already been rolled back", "DML Backup")
            return
        }

        // INSERT 部分列警告
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
            "Rollback ${record.operationType} on '${record.tableName}' (${record.rowCount} rows)?\n\n${record.originalSql}",
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

            // 匹配活跃连接：优先按 URL 精确匹配，fallback 到第一个 MySQL 连接
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

    private fun showDetail() {
        val record = this.getSelectedRecord() ?: return

        val detail = buildString {
            appendLine("ID:         ${record.id}")
            appendLine("Time:       ${record.createdAt.format(timeFmt)}")
            appendLine("Type:       ${record.operationType}")
            appendLine("Table:      ${record.tableName}")
            appendLine("Rows:       ${record.rowCount}")
            appendLine("Connection: ${record.connectionInfo}")
            appendLine("Status:     ${record.status}")
            appendLine()
            appendLine("Original SQL:")
            appendLine(record.originalSql)
            appendLine()
            appendLine("Backup Data:")
            appendLine(record.backupDataJson)
        }

        val textArea = JTextArea(detail)
        textArea.isEditable = false
        textArea.lineWrap = true
        textArea.wrapStyleWord = true

        val scrollPane = JScrollPane(textArea)
        scrollPane.preferredSize = java.awt.Dimension(600, 400)
        JOptionPane.showMessageDialog(this, scrollPane, "Backup Detail #${record.id}", JOptionPane.INFORMATION_MESSAGE)
    }
}
