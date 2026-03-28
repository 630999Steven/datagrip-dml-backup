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

    private val columnNames = arrayOf("ID", "Time", "Type", "Table", "Rows", "Status")
    private val tableModel = object : DefaultTableModel(columnNames, 0) {
        override fun isCellEditable(row: Int, column: Int) = false
    }
    private val table = JTable(tableModel)
    private var records: List<BackupRecord> = emptyList()

    init {
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION)

        table.columnModel.getColumn(0).preferredWidth = 40   // ID
        table.columnModel.getColumn(1).preferredWidth = 130  // Time
        table.columnModel.getColumn(2).preferredWidth = 55   // Type
        table.columnModel.getColumn(3).preferredWidth = 120  // Table
        table.columnModel.getColumn(4).preferredWidth = 40   // Rows
        table.columnModel.getColumn(5).preferredWidth = 75   // Status

        add(JBScrollPane(table), BorderLayout.CENTER)

        // 工具栏
        val toolbar = JPanel(FlowLayout(FlowLayout.LEFT, 4, 2))
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
            records = BackupStorage.findAll()
            tableModel.rowCount = 0
            for (r in records) {
                tableModel.addRow(arrayOf(
                    r.id,
                    r.createdAt.format(timeFmt),
                    r.operationType,
                    r.tableName,
                    r.rowCount,
                    r.status
                ))
            }
        } catch (e: Exception) {
            log.error("Failed to load backup records", e)
        }
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

            val conn = DatabaseConnectionManager.getInstance().activeConnections.firstOrNull()
                ?: throw IllegalStateException("No active database connection")

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
