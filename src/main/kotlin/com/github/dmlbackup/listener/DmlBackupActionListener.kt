package com.github.dmlbackup.listener

import com.github.dmlbackup.service.BackupService
import com.github.dmlbackup.service.DmlType
import com.github.dmlbackup.service.SqlParser
import com.github.dmlbackup.settings.DmlBackupSettings
import com.intellij.database.console.JdbcConsole
import com.intellij.notification.NotificationGroupManager
import com.intellij.notification.NotificationType
import com.intellij.openapi.actionSystem.ActionManager
import com.intellij.openapi.actionSystem.AnAction
import com.intellij.openapi.actionSystem.AnActionEvent
import com.intellij.openapi.actionSystem.CommonDataKeys
import com.intellij.openapi.actionSystem.ex.AnActionListener
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.editor.Editor
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class DmlBackupActionListener : AnActionListener {

    private val log = Logger.getInstance(DmlBackupActionListener::class.java)

    override fun beforeActionPerformed(action: AnAction, event: AnActionEvent) {
        val actionId = ActionManager.getInstance().getId(action) ?: return
        if (actionId != "Console.Jdbc.Execute") return

        if (!DmlBackupSettings.getInstance().enabled) return

        val editor = event.getData(CommonDataKeys.EDITOR) ?: return
        val sql = this.getExecutableSql(editor)
        if (sql.isNullOrBlank()) return

        val console = JdbcConsole.findConsole(event) ?: return

        // MySQL 检查
        if (!BackupService.isMySql(console)) return

        val cleaned = SqlParser.removeComments(sql)
        val statements = SqlParser.splitStatements(cleaned)

        val dmlStatements = statements.mapNotNull { stmt ->
            val parsed = SqlParser.parse(stmt)
            if (parsed != null && parsed.type != DmlType.OTHER) Triple(stmt, parsed, console)
            else null
        }

        if (dmlStatements.isEmpty()) return

        log.info("DML Backup: intercepted ${dmlStatements.size} DML statement(s)")

        val latch = CountDownLatch(1)
        ApplicationManager.getApplication().executeOnPooledThread {
            try {
                for ((originalSql, parsed, cons) in dmlStatements) {
                    try {
                        BackupService.backup(cons, parsed, originalSql)
                        log.info("DML Backup: backup completed for ${parsed.type} on ${parsed.tableName}")
                    } catch (ex: Exception) {
                        log.error("DML Backup: backup failed for ${parsed.tableName}", ex)
                        this.notifyUser(
                            "Backup failed for ${parsed.type} on '${parsed.tableName}': ${ex.message}",
                            NotificationType.ERROR
                        )
                    }
                }
            } finally {
                latch.countDown()
            }
        }

        if (!latch.await(10, TimeUnit.SECONDS)) {
            log.warn("DML Backup: backup timed out after 10s, proceeding with action")
            this.notifyUser(
                "DML backup timed out. This operation was NOT backed up, SQL will execute normally.",
                NotificationType.WARNING
            )
        }
    }

    private fun getExecutableSql(editor: Editor): String? {
        val selectionModel = editor.selectionModel
        if (selectionModel.hasSelection()) return selectionModel.selectedText
        return editor.document.text.trim().ifEmpty { null }
    }

    private fun notifyUser(content: String, type: NotificationType) {
        NotificationGroupManager.getInstance()
            .getNotificationGroup("DML Backup")
            .createNotification(content, type)
            .notify(null)
    }
}
