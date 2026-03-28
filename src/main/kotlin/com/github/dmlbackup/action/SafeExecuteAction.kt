package com.github.dmlbackup.action

import com.github.dmlbackup.service.BackupService
import com.github.dmlbackup.service.DmlType
import com.github.dmlbackup.service.SqlParser
import com.github.dmlbackup.settings.DmlBackupSettings
import com.intellij.database.console.JdbcConsole
import com.intellij.openapi.actionSystem.*
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.editor.Editor

/**
 * 包装 DataGrip 的 Console.Execute Action，在执行 DELETE/UPDATE 前自动静默备份数据
 */
class SafeExecuteAction : AnAction() {

    private val log = Logger.getInstance(SafeExecuteAction::class.java)

    override fun actionPerformed(e: AnActionEvent) {
        val settings = DmlBackupSettings.getInstance()
        if (!settings.enabled) return this.delegateOriginal(e)

        val editor = e.getData(CommonDataKeys.EDITOR) ?: return this.delegateOriginal(e)
        val sql = this.getExecutableSql(editor)
        if (sql.isNullOrBlank()) return this.delegateOriginal(e)

        val console = JdbcConsole.findConsole(e)
        if (console == null || !BackupService.isMySql(console)) return this.delegateOriginal(e)

        val cleaned = SqlParser.removeComments(sql)
        val statements = SqlParser.splitStatements(cleaned)

        for (stmt in statements) {
            val parsed = SqlParser.parse(stmt) ?: continue
            if (parsed.type == DmlType.OTHER) continue
            try {
                BackupService.backup(console, parsed, stmt)
                log.info("DML backup completed for: ${parsed.tableName}")
            } catch (ex: Exception) {
                log.error("Backup failed, continue executing original SQL", ex)
            }
        }

        this.delegateOriginal(e)
    }

    override fun update(e: AnActionEvent) {
        val original = ActionManager.getInstance().getAction("Console.Execute")
        if (original != null && original !== this) {
            original.update(e)
        } else {
            e.presentation.isEnabledAndVisible = e.getData(CommonDataKeys.EDITOR) != null
        }
    }

    override fun getActionUpdateThread(): ActionUpdateThread = ActionUpdateThread.BGT

    private fun delegateOriginal(e: AnActionEvent) {
        val original = ActionManager.getInstance().getAction("Console.Execute")
        if (original != null && original !== this) {
            original.actionPerformed(e)
        }
    }

    private fun getExecutableSql(editor: Editor): String? {
        val selectionModel = editor.selectionModel
        if (selectionModel.hasSelection()) return selectionModel.selectedText
        return editor.document.text.trim().ifEmpty { null }
    }
}
