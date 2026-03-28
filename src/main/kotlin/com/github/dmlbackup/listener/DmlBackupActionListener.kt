package com.github.dmlbackup.listener

import com.github.dmlbackup.model.BackupRecord
import com.github.dmlbackup.service.BackupService
import com.github.dmlbackup.service.DmlType
import com.github.dmlbackup.service.SqlParser
import com.github.dmlbackup.settings.DmlBackupSettings
import com.github.dmlbackup.storage.BackupStorage
import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.intellij.database.console.JdbcConsole
import com.intellij.notification.NotificationGroupManager
import com.intellij.notification.NotificationType
import com.intellij.openapi.actionSystem.*
import com.intellij.openapi.actionSystem.ex.AnActionListener
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.editor.Editor
import java.time.ZonedDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class DmlBackupActionListener : AnActionListener {

    private val log = Logger.getInstance(DmlBackupActionListener::class.java)
    private val gson = Gson()
    private val gridDataKey = DataKey.create<Any>("DATA_GRID_KEY")

    override fun beforeActionPerformed(action: AnAction, event: AnActionEvent) {
        val actionId = ActionManager.getInstance().getId(action) ?: return

        if (actionId == "Console.TableResult.Submit") {
            this.handleGridSubmit(event)
            return
        }

        if (actionId != "Console.Jdbc.Execute") return

        if (!DmlBackupSettings.getInstance().enabled) return

        val editor = event.getData(CommonDataKeys.EDITOR) ?: return
        val sql = this.getExecutableSql(editor)
        if (sql.isNullOrBlank()) return

        val console = JdbcConsole.findConsole(event) ?: return
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
                        this.notifyUser("Backup failed for ${parsed.type} on '${parsed.tableName}': ${ex.message}", NotificationType.ERROR)
                    }
                }
            } finally {
                latch.countDown()
            }
        }

        if (!latch.await(10, TimeUnit.SECONDS)) {
            log.warn("DML Backup: backup timed out after 10s, proceeding with action")
            this.notifyUser("DML backup timed out. This operation was NOT backed up, SQL will execute normally.", NotificationType.WARNING)
        }
    }

    /**
     * 处理可视化表格编辑器的 Submit 操作，通过反射访问 DataGrid API
     */
    private fun handleGridSubmit(event: AnActionEvent) {
        if (!DmlBackupSettings.getInstance().enabled) return

        try {
            val grid = event.getData(gridDataKey) ?: return
            val hookup = this.invoke(grid, "getDataHookup") ?: return
            val mutator = this.invoke(hookup, "getMutator") ?: return

            // 检查是否有待提交的变更
            val hasPending = this.invoke(mutator, "hasPendingChanges") as? Boolean ?: return
            if (!hasPending) return

            // 获取表名
            val gridHelper = this.invoke(grid, "getGridHelper")
            val tableName = (this.invokeWith(gridHelper, "getTableName", grid) ?: "unknown").toString()

            // 获取连接信息
            val connInfo = this.resolveConnectionInfo(grid)
            if (!connInfo.contains("mysql", ignoreCase = true) && !connInfo.contains("mariadb", ignoreCase = true)) return

            // 获取原始数据 model (DataAccessType.DATABASE_DATA)
            val dbDataType = this.findEnumValue("com.intellij.database.run.ui.DataAccessType", "DATABASE_DATA")
            val mutDataType = this.findEnumValue("com.intellij.database.run.ui.DataAccessType", "DATA_WITH_MUTATIONS")
            val dbModel = this.invokeWith(grid, "getDataModel", dbDataType) ?: return
            val mutModel = this.invokeWith(grid, "getDataModel", mutDataType) ?: return

            // 获取列信息
            val columnIndices = this.invoke(dbModel, "getColumnIndices") ?: return
            val colIterable = this.invoke(columnIndices, "asIterable") as? Iterable<*> ?: return
            val colList = colIterable.toList()
            val columnNames = colList.map { colIdx ->
                val col = this.invokeWith(dbModel, "getColumn", colIdx)
                this.invoke(col, "getName")?.toString() ?: "unknown"
            }

            // 获取受影响的行
            val affectedRows = this.invoke(mutator, "getAffectedRows") ?: return
            val rowIterable = this.invoke(affectedRows, "asIterable") as? Iterable<*> ?: return

            val deleteRows = mutableListOf<Map<String, String?>>()
            val updateRows = mutableListOf<Map<String, String?>>()
            val insertRows = mutableListOf<Map<String, String?>>()

            for (rowIdx in rowIterable) {
                rowIdx ?: continue
                val mutType = this.invokeWith(mutator, "getMutationType", rowIdx) ?: continue
                val typeName = mutType.toString()

                when (typeName) {
                    "DELETE" -> deleteRows.add(this.readRowData(dbModel, rowIdx, colList, columnNames))
                    "MODIFY" -> updateRows.add(this.readRowData(dbModel, rowIdx, colList, columnNames))
                    "INSERT" -> insertRows.add(this.readRowData(mutModel, rowIdx, colList, columnNames))
                }
            }

            if (deleteRows.isNotEmpty()) this.saveGridBackup("DELETE", tableName, connInfo, deleteRows, "DELETE ${deleteRows.size} row(s) via visual editor")
            if (updateRows.isNotEmpty()) this.saveGridBackup("UPDATE", tableName, connInfo, updateRows, "UPDATE ${updateRows.size} row(s) via visual editor")
            if (insertRows.isNotEmpty()) this.saveGridBackup("INSERT", tableName, connInfo, insertRows, "INSERT ${insertRows.size} row(s) via visual editor")

            val total = deleteRows.size + updateRows.size + insertRows.size
            log.info("DML Backup: grid submit backup completed for $tableName, $total row(s)")
        } catch (e: Exception) {
            log.error("DML Backup: grid submit backup failed", e)
        }
    }

    private fun readRowData(model: Any, rowIdx: Any, colList: List<Any?>, columnNames: List<String>): Map<String, String?> {
        val row = mutableMapOf<String, String?>()
        for ((i, colIdx) in colList.withIndex()) {
            colIdx ?: continue
            val value = this.invokeWith(model, "getValueAt", rowIdx, colIdx)
            row[columnNames[i]] = value?.toString()
        }
        return row
    }

    private fun saveGridBackup(operationType: String, tableName: String, connInfo: String,
                               rows: List<Map<String, String?>>, originalSql: String) {
        val jsonArray = JsonArray()
        for (row in rows) {
            val obj = JsonObject()
            for ((key, value) in row) {
                if (value == null) obj.add(key, JsonNull.INSTANCE)
                else obj.addProperty(key, value)
            }
            jsonArray.add(obj)
        }

        val record = BackupRecord(
            createdAt = ZonedDateTime.now(),
            operationType = operationType,
            tableName = tableName,
            originalSql = originalSql,
            connectionInfo = connInfo,
            backupDataJson = gson.toJson(jsonArray),
            rowCount = rows.size,
            partialColumns = operationType == "INSERT"
        )

        val id = BackupStorage.save(record)
        log.info("DML Backup: saved grid $operationType backup id=$id for $tableName")
        BackupStorage.trimRecords(DmlBackupSettings.getInstance().maxRecords)
    }

    private fun resolveConnectionInfo(grid: Any): String {
        try {
            // 尝试通过 hookup 链获取 data source
            val hookup = this.invoke(grid, "getDataHookup") ?: return "unknown"
            for (methodName in hookup.javaClass.methods.map { it.name }) {
                if (methodName.contains("DataSource", ignoreCase = true) && hookup.javaClass.getMethod(methodName).parameterCount == 0) {
                    val ds = hookup.javaClass.getMethod(methodName).invoke(hookup) ?: continue
                    val name = this.invoke(ds, "getName")?.toString() ?: continue
                    val url = this.invoke(ds, "getUrl")?.toString() ?: continue
                    return "$name ($url)"
                }
            }
        } catch (_: Exception) {}
        return "unknown (visual editor)"
    }

    /** 反射调用无参方法 */
    private fun invoke(obj: Any?, methodName: String): Any? {
        obj ?: return null
        return obj.javaClass.methods.find { it.name == methodName && it.parameterCount == 0 }?.invoke(obj)
    }

    /** 反射调用单参数方法 */
    private fun invokeWith(obj: Any?, methodName: String, vararg args: Any?): Any? {
        obj ?: return null
        val method = obj.javaClass.methods.find { it.name == methodName && it.parameterCount == args.size } ?: return null
        return method.invoke(obj, *args)
    }

    /** 通过类名和枚举值名获取枚举实例 */
    private fun findEnumValue(className: String, valueName: String): Any? {
        val cls = Class.forName(className)
        return cls.enumConstants?.find { (it as Enum<*>).name == valueName }
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
