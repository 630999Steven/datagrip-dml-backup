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
        this.handleConsoleExecute(event)
    }

    /**
     * 处理 SQL Console 执行
     */
    private fun handleConsoleExecute(event: AnActionEvent) {
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

            val hasPending = this.invoke(mutator, "hasPendingChanges") as? Boolean ?: return
            if (!hasPending) return

            // 表名和 schema
            val gridHelper = this.invoke(grid, "getGridHelper")
            val rawTableName = this.invokeWith(gridHelper, "getTableName", grid)?.toString()
                ?: event.getData(CommonDataKeys.VIRTUAL_FILE)?.nameWithoutExtension
                ?: "unknown"
            // 从 getDatabaseTable 获取 schema（DasObject.getDasParent → schema name）
            val schema = this.resolveGridSchema(grid)
            val tableName = if (schema != null && !rawTableName.contains(".")) "$schema.$rawTableName" else rawTableName

            // MySQL 检查：通过 DataGridUtil.getDbms(grid) 判断
            if (!this.isGridMySql(grid)) return

            // 连接信息：通过 DataGridUtilCore.getDatabaseSystem(grid) 获取
            val connInfo = this.resolveConnectionInfo(grid)
            log.info("DML Backup: grid submit on '$tableName', connInfo='$connInfo'")

            // 获取 DataAccessType 枚举
            val dbDataType = this.findEnumValue("com.intellij.database.run.ui.DataAccessType", "DATABASE_DATA")
            val mutDataType = this.findEnumValue("com.intellij.database.run.ui.DataAccessType", "DATA_WITH_MUTATIONS")
            val dbModel = this.invokeWith(grid, "getDataModel", dbDataType)
            val mutModel = this.invokeWith(grid, "getDataModel", mutDataType)
            if (dbModel == null || mutModel == null) {
                log.warn("DML Backup: grid getDataModel failed, dbModel=$dbModel, mutModel=$mutModel")
                return
            }

            // 列信息：从 mutModel 获取完整列列表（dbModel 可能只有部分列）
            val columnIndices = this.invoke(mutModel, "getColumnIndices") ?: return
            val colIterable = this.invoke(columnIndices, "asIterable") as? Iterable<*> ?: return
            val colList = colIterable.toList()
            val columnNames = colList.map { colIdx ->
                val col = this.invokeWith(mutModel, "getColumn", colIdx)
                this.invoke(col, "getName")?.toString() ?: "unknown"
            }

            // 按 mutation type 分组
            val affectedRows = this.invoke(mutator, "getAffectedRows") ?: return
            val rowIterable = this.invoke(affectedRows, "asIterable") as? Iterable<*> ?: return

            val deleteRows = mutableListOf<Map<String, String?>>()
            val updateRows = mutableListOf<Map<String, String?>>()
            val insertRows = mutableListOf<Map<String, String?>>()

            for (rowIdx in rowIterable) {
                rowIdx ?: continue
                val mutType = this.invokeWith(mutator, "getMutationType", rowIdx) ?: continue
                when (mutType.toString()) {
                    "DELETE" -> deleteRows.add(this.readRowData(dbModel, rowIdx, colList, columnNames))
                    "MODIFY" -> updateRows.add(this.readRowData(dbModel, rowIdx, colList, columnNames))
                    "INSERT" -> insertRows.add(this.readRowData(mutModel, rowIdx, colList, columnNames))
                }
            }

            if (deleteRows.isNotEmpty()) this.saveGridBackup("DELETE", tableName, connInfo, deleteRows, "DELETE ${deleteRows.size} row(s) via visual editor")
            if (updateRows.isNotEmpty()) this.saveGridBackup("UPDATE", tableName, connInfo, updateRows, "UPDATE ${updateRows.size} row(s) via visual editor")
            if (insertRows.isNotEmpty()) this.saveGridBackup("INSERT", tableName, connInfo, insertRows, "INSERT ${insertRows.size} row(s) via visual editor")

            val total = deleteRows.size + updateRows.size + insertRows.size
            log.info("DML Backup: grid backup completed for $tableName, $total row(s)")
        } catch (e: Exception) {
            log.error("DML Backup: grid backup failed", e)
        }
    }

    /** Grid 中自增/生成列的占位值 */
    private val GRID_PLACEHOLDER_VALUES = setOf("GENERATED", "DEFAULT", "AUTO_INCREMENT")

    private fun readRowData(model: Any, rowIdx: Any, colList: List<Any?>, columnNames: List<String>): Map<String, String?> {
        val row = mutableMapOf<String, String?>()
        for ((i, colIdx) in colList.withIndex()) {
            colIdx ?: continue
            val value = this.invokeWith(model, "getValueAt", rowIdx, colIdx)
            val strValue = value?.toString()
            // 跳过 Grid 占位值（自增/生成列）
            if (strValue != null && strValue.uppercase() in GRID_PLACEHOLDER_VALUES) continue
            // null 值和字符串 "null" 都当 null 处理
            row[columnNames[i]] = if (strValue == null || strValue.equals("null", ignoreCase = true)) null else strValue
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
            partialColumns = false  // 可视化编辑器提交全部列，不存在部分列问题
        )

        val id = BackupStorage.save(record)
        log.info("DML Backup: saved grid $operationType backup id=$id for $tableName")
        BackupStorage.trimRecords(DmlBackupSettings.getInstance().maxRecords)
    }

    /**
     * 通过 DataGridUtilCore.getDatabaseTable(grid) 获取 schema/database 名
     */
    private fun resolveGridSchema(grid: Any): String? {
        try {
            val utilCoreClass = Class.forName("com.intellij.database.datagrid.DataGridUtilCore")
            val getTable = utilCoreClass.methods.find { it.name == "getDatabaseTable" && it.parameterCount == 1 }
            val dasObject = getTable?.invoke(null, grid) ?: return null
            // DasObject.getDasParent() 返回 schema/database
            val parent = this.invoke(dasObject, "getDasParent") ?: return null
            return this.invoke(parent, "getName")?.toString()
        } catch (_: Exception) {
            return null
        }
    }

    /**
     * 通过 DataGridUtil.getDbms(grid) 判断是否为 MySQL
     */
    private fun isGridMySql(grid: Any): Boolean {
        try {
            val utilClass = Class.forName("com.intellij.database.datagrid.DataGridUtil")
            val method = utilClass.methods.find { it.name == "getDbms" && it.parameterCount == 1 } ?: return true
            val dbms = method.invoke(null, grid) ?: return true
            val dbmsName = dbms.toString().uppercase()
            return dbmsName.contains("MYSQL") || dbmsName.contains("MARIADB")
        } catch (_: Exception) {
            return true // 获取失败不拦截
        }
    }

    /**
     * 通过 DataGridUtilCore.getDatabaseSystem(grid) → DbImplUtil.getMaybeLocalDataSource() 获取连接信息
     */
    private fun resolveConnectionInfo(grid: Any): String {
        // 路径1：DataGridUtilCore.getDatabaseSystem(grid) → DbDataSource
        try {
            val utilCoreClass = Class.forName("com.intellij.database.datagrid.DataGridUtilCore")
            val getDatabaseSystem = utilCoreClass.methods.find { it.name == "getDatabaseSystem" && it.parameterCount == 1 }
            val dbDataSource = getDatabaseSystem?.invoke(null, grid)
            if (dbDataSource != null) {
                // DbImplUtil.getMaybeLocalDataSource(dbDataSource) → LocalDataSource
                val implUtilClass = Class.forName("com.intellij.database.util.DbImplUtil")
                val getLocalDs = implUtilClass.methods.find { it.name == "getMaybeLocalDataSource" && it.parameterCount == 1 }
                val localDs = getLocalDs?.invoke(null, dbDataSource)
                if (localDs != null) {
                    val name = this.invoke(localDs, "getName")?.toString()
                    val url = this.invoke(localDs, "getUrl")?.toString()
                    if (name != null && url != null) return "$name ($url)"
                }
                // fallback：直接从 DbDataSource 取
                val name = this.invoke(dbDataSource, "getName")?.toString()
                val url = this.invoke(this.invoke(dbDataSource, "getConnectionConfig"), "getUrl")?.toString()
                if (name != null && url != null) return "$name ($url)"
            }
        } catch (_: Exception) {}

        // 路径2：hookup 强转 DatabaseGridDataHookUp → getDataSource()
        try {
            val hookup = this.invoke(grid, "getDataHookup")
            val ds = this.invoke(hookup, "getDataSource")
            if (ds != null) {
                val implUtilClass = Class.forName("com.intellij.database.util.DbImplUtil")
                val getLocalDs = implUtilClass.methods.find { it.name == "getMaybeLocalDataSource" && it.parameterCount == 1 }
                val localDs = getLocalDs?.invoke(null, ds)
                if (localDs != null) {
                    val name = this.invoke(localDs, "getName")?.toString()
                    val url = this.invoke(localDs, "getUrl")?.toString()
                    if (name != null && url != null) return "$name ($url)"
                }
            }
        } catch (_: Exception) {}

        return "unknown (visual editor)"
    }

    /** 反射调用无参方法 */
    private fun invoke(obj: Any?, methodName: String): Any? {
        obj ?: return null
        return try {
            obj.javaClass.methods.find { it.name == methodName && it.parameterCount == 0 }?.invoke(obj)
        } catch (_: Exception) { null }
    }

    /** 反射调用方法（按参数数量匹配） */
    private fun invokeWith(obj: Any?, methodName: String, vararg args: Any?): Any? {
        obj ?: return null
        return try {
            obj.javaClass.methods.find { it.name == methodName && it.parameterCount == args.size }?.invoke(obj, *args)
        } catch (_: Exception) { null }
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
