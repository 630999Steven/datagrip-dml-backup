package com.github.dmlbackup.listener

import com.github.dmlbackup.model.BackupRecord
import com.github.dmlbackup.service.BackupService
import com.github.dmlbackup.service.DmlType
import com.github.dmlbackup.service.ParsedDml
import com.github.dmlbackup.service.SqlParser
import com.github.dmlbackup.settings.DmlBackupSettings
import com.github.dmlbackup.storage.BackupStorage
import com.google.gson.Gson
import com.intellij.database.console.JdbcConsole
import com.intellij.notification.NotificationGroupManager
import com.intellij.notification.NotificationType
import com.intellij.openapi.actionSystem.*
import com.intellij.openapi.actionSystem.ex.AnActionListener
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.ui.Messages
import com.intellij.openapi.editor.Editor
import java.time.ZonedDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

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

        val dmlStatements = mutableListOf<Triple<String, ParsedDml, JdbcConsole>>()
        for (stmt in statements) {
            val parsed = SqlParser.parse(stmt)
            if (parsed != null && parsed.type != DmlType.OTHER) {
                dmlStatements.add(Triple(stmt, parsed, console))
            } else if (SqlParser.looksLikeDml(stmt)) {
                this.notifyUser("Unsupported DML syntax detected, this statement was NOT backed up:\n${stmt.take(100)}", NotificationType.WARNING)
            }
        }
        if (dmlStatements.isEmpty()) return

        // 大表保护：COUNT 预检 + 弹窗确认（带进度条，可取消）
        val maxRows = DmlBackupSettings.getInstance().maxBackupRows
        if (maxRows > 0) {
            for ((_, parsed, cons) in dmlStatements) {
                if (parsed.type == DmlType.INSERT) continue
                try {
                    val count = BackupService.countAffectedRows(cons, parsed)
                    if (count > maxRows) {
                        val choice = Messages.showYesNoDialog(
                            event.project,
                            "Table '${parsed.tableName}': affected rows ($count) exceed backup limit ($maxRows).\n\nContinue with backup? (SQL will execute regardless)",
                            "DML Backup - Large Table Warning",
                            "Continue Backup", "Skip Backup",
                            Messages.getWarningIcon()
                        )
                        if (choice != Messages.YES) return // 跳过备份，SQL 继续执行
                    }
                } catch (_: com.intellij.openapi.progress.ProcessCanceledException) {
                    log.info("DML Backup: count check cancelled by user for ${parsed.tableName}, skipping backup")
                    return // 用户取消了预检，跳过备份
                } catch (ex: Exception) {
                    log.warn("DML Backup: count check failed for ${parsed.tableName}", ex)
                }
            }
        }

        log.info("DML Backup: intercepted ${dmlStatements.size} DML statement(s)")

        val latch = CountDownLatch(1)
        val timedOut = AtomicBoolean(false)
        val cancelActions = mutableListOf<Runnable>()
        ApplicationManager.getApplication().executeOnPooledThread {
            try {
                for ((originalSql, parsed, cons) in dmlStatements) {
                    if (timedOut.get()) {
                        log.warn("DML Backup: skipping backup for ${parsed.tableName} due to timeout")
                        break
                    }
                    try {
                        BackupService.backup(cons, parsed, originalSql,
                            timedOutFlag = { timedOut.get() },
                            cancelHook = { cancelActions.add(it) })
                        log.info("DML Backup: backup completed for ${parsed.type} on ${parsed.tableName}")
                    } catch (ex: Exception) {
                        if (!timedOut.get()) {
                            log.error("DML Backup: backup failed for ${parsed.tableName}", ex)
                            this.notifyUser("Backup failed for ${parsed.type} on '${parsed.tableName}': ${ex.message}", NotificationType.ERROR)
                        }
                    }
                }
            } finally {
                latch.countDown()
            }
        }

        if (!latch.await(10, TimeUnit.SECONDS)) {
            timedOut.set(true)
            // 取消正在执行的 DB 查询
            cancelActions.forEach { try { it.run() } catch (_: Exception) {} }
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

            // 每个 model 各自的列信息（列索引不可跨 model 使用）
            val dbColInfo = this.getColumnInfo(dbModel)
            val mutColInfo = this.getColumnInfo(mutModel)
            if (dbColInfo == null && mutColInfo == null) return

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
                    "DELETE" -> if (dbColInfo != null) deleteRows.add(this.readRowData(dbModel, rowIdx, dbColInfo.first, dbColInfo.second))
                    "MODIFY" -> if (dbColInfo != null) updateRows.add(this.readRowData(dbModel, rowIdx, dbColInfo.first, dbColInfo.second))
                    "INSERT" -> if (mutColInfo != null) insertRows.add(this.readRowData(mutModel, rowIdx, mutColInfo.first, mutColInfo.second))
                }
            }

            // 尝试从 Grid 元数据获取主键列
            val primaryKeys = this.resolveGridPrimaryKeys(grid)
            val pkJson = if (primaryKeys.isNotEmpty()) gson.toJson(primaryKeys) else null

            if (deleteRows.isNotEmpty()) this.saveGridBackup("DELETE", tableName, connInfo, deleteRows, "DELETE ${deleteRows.size} row(s) via visual editor", pkJson)
            if (updateRows.isNotEmpty()) this.saveGridBackup("UPDATE", tableName, connInfo, updateRows, "UPDATE ${updateRows.size} row(s) via visual editor", pkJson)
            if (insertRows.isNotEmpty()) this.saveGridBackup("INSERT", tableName, connInfo, insertRows, "INSERT ${insertRows.size} row(s) via visual editor", pkJson)

            val total = deleteRows.size + updateRows.size + insertRows.size
            log.info("DML Backup: grid backup completed for $tableName, $total row(s)")
        } catch (e: Exception) {
            log.error("DML Backup: grid backup failed", e)
        }
    }

    /** 获取 model 的列索引和列名列表 */
    private fun getColumnInfo(model: Any): Pair<List<Any?>, List<String>>? {
        val indices = this.invoke(model, "getColumnIndices") ?: return null
        val iterable = this.invoke(indices, "asIterable") as? Iterable<*> ?: return null
        val colList = iterable.toList()
        val names = colList.map { colIdx ->
            val col = this.invokeWith(model, "getColumn", colIdx)
            this.invoke(col, "getName")?.toString() ?: "unknown"
        }
        return Pair(colList, names)
    }

    /** Grid 中自增/生成列的占位值 */
    private val GRID_PLACEHOLDER_VALUES = setOf("GENERATED", "DEFAULT", "AUTO_INCREMENT")

    private fun readRowData(model: Any, rowIdx: Any, colList: List<Any?>, columnNames: List<String>): Map<String, String?> {
        val row = mutableMapOf<String, String?>()
        for ((i, colIdx) in colList.withIndex()) {
            colIdx ?: continue
            val value = this.invokeWith(model, "getValueAt", rowIdx, colIdx)
            val strValue = value?.toString()
            log.debug("DML Backup GRID readRow: col=${columnNames[i]}, class=${value?.javaClass?.name}")
            // 跳过 Grid 占位值（自增/生成列）
            if (strValue != null && strValue.uppercase() in GRID_PLACEHOLDER_VALUES) continue
            // null 值和字符串 "null" 都当 null 处理
            row[columnNames[i]] = if (strValue == null || strValue.equals("null", ignoreCase = true)) null else strValue
        }
        log.debug("DML Backup GRID readRow: ${row.size} columns")
        return row
    }

    private val nullSafeGson = com.google.gson.GsonBuilder().serializeNulls().create()

    private fun saveGridBackup(operationType: String, tableName: String, connInfo: String,
                               rows: List<Map<String, String?>>, originalSql: String, primaryKeys: String? = null) {
        // INSERT 可能跳过了 GENERATED/DEFAULT 列，标记为 partial
        val partial = operationType == "INSERT"
        val record = BackupRecord(
            createdAt = ZonedDateTime.now(),
            operationType = operationType,
            tableName = tableName,
            originalSql = originalSql,
            connectionInfo = connInfo,
            backupDataJson = nullSafeGson.toJson(rows),
            rowCount = rows.size,
            primaryKeys = primaryKeys,
            partialColumns = partial
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
     * 通过 DataGridUtilCore.getDatabaseTable(grid) 获取主键列名
     */
    private fun resolveGridPrimaryKeys(grid: Any): List<String> {
        try {
            val utilCoreClass = Class.forName("com.intellij.database.datagrid.DataGridUtilCore")
            val getTable = utilCoreClass.methods.find { it.name == "getDatabaseTable" && it.parameterCount == 1 }
            val dasTable = getTable?.invoke(null, grid) ?: return emptyList()

            // DasUtil.getPrimaryKey(DasTable) → DasIndex
            val dasUtilClass = Class.forName("com.intellij.database.util.DasUtil")
            val getPk = dasUtilClass.methods.find { it.name == "getPrimaryKey" && it.parameterCount == 1 }
            val pkIndex = getPk?.invoke(null, dasTable) ?: return emptyList()

            // DasIndex.getColumnsRef() → Iterable<DasColumn>
            val columnsRef = this.invoke(pkIndex, "getColumnsRef") ?: return emptyList()
            val names = this.invoke(columnsRef, "names")
            val iterable = this.invoke(names, "asIterable") as? Iterable<*>
                ?: (names as? Iterable<*>)
                ?: return emptyList()
            return iterable.mapNotNull { it?.toString() }
        } catch (_: Exception) {
            return emptyList()
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
