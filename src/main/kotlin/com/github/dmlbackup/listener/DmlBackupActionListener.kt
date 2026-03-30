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
import java.sql.Types
import java.time.ZonedDateTime
import java.util.Base64
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

        log.info("DML Backup: intercepted ${dmlStatements.size} DML statement(s)")

        val maxRows = DmlBackupSettings.getInstance().maxBackupRows
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
                        // 大表保护：在后台线程做 COUNT 预检
                        if (maxRows > 0 && parsed.type != DmlType.INSERT) {
                            val count = BackupService.countAffectedRows(cons, parsed)
                            if (count > maxRows) {
                                log.warn("DML Backup: table '${parsed.tableName}' affected rows ($count) exceed limit ($maxRows), skipping backup")
                                this.notifyUser("Table '${parsed.tableName}': affected rows ($count) exceed backup limit ($maxRows). Backup skipped.", NotificationType.WARNING)
                                continue
                            }
                        }
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
                    "DELETE" -> if (dbColInfo != null) deleteRows.add(this.readRowData(dbModel, rowIdx, dbColInfo))
                    "MODIFY" -> if (dbColInfo != null) updateRows.add(this.readRowData(dbModel, rowIdx, dbColInfo))
                    "INSERT" -> if (mutColInfo != null) insertRows.add(this.readRowData(mutModel, rowIdx, mutColInfo))
                }
            }

            // 尝试从 Grid 元数据获取主键列
            val primaryKeys = this.resolveGridPrimaryKeys(grid)
            val pkJson = if (primaryKeys.isNotEmpty()) gson.toJson(primaryKeys) else null

            // 用 dbColInfo 的类型信息（DELETE/UPDATE 用 db model 列，INSERT 用 mut model 列）
            val dbTypes = dbColInfo?.let { info -> info.names.zip(info.jdbcTypes).toMap() }
            val mutTypes = mutColInfo?.let { info -> info.names.zip(info.jdbcTypes).toMap() }

            if (deleteRows.isNotEmpty()) this.saveGridBackup("DELETE", tableName, connInfo, deleteRows, "DELETE ${deleteRows.size} row(s) via visual editor", pkJson, dbTypes)
            if (updateRows.isNotEmpty()) this.saveGridBackup("UPDATE", tableName, connInfo, updateRows, "UPDATE ${updateRows.size} row(s) via visual editor", pkJson, dbTypes)
            if (insertRows.isNotEmpty()) this.saveGridBackup("INSERT", tableName, connInfo, insertRows, "INSERT ${insertRows.size} row(s) via visual editor", pkJson, mutTypes)

            val total = deleteRows.size + updateRows.size + insertRows.size
            log.info("DML Backup: grid backup completed for $tableName, $total row(s)")
        } catch (e: Exception) {
            log.error("DML Backup: grid backup failed", e)
        }
    }

    /** 列信息：索引列表、列名列表、JDBC 类型列表 */
    private data class ColumnInfo(val indices: List<Any?>, val names: List<String>, val jdbcTypes: List<Int>)

    /** 获取 model 的列索引、列名和 JDBC 类型 */
    private fun getColumnInfo(model: Any): ColumnInfo? {
        val indices = this.invoke(model, "getColumnIndices") ?: return null
        val iterable = this.invoke(indices, "asIterable") as? Iterable<*> ?: return null
        val colList = iterable.toList()
        val names = mutableListOf<String>()
        val types = mutableListOf<Int>()
        for (colIdx in colList) {
            val col = this.invokeWith(model, "getColumn", colIdx)
            names.add(this.invoke(col, "getName")?.toString() ?: "unknown")
            types.add(this.resolveColumnJdbcType(col))
        }
        return ColumnInfo(colList, names, types)
    }

    /** 从 DataGrid column 对象反射获取 JDBC 类型 */
    private fun resolveColumnJdbcType(column: Any?): Int {
        column ?: return java.sql.Types.VARCHAR
        // 路径1: column.getColumnType().getJdbcType()（DatabaseTableColumn → DbmsColumnType）
        val colType = this.invoke(column, "getColumnType")
        if (colType != null) {
            val jdbcType = this.invoke(colType, "getJdbcType")
            if (jdbcType is Number) return jdbcType.toInt()
        }
        // 路径2: column.getType()（返回 int 或枚举）
        val typeVal = this.invoke(column, "getType")
        if (typeVal is Number) return typeVal.toInt()
        // 默认 VARCHAR
        return java.sql.Types.VARCHAR
    }

    /** Grid 中自增/生成列的占位值 */
    private val GRID_PLACEHOLDER_VALUES = setOf("GENERATED", "DEFAULT", "AUTO_INCREMENT")
    private val BINARY_TYPES = setOf(Types.BLOB, Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY)

    private fun readRowData(model: Any, rowIdx: Any, colInfo: ColumnInfo): Map<String, String?> {
        val row = mutableMapOf<String, String?>()
        for ((i, colIdx) in colInfo.indices.withIndex()) {
            colIdx ?: continue
            val value = this.invokeWith(model, "getValueAt", rowIdx, colIdx)
            val jdbcType = colInfo.jdbcTypes[i]
            log.debug("DML Backup GRID readRow: col=${colInfo.names[i]}, class=${value?.javaClass?.name}, jdbcType=$jdbcType")

            // 跳过 Grid 占位值（自增/生成列）
            val strValue = value?.toString()
            if (strValue != null && strValue.uppercase() in GRID_PLACEHOLDER_VALUES) continue

            // 时间类型：存 epoch millis，回滚时由 JDBC 驱动处理时区
            if ((jdbcType == Types.DATE || jdbcType == Types.TIME || jdbcType == Types.TIMESTAMP) && value is java.util.Date) {
                row[colInfo.names[i]] = value.time.toString()
                continue
            }

            // 按 JDBC 类型提取值
            val extracted = this.extractGridValue(value, jdbcType)
            row[colInfo.names[i]] = extracted
        }
        log.debug("DML Backup GRID readRow: ${row.size} columns")
        return row
    }

    /** 按 JDBC 类型提取 Grid 单元格值 */
    private fun extractGridValue(value: Any?, jdbcType: Int): String? {
        if (value == null) return null
        val strValue = value.toString()
        if (strValue.equals("null", ignoreCase = true)) return null

        return when (jdbcType) {
            in BINARY_TYPES -> when (value) {
                is ByteArray -> Base64.getEncoder().encodeToString(value)
                else -> Base64.getEncoder().encodeToString(strValue.toByteArray())
            }
            Types.BIT, Types.BOOLEAN -> when (value) {
                is Boolean -> if (value) "1" else "0"
                is ByteArray -> { var r = 0L; for (b in value) r = (r shl 8) or (b.toLong() and 0xFF); r.toString() }
                else -> {
                    // Grid 可能展示为二进制字符串如 "10101010"，转为整数
                    if (strValue.length > 1 && strValue.all { it == '0' || it == '1' }) strValue.toLong(2).toString()
                    else strValue
                }
            }
            Types.DECIMAL, Types.NUMERIC -> {
                (value as? java.math.BigDecimal)?.toPlainString() ?: strValue
            }
            // DATE/TIME/TIMESTAMP 由 readRowData 存为 epoch millis，不经过这里
            else -> strValue
        }
    }

    private val nullSafeGson = com.google.gson.GsonBuilder().serializeNulls().create()

    private fun saveGridBackup(operationType: String, tableName: String, connInfo: String,
                               rows: List<Map<String, String?>>, originalSql: String,
                               primaryKeys: String? = null, columnTypes: Map<String, Int>? = null) {
        val partial = operationType == "INSERT"

        // 如果有类型信息，使用新格式
        val json = if (columnTypes != null && columnTypes.values.any { it != Types.VARCHAR }) {
            val wrapper = linkedMapOf<String, Any>("__types__" to columnTypes, "rows" to rows)
            nullSafeGson.toJson(wrapper)
        } else {
            nullSafeGson.toJson(rows)
        }

        val record = BackupRecord(
            createdAt = ZonedDateTime.now(),
            operationType = operationType,
            tableName = tableName,
            originalSql = originalSql,
            connectionInfo = connInfo,
            backupDataJson = json,
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
