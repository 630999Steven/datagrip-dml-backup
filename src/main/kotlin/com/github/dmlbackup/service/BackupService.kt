package com.github.dmlbackup.service

import com.github.dmlbackup.model.BackupRecord
import com.github.dmlbackup.settings.DmlBackupSettings
import com.github.dmlbackup.storage.BackupStorage
import com.google.gson.Gson
import com.intellij.database.console.JdbcConsole
import com.intellij.database.dataSource.DatabaseConnectionManager
import com.intellij.database.remote.jdbc.RemoteConnection
import com.intellij.database.remote.jdbc.RemoteResultSet
import com.intellij.notification.NotificationGroupManager
import com.intellij.notification.NotificationType
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.progress.EmptyProgressIndicator
import com.intellij.openapi.progress.ProgressManager
import java.time.ZonedDateTime

object BackupService {

    private val log = Logger.getInstance(BackupService::class.java)
    private val gson = Gson()

    private val SYSTEM_SCHEMAS = setOf(
        "information_schema", "mysql", "performance_schema", "sys"
    )

    fun isMySql(console: JdbcConsole): Boolean {
        val url = console.dataSource?.url ?: return false
        return url.startsWith("jdbc:mysql://") || url.startsWith("jdbc:mariadb://")
    }

    /**
     * COUNT 预检：返回受影响行数，供 Listener 在 EDT 上判断是否需要弹窗确认
     */
    fun countAffectedRows(console: JdbcConsole, parsed: ParsedDml): Int {
        var count = 0
        ProgressManager.getInstance().runProcessWithProgressSynchronously({
            val connectionPoint = console.target ?: return@runProcessWithProgressSynchronously
            val guardedRef = DatabaseConnectionManager.getInstance()
                .build(console.project, connectionPoint)
                .createBlocking() ?: return@runProcessWithProgressSynchronously
            try {
                val remoteConn = guardedRef.get().remoteConnection
                val schema = this.resolveSchema(console, remoteConn, parsed.tableName)
                if (schema != null) {
                    val useStmt = remoteConn.createStatement()
                    useStmt.execute("USE `$schema`")
                    useStmt.close()
                }
                count = this.countRows(remoteConn, parsed)
            } finally {
                guardedRef.close()
            }
        }, "DML Backup: Checking affected rows...", true, console.project)
        return count
    }

    /**
     * 执行备份：DELETE/UPDATE 通过 SELECT 备份，INSERT 直接从 SQL 解析值
     */
    fun backup(console: JdbcConsole, parsed: ParsedDml, originalSql: String, timedOutFlag: (() -> Boolean)? = null) {
        val connInfo = "${console.dataSource?.name ?: "unknown"} (${console.dataSource?.url ?: "unknown"})"
        // 从 URL 中提取 schema，用于 INSERT 的 tableName 前缀
        val urlSchema = console.dataSource?.url?.let { Regex("://[^/]+/([^?;&]+)").find(it)?.groupValues?.get(1) }

        if (parsed.type == DmlType.INSERT) {
            // 只有表名不含 schema 前缀时才拼接 URL schema
            val fullTableName = if (urlSchema != null && !parsed.tableName.contains(".")) "$urlSchema.${parsed.tableName}" else parsed.tableName
            this.backupInsert(parsed, originalSql, connInfo, fullTableName, timedOutFlag)
            return
        }

        this.backupSelectBased(console, parsed, originalSql, connInfo, timedOutFlag)
    }

    /**
     * INSERT 备份：直接从 SQL 中解析列名和值，不需要查询数据库
     */
    private fun backupInsert(parsed: ParsedDml, originalSql: String, connInfo: String, fullTableName: String, timedOutFlag: (() -> Boolean)? = null) {
        val columns = parsed.insertColumns ?: run {
            log.warn("DML Backup: INSERT without column list, skipping backup")
            this.notifyUser("INSERT without column list detected, this statement was NOT backed up.", NotificationType.WARNING)
            return
        }
        val rows = parsed.insertValues ?: return

        val rowMaps = rows.map { values ->
            linkedMapOf<String, String?>().also { map ->
                columns.forEachIndexed { idx, name -> map[name] = values.getOrNull(idx) }
            }
        }

        if (timedOutFlag?.invoke() == true) {
            log.warn("DML Backup: timeout detected, discarding INSERT backup for ${parsed.tableName}")
            return
        }

        val record = BackupRecord(
            createdAt = ZonedDateTime.now(),
            operationType = parsed.type.name,
            tableName = fullTableName,
            originalSql = originalSql,
            connectionInfo = connInfo,
            backupDataJson = nullSafeGson.toJson(rowMaps),
            rowCount = rows.size,
            partialColumns = true  // SQL INSERT 只备份了指定列，回滚 DELETE 条件可能不完整
        )

        val id = BackupStorage.save(record)
        log.info("DML Backup: saved INSERT backup id=$id, rowCount=${rows.size} for table: ${parsed.tableName}")
        BackupStorage.trimRecords(DmlBackupSettings.getInstance().maxRecords)
    }

    /**
     * DELETE/UPDATE 备份：通过 SELECT FOR UPDATE 查询备份原始数据
     */
    private fun backupSelectBased(console: JdbcConsole, parsed: ParsedDml, originalSql: String, connInfo: String, timedOutFlag: (() -> Boolean)? = null) {
        ProgressManager.getInstance().runProcess({
            val connectionPoint = console.target
                ?: throw IllegalStateException("No connection point found for current console")

            val guardedRef = DatabaseConnectionManager.getInstance()
                .build(console.project, connectionPoint)
                .createBlocking()
                ?: throw IllegalStateException("Failed to create database connection")

            try {
                val remoteConn = guardedRef.get().remoteConnection

                val schema = this.resolveSchema(console, remoteConn, parsed.tableName)
                if (schema != null) {
                    log.info("DML Backup: USE `$schema`")
                    val useStmt = remoteConn.createStatement()
                    useStmt.execute("USE `$schema`")
                    useStmt.close()
                }

                // 检测主键
                val primaryKeys = this.detectPrimaryKeys(remoteConn, parsed.tableName)
                val pkJson = if (primaryKeys.isNotEmpty()) gson.toJson(primaryKeys) else null

                // SELECT FOR UPDATE 事务包裹
                remoteConn.setAutoCommit(false)
                try {
                    val forUpdateSql = parsed.backupSql + " FOR UPDATE"
                    val stmt = remoteConn.createStatement()
                    try {
                        log.debug("DML Backup: executing backup SQL: $forUpdateSql")
                        val rs = stmt.executeQuery(forUpdateSql)
                        val (json, rowCount) = this.resultSetToJson(rs)
                        rs.close()

                        if (timedOutFlag?.invoke() == true) {
                            log.warn("DML Backup: timeout detected, discarding backup for ${parsed.tableName}")
                            return@runProcess
                        }

                        // tableName 带上 schema 前缀，确保回滚时能定位到正确的库（已含 schema 的不重复拼接）
                        val fullTableName = if (schema != null && !parsed.tableName.contains(".")) "$schema.${parsed.tableName}" else parsed.tableName
                        val record = BackupRecord(
                            createdAt = ZonedDateTime.now(),
                            operationType = parsed.type.name,
                            tableName = fullTableName,
                            originalSql = originalSql,
                            connectionInfo = connInfo,
                            backupDataJson = json,
                            rowCount = rowCount,
                            primaryKeys = pkJson
                        )

                        val id = BackupStorage.save(record)
                        log.info("DML Backup: saved id=$id, rowCount=$rowCount for table: ${parsed.tableName}")
                        BackupStorage.trimRecords(DmlBackupSettings.getInstance().maxRecords)
                    } finally {
                        stmt.close()
                    }
                    remoteConn.commit()
                } catch (ex: Exception) {
                    remoteConn.rollback()
                    throw ex
                } finally {
                    remoteConn.setAutoCommit(true)
                }
            } finally {
                guardedRef.close()
            }
        }, EmptyProgressIndicator())
    }

    private fun countRows(remoteConn: RemoteConnection, parsed: ParsedDml): Int {
        val countSql = parsed.backupSql.replaceFirst(
            Regex("SELECT\\s+.*?\\s+FROM", setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL)), "SELECT COUNT(*) FROM"
        )
        val stmt = remoteConn.createStatement()
        try {
            val rs = stmt.executeQuery(countSql)
            rs.next()
            val count = rs.getInt(1)
            rs.close()
            return count
        } finally {
            stmt.close()
        }
    }

    private fun detectPrimaryKeys(remoteConn: RemoteConnection, tableName: String): List<String> {
        // 如果表名含 schema 前缀（如 db.table），只取最后一段
        val pureTableName = tableName.substringAfterLast(".")
        val stmt = remoteConn.createStatement()
        try {
            val rs = stmt.executeQuery("SHOW KEYS FROM `$pureTableName` WHERE Key_name = 'PRIMARY'")
            val keys = mutableListOf<Pair<Int, String>>()
            while (rs.next()) {
                keys.add(rs.getInt("Seq_in_index") to rs.getString("Column_name"))
            }
            rs.close()
            return keys.sortedBy { it.first }.map { it.second }
        } finally {
            stmt.close()
        }
    }

    /**
     * 推断当前使用的 database/schema：
     * 1. 如果表名已含 schema 前缀（如 other_db.table），直接使用
     * 2. 从 URL 解析
     * 3. 从 INFORMATION_SCHEMA 查询
     */
    private fun resolveSchema(console: JdbcConsole, remoteConn: RemoteConnection, tableName: String): String? {
        // 表名已含 schema 前缀（如 other_db.table），优先使用
        if (tableName.contains(".")) return tableName.substringBeforeLast(".")

        val url = console.dataSource?.url ?: ""
        val urlMatch = Regex("://[^/]+/([^?;&]+)").find(url)
        if (urlMatch != null) return urlMatch.groupValues[1]

        log.info("DML Backup: querying INFORMATION_SCHEMA for table: $tableName")
        val ps = remoteConn.prepareStatement(
            "SELECT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ? ORDER BY TABLE_SCHEMA"
        )
        try {
            ps.setString(1, tableName)
            val rs = ps.executeQuery()
            val schemas = mutableListOf<String>()
            while (rs.next()) { schemas.add(rs.getString(1)) }
            rs.close()

            if (schemas.isEmpty()) return null

            val userSchemas = schemas.filter { it.lowercase() !in SYSTEM_SCHEMAS }
            val result = userSchemas.firstOrNull() ?: schemas.first()
            log.info("DML Backup: resolved schema from INFORMATION_SCHEMA: $result")
            return result
        } finally {
            ps.close()
        }
    }

    private val nullSafeGson = com.google.gson.GsonBuilder().serializeNulls().create()

    private fun resultSetToJson(rs: RemoteResultSet): Pair<String, Int> {
        val meta = rs.metaData
        val columnCount = meta.columnCount
        val columnNames = (1..columnCount).map { meta.getColumnName(it) }

        val rows = mutableListOf<Map<String, String?>>()
        while (rs.next()) {
            val row = linkedMapOf<String, String?>()
            for (i in 1..columnCount) {
                val value = rs.getObject(i)
                row[columnNames[i - 1]] = value?.toString()
            }
            rows.add(row)
        }
        val result = nullSafeGson.toJson(rows)
        log.debug("DML Backup: resultSetToJson columns=$columnNames, rowCount=${rows.size}")
        return Pair(result, rows.size)
    }

    private fun notifyUser(content: String, type: NotificationType) {
        NotificationGroupManager.getInstance()
            .getNotificationGroup("DML Backup")
            .createNotification(content, type)
            .notify(null)
    }
}
