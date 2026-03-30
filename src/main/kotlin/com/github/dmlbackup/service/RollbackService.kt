package com.github.dmlbackup.service

import com.github.dmlbackup.model.BackupRecord
import com.google.gson.Gson
import com.google.gson.JsonParser
import com.intellij.database.remote.jdbc.RemoteConnection
import com.intellij.openapi.diagnostic.Logger
import java.math.BigDecimal
import java.sql.Types
import java.util.Base64

/** 回滚语句 */
data class RollbackStatement(
    val sql: String,
    /** null = legacy 字面值 SQL，直接 executeUpdate；非 null = 参数化 SQL */
    val parameters: List<RollbackParam>?
)

data class RollbackParam(val value: String?, val jdbcType: Int)

/** 解析后的备份数据 */
private data class TypedBackupData(
    val types: Map<String, Int>?,
    val rows: List<Map<String, String?>>
)

/**
 * 回滚服务：从备份记录生成反向 SQL 并执行
 */
object RollbackService {

    private val log = Logger.getInstance(RollbackService::class.java)
    private val gson = Gson()

    private val BINARY_TYPES = setOf(Types.BLOB, Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY)

    // ==================== 公开 API ====================

    /**
     * 生成回滚计划
     */
    fun generateRollbackPlan(record: BackupRecord): List<RollbackStatement> {
        val data = this.parseBackupData(record.backupDataJson)
        if (data.rows.isEmpty()) {
            log.warn("No backup data to rollback for record id=${record.id}")
            return emptyList()
        }

        val primaryKeys = this.parsePrimaryKeys(record.primaryKeys)

        return when (record.operationType) {
            "DELETE" -> this.generateInsertStatements(record.tableName, data)
            "UPDATE" -> this.generateUpdateStatements(record.tableName, data, primaryKeys)
            "INSERT" -> this.generateDeleteStatements(record.tableName, data)
            else -> throw IllegalArgumentException("Unsupported operation type: ${record.operationType}")
        }
    }

    /**
     * 执行回滚计划，返回总影响行数
     */
    fun executeRollback(remoteConn: RemoteConnection, plan: List<RollbackStatement>): Int {
        var totalAffected = 0
        for (rstmt in plan) {
            val affected: Int
            if (rstmt.parameters == null) {
                val stmt = remoteConn.createStatement()
                log.info("Executing rollback SQL: ${rstmt.sql}")
                affected = stmt.executeUpdate(rstmt.sql)
                stmt.close()
            } else {
                val ps = remoteConn.prepareStatement(rstmt.sql)
                rstmt.parameters.forEachIndexed { idx, param -> this.bindParam(ps, idx + 1, param) }
                log.info("Executing typed rollback SQL: ${rstmt.sql} (${rstmt.parameters.size} params)")
                affected = ps.executeUpdate()
                ps.close()
            }
            log.info("Rollback SQL affected $affected row(s)")
            if (affected == 0) {
                throw IllegalStateException("Rollback aborted: statement affected 0 rows (target data may have changed). SQL: ${rstmt.sql.take(200)}")
            }
            totalAffected += affected
        }
        return totalAffected
    }

    // ==================== 备份数据解析 ====================

    private fun parseBackupData(json: String): TypedBackupData {
        val trimmed = json.trim()
        if (trimmed.isEmpty() || trimmed == "[]") return TypedBackupData(null, emptyList())

        // 旧格式: JSON array [...]
        if (trimmed.startsWith("[")) return TypedBackupData(null, this.parseJsonArray(trimmed))

        // 新格式: {"__types__":{...},"rows":[...]}
        val obj = JsonParser.parseString(trimmed).asJsonObject
        val typesObj = obj.getAsJsonObject("__types__")
        val types = typesObj.entrySet().associate { it.key to it.value.asInt }
        val rowsArray = obj.getAsJsonArray("rows")
        val rows = rowsArray.map { element ->
            val rowObj = element.asJsonObject
            rowObj.entrySet().associate { (key, value) ->
                key to if (value.isJsonNull) null else value.asString
            }
        }
        return TypedBackupData(types, rows)
    }

    private fun parseJsonArray(json: String): List<Map<String, String?>> {
        val array = JsonParser.parseString(json).asJsonArray
        return array.map { element ->
            val obj = element.asJsonObject
            obj.entrySet().associate { (key, value) ->
                key to if (value.isJsonNull) null else value.asString
            }
        }
    }

    private fun parsePrimaryKeys(primaryKeysJson: String?): List<String> {
        if (primaryKeysJson.isNullOrBlank()) return emptyList()
        return gson.fromJson(primaryKeysJson, Array<String>::class.java).toList()
    }

    // ==================== 语句生成 ====================

    /** DELETE 回滚 → INSERT */
    private fun generateInsertStatements(tableName: String, data: TypedBackupData): List<RollbackStatement> {
        return data.rows.map { row ->
            val columns = row.keys.joinToString(", ") { "`$it`" }
            if (data.types != null) {
                val placeholders = row.keys.joinToString(", ") { "?" }
                val sql = "INSERT INTO ${this.quoteTableName(tableName)} ($columns) VALUES ($placeholders)"
                val params = row.entries.map { RollbackParam(it.value, data.types[it.key] ?: Types.VARCHAR) }
                RollbackStatement(sql, params)
            } else {
                val values = row.values.joinToString(", ") { this.escapeValue(it) }
                RollbackStatement("INSERT INTO ${this.quoteTableName(tableName)} ($columns) VALUES ($values)", null)
            }
        }
    }

    /** UPDATE 回滚 → UPDATE（恢复旧值） */
    private fun generateUpdateStatements(tableName: String, data: TypedBackupData, primaryKeys: List<String>): List<RollbackStatement> {
        return data.rows.map { row ->
            if (data.types != null) {
                val setClauses = row.keys.joinToString(", ") { "`$it` = ?" }
                val (whereClause, whereParams) = this.buildTypedWhereClause(row, primaryKeys, data.types)
                val sql = "UPDATE ${this.quoteTableName(tableName)} SET $setClauses WHERE $whereClause LIMIT 1"
                val setParams = row.entries.map { RollbackParam(it.value, data.types[it.key] ?: Types.VARCHAR) }
                RollbackStatement(sql, setParams + whereParams)
            } else {
                val setClauses = row.entries.joinToString(", ") { "`${it.key}` = ${this.escapeValue(it.value)}" }
                val whereClause = this.buildLegacyWhereClause(row, primaryKeys)
                RollbackStatement("UPDATE ${this.quoteTableName(tableName)} SET $setClauses WHERE $whereClause LIMIT 1", null)
            }
        }
    }

    /** INSERT 回滚 → DELETE */
    private fun generateDeleteStatements(tableName: String, data: TypedBackupData): List<RollbackStatement> {
        return data.rows.map { row ->
            if (data.types != null) {
                // typed 格式（Grid INSERT）：用参数化 WHERE
                val whereClauses = row.entries.joinToString(" AND ") {
                    if (it.value == null) "`${it.key}` IS NULL" else "`${it.key}` = ?"
                }
                val params = row.entries.filter { it.value != null }.map { RollbackParam(it.value, data.types[it.key] ?: Types.VARCHAR) }
                RollbackStatement("DELETE FROM ${this.quoteTableName(tableName)} WHERE $whereClauses LIMIT 1", params)
            } else {
                // 旧格式（Console INSERT）：字面值拼接
                val whereClauses = row.entries.joinToString(" AND ") {
                    if (it.value == null) "`${it.key}` IS NULL"
                    else "`${it.key}` = ${this.escapeValue(it.value)}"
                }
                RollbackStatement("DELETE FROM ${this.quoteTableName(tableName)} WHERE $whereClauses LIMIT 1", null)
            }
        }
    }

    // ==================== WHERE 子句构建 ====================

    private fun buildTypedWhereClause(row: Map<String, String?>, primaryKeys: List<String>, types: Map<String, Int>): Pair<String, List<RollbackParam>> {
        val keys = if (primaryKeys.isNotEmpty()) primaryKeys
        else listOf(row.entries.find { it.key.equals("id", ignoreCase = true) }?.key ?: row.keys.first())

        val clause = keys.joinToString(" AND ") {
            if (row[it] == null) "`$it` IS NULL" else "`$it` = ?"
        }
        val params = keys.filter { row[it] != null }.map { RollbackParam(row[it], types[it] ?: Types.VARCHAR) }
        return Pair(clause, params)
    }

    private fun buildLegacyWhereClause(row: Map<String, String?>, primaryKeys: List<String>): String {
        return if (primaryKeys.isNotEmpty()) {
            primaryKeys.joinToString(" AND ") { pk ->
                if (row[pk] == null) "`$pk` IS NULL" else "`$pk` = ${this.escapeValue(row[pk])}"
            }
        } else {
            val pkEntry = row.entries.find { it.key.equals("id", ignoreCase = true) } ?: row.entries.first()
            if (pkEntry.value == null) "`${pkEntry.key}` IS NULL" else "`${pkEntry.key}` = ${this.escapeValue(pkEntry.value)}"
        }
    }

    // ==================== 参数绑定 ====================

    private fun bindParam(ps: Any, index: Int, param: RollbackParam) {
        // ps 是 RemotePreparedStatement，通过反射调用 set 方法
        if (param.value == null) {
            this.invokePs(ps, "setNull", index, param.jdbcType)
            return
        }
        when (param.jdbcType) {
            in BINARY_TYPES -> this.invokePs(ps, "setBytes", index, Base64.getDecoder().decode(param.value))
            Types.BIT, Types.BOOLEAN -> {
                val str = param.value
                // 二进制字符串如 "10101010"（Grid 旧记录兼容）
                val isBinaryStr = str.length > 1 && str.all { it == '0' || it == '1' }
                val longVal = if (isBinaryStr) str.toLongOrNull(2) else str.toLongOrNull()
                if (longVal != null) {
                    this.invokePs(ps, "setLong", index, longVal)
                } else {
                    // 兼容旧记录：base64 bytes → 转整数
                    try {
                        val bytes = Base64.getDecoder().decode(str)
                        var num = 0L; for (b in bytes) num = (num shl 8) or (b.toLong() and 0xFF)
                        this.invokePs(ps, "setLong", index, num)
                    } catch (_: Exception) { this.invokePs(ps, "setString", index, str) }
                }
            }
            Types.DECIMAL, Types.NUMERIC -> this.invokePs(ps, "setBigDecimal", index, BigDecimal(param.value))
            Types.TINYINT -> this.invokePs(ps, "setInt", index, param.value.toInt())
            Types.SMALLINT -> this.invokePs(ps, "setShort", index, param.value.toShort())
            Types.INTEGER -> this.invokePs(ps, "setInt", index, param.value.toInt())
            Types.BIGINT -> this.invokePs(ps, "setLong", index, param.value.toLong())
            Types.FLOAT, Types.REAL -> this.invokePs(ps, "setFloat", index, param.value.toFloat())
            Types.DOUBLE -> this.invokePs(ps, "setDouble", index, param.value.toDouble())
            Types.TIMESTAMP -> {
                // Grid 存 epoch millis，SQL Console 存格式化字符串
                val millis = param.value.toLongOrNull()
                if (millis != null) this.invokePs(ps, "setTimestamp", index, java.sql.Timestamp(millis))
                else this.invokePs(ps, "setString", index, param.value)
            }
            Types.DATE -> {
                val millis = param.value.toLongOrNull()
                if (millis != null) this.invokePs(ps, "setDate", index, java.sql.Date(millis))
                else this.invokePs(ps, "setString", index, param.value)
            }
            Types.TIME -> {
                val millis = param.value.toLongOrNull()
                if (millis != null) this.invokePs(ps, "setTime", index, java.sql.Time(millis))
                else this.invokePs(ps, "setString", index, param.value)
            }
            else -> this.invokePs(ps, "setString", index, param.value)
        }
    }

    /** 反射调用 PreparedStatement 的 set 方法 */
    private fun invokePs(ps: Any, methodName: String, index: Int, value: Any) {
        val method = ps.javaClass.methods.find { it.name == methodName && it.parameterCount == 2 }
            ?: throw IllegalStateException("Method $methodName not found on ${ps.javaClass.name}")
        method.invoke(ps, index, value)
    }

    // ==================== 工具方法 ====================

    private fun quoteTableName(tableName: String): String =
        tableName.split(".").joinToString(".") { "`$it`" }

    private fun escapeValue(value: String?): String =
        if (value == null) "NULL" else "'${value.replace("\\", "\\\\").replace("'", "\\'")}'"
}
