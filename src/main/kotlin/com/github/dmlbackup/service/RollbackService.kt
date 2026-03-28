package com.github.dmlbackup.service

import com.github.dmlbackup.model.BackupRecord
import com.google.gson.Gson
import com.google.gson.JsonParser
import com.intellij.openapi.diagnostic.Logger

/**
 * 回滚服务：从备份记录生成反向 SQL
 */
object RollbackService {

    private val log = Logger.getInstance(RollbackService::class.java)
    private val gson = Gson()

    /**
     * 生成回滚 SQL 列表
     */
    fun generateRollbackSqls(record: BackupRecord): List<String> {
        val rows = this.parseJsonArray(record.backupDataJson)
        if (rows.isEmpty()) {
            log.warn("No backup data to rollback for record id=${record.id}")
            return emptyList()
        }

        val primaryKeys = this.parsePrimaryKeys(record.primaryKeys)

        return when (record.operationType) {
            "DELETE" -> this.generateInsertSqls(record.tableName, rows)
            "UPDATE" -> this.generateUpdateSqls(record.tableName, rows, primaryKeys)
            "INSERT" -> this.generateDeleteSqls(record.tableName, rows)
            else -> throw IllegalArgumentException("Unsupported operation type: ${record.operationType}")
        }
    }

    private fun generateInsertSqls(tableName: String, rows: List<Map<String, String?>>): List<String> {
        return rows.map { row ->
            val columns = row.keys.joinToString(", ") { "`$it`" }
            val values = row.values.joinToString(", ") { this.escapeValue(it) }
            "INSERT INTO ${this.quoteTableName(tableName)} ($columns) VALUES ($values)"
        }
    }

    private fun generateUpdateSqls(
        tableName: String,
        rows: List<Map<String, String?>>,
        primaryKeys: List<String>
    ): List<String> {
        return rows.map { row ->
            val setClauses = row.entries.joinToString(", ") { "`${it.key}` = ${this.escapeValue(it.value)}" }
            val whereClause = if (primaryKeys.isNotEmpty()) {
                primaryKeys.joinToString(" AND ") { pk ->
                    "`$pk` = ${this.escapeValue(row[pk])}"
                }
            } else {
                // 旧数据兼容：用 id 列或第一列
                val pkEntry = row.entries.find { it.key.equals("id", ignoreCase = true) } ?: row.entries.first()
                "`${pkEntry.key}` = ${this.escapeValue(pkEntry.value)}"
            }
            "UPDATE ${this.quoteTableName(tableName)} SET $setClauses WHERE $whereClause LIMIT 1"
        }
    }

    private fun generateDeleteSqls(tableName: String, rows: List<Map<String, String?>>): List<String> {
        return rows.map { row ->
            val whereClauses = row.entries.joinToString(" AND ") {
                if (it.value == null) "`${it.key}` IS NULL"
                else "`${it.key}` = ${this.escapeValue(it.value)}"
            }
            "DELETE FROM ${this.quoteTableName(tableName)} WHERE $whereClauses LIMIT 1"
        }
    }

    /** 将 tableName（可能是 schema.table 格式）包裹反引号 */
    private fun quoteTableName(tableName: String): String {
        return tableName.split(".").joinToString(".") { "`$it`" }
    }

    private fun escapeValue(value: String?): String {
        return if (value == null) "NULL" else "'${value.replace("'", "''")}'"
    }

    private fun parseJsonArray(json: String): List<Map<String, String?>> {
        val trimmed = json.trim()
        if (trimmed == "[]" || trimmed.isEmpty()) return emptyList()

        val array = JsonParser.parseString(trimmed).asJsonArray
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
}
