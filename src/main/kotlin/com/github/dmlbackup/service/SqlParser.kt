package com.github.dmlbackup.service

/**
 * SQL 解析结果
 */
data class ParsedDml(
    val type: DmlType,
    val tableName: String,
    val whereClause: String?,
    val backupSql: String,
    val insertColumns: List<String>? = null,
    val insertValues: List<List<String?>>? = null
)

enum class DmlType { DELETE, UPDATE, INSERT, OTHER }

object SqlParser {

    companion object {
        private const val TABLE_ID = """[`"\[\]]?\w+[`"\[\]]?(?:\.[`"\[\]]?\w+[`"\[\]]?)*"""
    }

    private val OPTS = setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL)

    // DELETE FROM table [WHERE ...]
    private val DELETE_SIMPLE = Regex(
        """^\s*DELETE\s+FROM\s+($TABLE_ID)\s*(WHERE\s+.+)?$""", OPTS
    )

    // DELETE table[.*] FROM table_refs [WHERE ...] (multi-table)
    private val DELETE_JOIN = Regex(
        """^\s*DELETE\s+\w+(?:\.\*)?\s+FROM\s+($TABLE_ID)\s+(.*?)\s*(WHERE\s+.+)?$""", OPTS
    )

    // UPDATE table [AS alias] SET ... [WHERE ...]
    private val UPDATE_SIMPLE = Regex(
        """^\s*UPDATE\s+($TABLE_ID)(?:\s+(?:AS\s+)?\w+)?\s+SET\s+.+?(?:\s+(WHERE\s+.+))?$""", OPTS
    )

    // UPDATE table [AS alias] JOIN ... SET ... [WHERE ...]
    private val UPDATE_JOIN = Regex(
        """^\s*UPDATE\s+($TABLE_ID(?:\s+(?:AS\s+)?\w+)?)\s+((?:(?:INNER|LEFT|RIGHT|CROSS)\s+)?JOIN\s+.+?)\s+SET\s+.+?(?:\s+(WHERE\s+.+))?$""", OPTS
    )

    // INSERT [IGNORE] INTO table [(columns)] VALUES (...)
    private val INSERT_PATTERN = Regex(
        """^\s*INSERT\s+(?:IGNORE\s+)?INTO\s+($TABLE_ID)\s*(?:\(([^)]+)\))?\s*VALUES\s*(.+)$""", OPTS
    )

    /**
     * 移除 SQL 中的块注释和行注释，保留字符串字面量中的内容
     */
    fun removeComments(sql: String): String {
        val sb = StringBuilder(sql.length)
        var i = 0
        while (i < sql.length) {
            // 字符串字面量
            if (sql[i] == '\'') {
                sb.append(sql[i])
                i++
                while (i < sql.length) {
                    if (sql[i] == '\'' && i + 1 < sql.length && sql[i + 1] == '\'') {
                        sb.append("''"); i += 2
                    } else if (sql[i] == '\\' && i + 1 < sql.length) {
                        sb.append(sql[i]); sb.append(sql[i + 1]); i += 2
                    } else if (sql[i] == '\'') {
                        sb.append('\''); i++; break
                    } else {
                        sb.append(sql[i]); i++
                    }
                }
            }
            // 块注释
            else if (i + 1 < sql.length && sql[i] == '/' && sql[i + 1] == '*') {
                i += 2
                while (i + 1 < sql.length && !(sql[i] == '*' && sql[i + 1] == '/')) i++
                if (i + 1 < sql.length) i += 2
                sb.append(' ')
            }
            // 行注释
            else if (i + 1 < sql.length && sql[i] == '-' && sql[i + 1] == '-') {
                i += 2
                while (i < sql.length && sql[i] != '\n') i++
                sb.append(' ')
            }
            else {
                sb.append(sql[i]); i++
            }
        }
        return sb.toString()
    }

    /**
     * 按分号拆分 SQL 语句，排除字符串字面量中的分号
     */
    fun splitStatements(sql: String): List<String> {
        val statements = mutableListOf<String>()
        var inString = false
        var start = 0
        var i = 0
        while (i < sql.length) {
            if (inString) {
                if (sql[i] == '\'' && i + 1 < sql.length && sql[i + 1] == '\'') {
                    i += 2
                } else if (sql[i] == '\\' && i + 1 < sql.length) {
                    i += 2
                } else if (sql[i] == '\'') {
                    inString = false; i++
                } else {
                    i++
                }
            } else {
                if (sql[i] == '\'') {
                    inString = true; i++
                } else if (sql[i] == ';') {
                    val stmt = sql.substring(start, i).trim()
                    if (stmt.isNotEmpty()) statements.add(stmt)
                    start = i + 1; i++
                } else {
                    i++
                }
            }
        }
        val last = sql.substring(start).trim()
        if (last.isNotEmpty()) statements.add(last)
        return statements
    }

    /**
     * 解析 SQL，如果是 DELETE/UPDATE/INSERT 则返回 ParsedDml，否则返回 null
     */
    fun parse(sql: String): ParsedDml? {
        val cleaned = this.removeComments(sql.trim()).removeSuffix(";").trim()

        this.parseDelete(cleaned)?.let { return it }
        this.parseUpdate(cleaned)?.let { return it }
        this.parseInsert(cleaned)?.let { return it }
        return null
    }

    private fun parseDelete(sql: String): ParsedDml? {
        // 先尝试 JOIN 模式
        DELETE_JOIN.matchEntire(sql)?.let { match ->
            val tableName = this.cleanIdentifier(match.groupValues[1])
            val joinClause = match.groupValues[2].trim()
            val whereClause = match.groupValues[3].trim().ifEmpty { null }
            val backupSql = "SELECT $tableName.* FROM $tableName $joinClause" +
                (whereClause?.let { " $it" } ?: "")
            return ParsedDml(DmlType.DELETE, tableName, whereClause, backupSql)
        }

        // 简单 DELETE
        val match = DELETE_SIMPLE.matchEntire(sql) ?: return null
        val tableName = this.cleanIdentifier(match.groupValues[1])
        val whereClause = match.groupValues[2].trim().ifEmpty { null }
        val backupSql = "SELECT * FROM $tableName" + (whereClause?.let { " $it" } ?: "")
        return ParsedDml(DmlType.DELETE, tableName, whereClause, backupSql)
    }

    private fun parseUpdate(sql: String): ParsedDml? {
        // 先尝试 JOIN 模式
        UPDATE_JOIN.matchEntire(sql)?.let { match ->
            val tableWithAlias = match.groupValues[1].trim()
            val tableName = this.cleanIdentifier(tableWithAlias.split("\\s+".toRegex())[0])
            val joinClause = match.groupValues[2].trim()
            val whereClause = match.groupValues[3].trim().ifEmpty { null }
            val backupSql = "SELECT $tableName.* FROM $tableWithAlias $joinClause" +
                (whereClause?.let { " $it" } ?: "")
            return ParsedDml(DmlType.UPDATE, tableName, whereClause, backupSql)
        }

        // 简单 UPDATE
        val match = UPDATE_SIMPLE.matchEntire(sql) ?: return null
        val tableName = this.cleanIdentifier(match.groupValues[1])
        val whereClause = match.groupValues[2].trim().ifEmpty { null }
        val backupSql = "SELECT * FROM $tableName" + (whereClause?.let { " $it" } ?: "")
        return ParsedDml(DmlType.UPDATE, tableName, whereClause, backupSql)
    }

    private fun parseInsert(sql: String): ParsedDml? {
        val match = INSERT_PATTERN.matchEntire(sql) ?: return null
        val tableName = this.cleanIdentifier(match.groupValues[1])
        val columnsRaw = match.groupValues[2].trim()
        val valuesRaw = match.groupValues[3].trim()

        val columns = if (columnsRaw.isNotEmpty()) {
            columnsRaw.split(",").map { this.cleanIdentifier(it.trim()) }
        } else null

        // 解析 VALUES 部分，支持多行 INSERT: VALUES (...), (...)
        val rows = this.parseValuesClause(valuesRaw)

        return ParsedDml(DmlType.INSERT, tableName, null, "", columns, rows)
    }

    /**
     * 解析 VALUES (...), (...) 部分，返回每行的值列表
     */
    private fun parseValuesClause(valuesRaw: String): List<List<String?>> {
        val rows = mutableListOf<List<String?>>()
        // 按顶层括号分割每一行，跳过字符串字面量中的括号
        var depth = 0
        var start = -1
        var inString = false
        var i = 0
        while (i < valuesRaw.length) {
            if (inString) {
                if (valuesRaw[i] == '\'' && i + 1 < valuesRaw.length && valuesRaw[i + 1] == '\'') {
                    i += 2
                } else if (valuesRaw[i] == '\\' && i + 1 < valuesRaw.length) {
                    i += 2
                } else if (valuesRaw[i] == '\'') {
                    inString = false; i++
                } else {
                    i++
                }
            } else {
                when (valuesRaw[i]) {
                    '\'' -> { inString = true; i++ }
                    '(' -> { if (depth == 0) start = i + 1; depth++; i++ }
                    ')' -> {
                        depth--
                        if (depth == 0 && start >= 0) {
                            rows.add(this.parseSingleRow(valuesRaw.substring(start, i)))
                            start = -1
                        }
                        i++
                    }
                    else -> i++
                }
            }
        }
        return rows
    }

    /**
     * 解析单行值列表，如: 'abc', 123, NULL
     */
    private fun parseSingleRow(row: String): List<String?> {
        val values = mutableListOf<String?>()
        var i = 0
        val s = row.trim()
        while (i < s.length) {
            when {
                s[i] == '\'' -> {
                    // 字符串值，处理转义的单引号
                    val sb = StringBuilder()
                    i++ // skip opening '
                    while (i < s.length) {
                        if (s[i] == '\'' && i + 1 < s.length && s[i + 1] == '\'') {
                            sb.append('\''); i += 2
                        } else if (s[i] == '\\' && i + 1 < s.length) {
                            sb.append(s[i + 1]); i += 2
                        } else if (s[i] == '\'') {
                            i++; break
                        } else {
                            sb.append(s[i]); i++
                        }
                    }
                    values.add(sb.toString())
                }
                s.regionMatches(i, "NULL", 0, 4, ignoreCase = true) -> {
                    values.add(null); i += 4
                }
                s[i] == ',' || s[i] == ' ' -> i++
                else -> {
                    // 数字或其他非引号值
                    val end = s.indexOf(',', i).let { if (it == -1) s.length else it }
                    values.add(s.substring(i, end).trim())
                    i = end
                }
            }
        }
        return values
    }

    private fun cleanIdentifier(identifier: String): String {
        return identifier.replace(Regex("[`\"\\[\\]]"), "")
    }
}
