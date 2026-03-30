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
    val insertValues: List<List<String?>>? = null,
    /** INSERT IGNORE / ON DUPLICATE KEY UPDATE 等不可靠回滚的变体 */
    val unsafeInsert: Boolean = false
)

enum class DmlType { DELETE, UPDATE, INSERT, OTHER }

object SqlParser {

    private const val TABLE_ID = """[`"\[\]]?\w+[`"\[\]]?(?:\.[`"\[\]]?\w+[`"\[\]]?)*"""

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
            // 单引号字符串字面量
            if (sql[i] == '\'') {
                sb.append(sql[i]); i++
                while (i < sql.length) {
                    if (sql[i] == '\'' && i + 1 < sql.length && sql[i + 1] == '\'') { sb.append("''"); i += 2 }
                    else if (sql[i] == '\\' && i + 1 < sql.length) { sb.append(sql[i]); sb.append(sql[i + 1]); i += 2 }
                    else if (sql[i] == '\'') { sb.append('\''); i++; break }
                    else { sb.append(sql[i]); i++ }
                }
            }
            // 双引号字符串字面量（MySQL 默认支持）
            else if (sql[i] == '"') {
                sb.append(sql[i]); i++
                while (i < sql.length) {
                    if (sql[i] == '"' && i + 1 < sql.length && sql[i + 1] == '"') { sb.append("\"\""); i += 2 }
                    else if (sql[i] == '\\' && i + 1 < sql.length) { sb.append(sql[i]); sb.append(sql[i + 1]); i += 2 }
                    else if (sql[i] == '"') { sb.append('"'); i++; break }
                    else { sb.append(sql[i]); i++ }
                }
            }
            // 块注释
            else if (i + 1 < sql.length && sql[i] == '/' && sql[i + 1] == '*') {
                i += 2
                while (i + 1 < sql.length && !(sql[i] == '*' && sql[i + 1] == '/')) i++
                if (i + 1 < sql.length) i += 2
                sb.append(' ')
            }
            // -- 行注释（MySQL 要求 -- 后跟空白或行尾）
            else if (i + 1 < sql.length && sql[i] == '-' && sql[i + 1] == '-'
                && (i + 2 >= sql.length || sql[i + 2].isWhitespace())) {
                i += 2
                while (i < sql.length && sql[i] != '\n') i++
                sb.append(' ')
            }
            // # 行注释（MySQL 特有）
            else if (sql[i] == '#') {
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
        var stringChar: Char? = null  // 当前字符串引号字符（' 或 "）
        var start = 0
        var i = 0
        while (i < sql.length) {
            if (stringChar != null) {
                if (sql[i] == stringChar && i + 1 < sql.length && sql[i + 1] == stringChar) i += 2
                else if (sql[i] == '\\' && i + 1 < sql.length) i += 2
                else if (sql[i] == stringChar) { stringChar = null; i++ }
                else i++
            } else {
                when (sql[i]) {
                    '\'', '"' -> { stringChar = sql[i]; i++ }
                    ';' -> { val stmt = sql.substring(start, i).trim(); if (stmt.isNotEmpty()) statements.add(stmt); start = i + 1; i++ }
                    else -> i++
                }
            }
        }
        val last = sql.substring(start).trim()
        if (last.isNotEmpty()) statements.add(last)
        return statements
    }

    /**
     * 在原始文本上按分号拆分语句，返回每段的 (startOffset, endOffset)，
     * 跳过字符串字面量和注释中的分号。偏移基于原始文本。
     */
    fun splitStatementsWithOffsets(sql: String): List<Pair<Int, Int>> {
        val ranges = mutableListOf<Pair<Int, Int>>()
        var stringChar: Char? = null
        var start = 0
        var i = 0
        while (i < sql.length) {
            if (stringChar != null) {
                if (sql[i] == stringChar && i + 1 < sql.length && sql[i + 1] == stringChar) i += 2
                else if (sql[i] == '\\' && i + 1 < sql.length) i += 2
                else if (sql[i] == stringChar) { stringChar = null; i++ }
                else i++
            } else {
                when {
                    sql[i] == '\'' || sql[i] == '"' -> { stringChar = sql[i]; i++ }
                    sql[i] == ';' -> {
                        if (sql.substring(start, i).isNotBlank()) ranges.add(start to i)
                        start = i + 1; i++
                    }
                    // 跳过块注释
                    i + 1 < sql.length && sql[i] == '/' && sql[i + 1] == '*' -> {
                        i += 2; while (i + 1 < sql.length && !(sql[i] == '*' && sql[i + 1] == '/')) i++; if (i + 1 < sql.length) i += 2
                    }
                    // 跳过 -- 行注释（MySQL 要求 -- 后跟空白）
                    i + 2 < sql.length && sql[i] == '-' && sql[i + 1] == '-' && sql[i + 2].isWhitespace() -> {
                        i += 2; while (i < sql.length && sql[i] != '\n') i++
                    }
                    // 跳过 # 行注释
                    sql[i] == '#' -> { while (i < sql.length && sql[i] != '\n') i++ }
                    else -> i++
                }
            }
        }
        if (sql.substring(start).isNotBlank()) ranges.add(start to sql.length)
        return ranges
    }

    /**
     * 解析 SQL，如果是 DELETE/UPDATE/INSERT 则返回 ParsedDml，否则返回 null
     */
    fun parse(sql: String): ParsedDml? {
        val cleaned = sql.trim().removeSuffix(";").trim()

        this.parseDelete(cleaned)?.let { return it }
        this.parseUpdate(cleaned)?.let { return it }
        this.parseInsert(cleaned)?.let { return it }
        return null
    }

    /** 检测是否像 DML 语句（以 DELETE/UPDATE/INSERT 开头）但 parse() 返回 null */
    fun looksLikeDml(sql: String): Boolean {
        val trimmed = sql.trim().uppercase()
        return trimmed.startsWith("DELETE") || trimmed.startsWith("UPDATE") || trimmed.startsWith("INSERT") || trimmed.startsWith("WITH")
    }

    private fun parseDelete(sql: String): ParsedDml? {
        // 先尝试 JOIN 模式
        DELETE_JOIN.matchEntire(sql)?.let { match ->
            val fromTable = this.cleanIdentifier(match.groupValues[1])
            val joinClause = match.groupValues[2].trim()
            val whereClause = match.groupValues[3].trim().ifEmpty { null }
            // DELETE t2 FROM t1 JOIN t2 ... → deleteTarget 是被删的表（DELETE 后面的标识符）
            val deleteTarget = sql.trim().replaceFirst(Regex("^DELETE\\s+", RegexOption.IGNORE_CASE), "")
                .split("\\s+".toRegex()).first().removeSuffix(".*").let { this.cleanIdentifier(it) }
            // tableName 应是被删的目标表，不是 FROM 后的表
            val tableName = if (deleteTarget != fromTable) deleteTarget else fromTable
            val selectRef = if (deleteTarget != fromTable) deleteTarget else fromTable
            val backupSql = "SELECT $selectRef.* FROM $fromTable $joinClause" +
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
            val parts = tableWithAlias.split("\\s+".toRegex())
            val tableName = this.cleanIdentifier(parts[0])
            // 提取别名：UPDATE table alias JOIN ... 或 UPDATE table AS alias JOIN ...
            val alias = when {
                parts.size >= 3 && parts[1].equals("AS", ignoreCase = true) -> this.cleanIdentifier(parts[2])
                parts.size >= 2 && !parts[1].equals("AS", ignoreCase = true) -> this.cleanIdentifier(parts[1])
                else -> tableName
            }
            val joinClause = match.groupValues[2].trim()
            val whereClause = match.groupValues[3].trim().ifEmpty { null }
            val backupSql = "SELECT $alias.* FROM $tableWithAlias $joinClause" +
                (whereClause?.let { " $it" } ?: "")
            return ParsedDml(DmlType.UPDATE, tableName, whereClause, backupSql)
        }

        // 简单 UPDATE：用词法扫描找顶层 WHERE（跳过子查询中的 WHERE）
        val match = UPDATE_SIMPLE.matchEntire(sql) ?: return null
        val tableName = this.cleanIdentifier(match.groupValues[1])
        val topLevelWhere = this.findTopLevelWhere(sql)
        val whereClause = topLevelWhere?.let { sql.substring(it).trim() }?.ifEmpty { null }
        val backupSql = "SELECT * FROM $tableName" + (whereClause?.let { " $it" } ?: "")
        return ParsedDml(DmlType.UPDATE, tableName, whereClause, backupSql)
    }

    /** 在 SQL 中找到顶层（括号深度=0）的最后一个 WHERE 关键字的起始位置 */
    private fun findTopLevelWhere(sql: String): Int? {
        var depth = 0
        var stringChar: Char? = null
        var lastWhereIdx: Int? = null
        var i = 0
        while (i < sql.length) {
            if (stringChar != null) {
                if (sql[i] == stringChar && i + 1 < sql.length && sql[i + 1] == stringChar) i += 2
                else if (sql[i] == '\\' && i + 1 < sql.length) i += 2
                else if (sql[i] == stringChar) { stringChar = null; i++ }
                else i++
            } else {
                when {
                    sql[i] == '\'' || sql[i] == '"' -> { stringChar = sql[i]; i++ }
                    sql[i] == '(' -> { depth++; i++ }
                    sql[i] == ')' -> { depth--; i++ }
                    depth == 0 && i + 5 <= sql.length && sql.regionMatches(i, "WHERE", 0, 5, ignoreCase = true)
                        && (i == 0 || !sql[i - 1].isLetterOrDigit()) && (i + 5 >= sql.length || !sql[i + 5].isLetterOrDigit()) -> {
                        lastWhereIdx = i; i += 5
                    }
                    else -> i++
                }
            }
        }
        return lastWhereIdx
    }

    private fun parseInsert(sql: String): ParsedDml? {
        val match = INSERT_PATTERN.matchEntire(sql) ?: return null
        val tableName = this.cleanIdentifier(match.groupValues[1])
        val columnsRaw = match.groupValues[2].trim()
        var valuesRaw = match.groupValues[3].trim()

        // 检测不可靠回滚的变体
        val isIgnore = sql.trimStart().matches(Regex("^INSERT\\s+IGNORE\\s+.*", OPTS))
        val hasOnDuplicate = sql.contains(Regex("ON\\s+DUPLICATE\\s+KEY\\s+UPDATE", OPTS))
        val unsafeInsert = isIgnore || hasOnDuplicate

        // 去掉 ON DUPLICATE KEY UPDATE ... 尾部，避免其括号被误解析为 VALUES 行
        if (hasOnDuplicate) {
            val onDupIdx = valuesRaw.indexOfFirst {
                valuesRaw.regionMatches(valuesRaw.indexOf(it), "ON ", 0, 3, ignoreCase = true)
            }
            val onDupMatch = Regex("\\)\\s*ON\\s+DUPLICATE\\s+KEY\\s+UPDATE", OPTS).find(valuesRaw)
            if (onDupMatch != null) {
                valuesRaw = valuesRaw.substring(0, onDupMatch.range.first + 1) // 保留最后一个 )
            }
        }

        val columns = if (columnsRaw.isNotEmpty()) {
            columnsRaw.split(",").map { this.cleanIdentifier(it.trim()) }
        } else null

        // 解析 VALUES 部分，支持多行 INSERT: VALUES (...), (...)
        val rows = this.parseValuesClause(valuesRaw)

        // 检查是否包含函数调用/表达式值（如 NOW(), UUID(), POINT(1,2)），这些值无法可靠回滚
        val hasExpression = rows.any { row -> row.any { v -> v != null && this.looksLikeExpression(v) } }

        return ParsedDml(DmlType.INSERT, tableName, null, "", columns, rows, unsafeInsert || hasExpression)
    }

    /**
     * 解析 VALUES (...), (...) 部分，返回每行的值列表
     */
    private fun parseValuesClause(valuesRaw: String): List<List<String?>> {
        val rows = mutableListOf<List<String?>>()
        var depth = 0
        var start = -1
        var stringChar: Char? = null
        var i = 0
        while (i < valuesRaw.length) {
            if (stringChar != null) {
                if (valuesRaw[i] == stringChar && i + 1 < valuesRaw.length && valuesRaw[i + 1] == stringChar) i += 2
                else if (valuesRaw[i] == '\\' && i + 1 < valuesRaw.length) i += 2
                else if (valuesRaw[i] == stringChar) { stringChar = null; i++ }
                else i++
            } else {
                when (valuesRaw[i]) {
                    '\'', '"' -> { stringChar = valuesRaw[i]; i++ }
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
     * 解析单行值列表，如: 'abc', 123, NULL, POINT(1,2)
     * 支持嵌套括号（函数调用如 POINT(1,2)、NOW()、JSON_OBJECT('a',1)）
     */
    private fun parseSingleRow(row: String): List<String?> {
        val values = mutableListOf<String?>()
        var i = 0
        val s = row.trim()
        while (i < s.length) {
            when {
                s[i] == '\'' || s[i] == '"' -> {
                    val quote = s[i]
                    val sb = StringBuilder()
                    i++
                    while (i < s.length) {
                        if (s[i] == quote && i + 1 < s.length && s[i + 1] == quote) {
                            sb.append(quote); i += 2
                        } else if (s[i] == '\\' && i + 1 < s.length) {
                            sb.append('\\'); sb.append(s[i + 1]); i += 2
                        } else if (s[i] == quote) {
                            i++; break
                        } else {
                            sb.append(s[i]); i++
                        }
                    }
                    values.add(sb.toString())
                }
                s.regionMatches(i, "NULL", 0, 4, ignoreCase = true) &&
                    (i + 4 >= s.length || s[i + 4] == ',' || s[i + 4] == ' ') -> {
                    values.add(null); i += 4
                }
                s[i] == ',' || s[i] == ' ' -> i++
                else -> {
                    // 数字、函数调用（如 POINT(1,2)）等非引号值，跟踪括号深度
                    val sb = StringBuilder()
                    var depth = 0
                    while (i < s.length) {
                        when {
                            s[i] == '\'' || s[i] == '"' -> {
                                // 函数参数中的字符串
                                val q = s[i]
                                sb.append(s[i]); i++
                                while (i < s.length) {
                                    if (s[i] == q && i + 1 < s.length && s[i + 1] == q) {
                                        sb.append(q); sb.append(q); i += 2
                                    } else if (s[i] == '\\' && i + 1 < s.length) {
                                        sb.append(s[i]); sb.append(s[i + 1]); i += 2
                                    } else if (s[i] == q) {
                                        sb.append(q); i++; break
                                    } else {
                                        sb.append(s[i]); i++
                                    }
                                }
                            }
                            s[i] == '(' -> { depth++; sb.append(s[i]); i++ }
                            s[i] == ')' -> { depth--; sb.append(s[i]); i++ }
                            s[i] == ',' && depth == 0 -> break
                            else -> { sb.append(s[i]); i++ }
                        }
                    }
                    values.add(sb.toString().trim())
                }
            }
        }
        return values
    }

    private val EXPRESSION_KEYWORDS = setOf(
        "DEFAULT", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME",
        "LOCALTIMESTAMP", "LOCALTIME", "NOW", "UUID", "RAND"
    )

    /** 检测值是否为函数调用或 MySQL 非确定性表达式（parseSingleRow 已去掉引号，纯文本字面量不会误判） */
    private fun looksLikeExpression(value: String): Boolean {
        val trimmed = value.trim()
        if (trimmed.contains('(')) return true
        return trimmed.uppercase() in EXPRESSION_KEYWORDS
    }

    private fun cleanIdentifier(identifier: String): String {
        return identifier.replace(Regex("[`\"\\[\\]]"), "")
    }
}
