# Risk Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 11 identified risks in the DML Backup DataGrip plugin covering SQL parsing, rollback accuracy, performance, concurrency, and robustness.

**Architecture:** Enhance SqlParser with comment removal, JOIN/alias support, and multi-statement splitting. Replace custom JSON with Gson. Add SELECT FOR UPDATE for backup consistency, COUNT(*) precheck for large tables, real primary key detection, transaction-wrapped rollback, timeout notifications, and MySQL-only guard.

**Tech Stack:** Kotlin, IntelliJ Platform SDK, Gson (bundled in IntelliJ), SQLite JDBC

---

### Task 1: SqlParser Enhancement

**Files:**
- Modify: `src/main/kotlin/com/github/dmlbackup/service/SqlParser.kt`

- [ ] **Step 1: Add comment removal and statement splitting utilities**

Add these two utility methods to `SqlParser`:

```kotlin
/**
 * 移除 SQL 中的注释（块注释和行注释），保留字符串内的内容
 */
fun removeComments(sql: String): String {
    val sb = StringBuilder()
    var i = 0
    while (i < sql.length) {
        when {
            sql[i] == '\'' -> {
                // 字符串字面量，跳过直到闭合引号
                sb.append(sql[i++])
                while (i < sql.length) {
                    if (sql[i] == '\'' && i + 1 < sql.length && sql[i + 1] == '\'') {
                        sb.append("''"); i += 2
                    } else if (sql[i] == '\\' && i + 1 < sql.length) {
                        sb.append(sql[i]).append(sql[i + 1]); i += 2
                    } else if (sql[i] == '\'') {
                        sb.append(sql[i++]); break
                    } else {
                        sb.append(sql[i++])
                    }
                }
            }
            sql[i] == '/' && i + 1 < sql.length && sql[i + 1] == '*' -> {
                i += 2
                while (i + 1 < sql.length && !(sql[i] == '*' && sql[i + 1] == '/')) i++
                if (i + 1 < sql.length) i += 2
                sb.append(' ')
            }
            sql[i] == '-' && i + 1 < sql.length && sql[i + 1] == '-' -> {
                while (i < sql.length && sql[i] != '\n') i++
            }
            else -> sb.append(sql[i++])
        }
    }
    return sb.toString()
}

/**
 * 按分号拆分多条 SQL（排除字符串内的分号）
 */
fun splitStatements(sql: String): List<String> {
    val statements = mutableListOf<String>()
    val current = StringBuilder()
    var inString = false
    var i = 0
    while (i < sql.length) {
        when {
            sql[i] == '\'' && !inString -> { inString = true; current.append(sql[i++]) }
            sql[i] == '\'' && inString -> {
                current.append(sql[i])
                if (i + 1 < sql.length && sql[i + 1] == '\'') {
                    current.append(sql[++i])
                } else {
                    inString = false
                }
                i++
            }
            sql[i] == ';' && !inString -> {
                val stmt = current.toString().trim()
                if (stmt.isNotEmpty()) statements.add(stmt)
                current.clear()
                i++
            }
            else -> { current.append(sql[i++]) }
        }
    }
    val last = current.toString().trim()
    if (last.isNotEmpty()) statements.add(last)
    return statements
}
```

- [ ] **Step 2: Replace DML regex patterns with enhanced versions**

Replace the three existing pattern fields and parse methods with:

```kotlin
private val OPTS = setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL)

// DELETE FROM table [WHERE ...]
private val DELETE_SIMPLE = Regex(
    """^\s*DELETE\s+FROM\s+($TABLE_ID)\s*(WHERE\s+.+)?$""", OPTS
)

// DELETE table[.*] FROM table_refs [WHERE ...] (multi-table delete with JOIN)
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

private val INSERT_PATTERN = Regex(
    """^\s*INSERT\s+(?:IGNORE\s+)?INTO\s+($TABLE_ID)\s*(?:\(([^)]+)\))?\s*VALUES\s*(.+)$""", OPTS
)

companion object {
    private const val TABLE_ID = """[`"\[\]]?\w+[`"\[\]]?(?:\.[`"\[\]]?\w+[`"\[\]]?)*"""
}
```

And update parse methods:

```kotlin
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
    // 简单模式
    DELETE_SIMPLE.matchEntire(sql)?.let { match ->
        val tableName = this.cleanIdentifier(match.groupValues[1])
        val whereClause = match.groupValues[2].trim().ifEmpty { null }
        val backupSql = "SELECT * FROM $tableName" + (whereClause?.let { " $it" } ?: "")
        return ParsedDml(DmlType.DELETE, tableName, whereClause, backupSql)
    }
    return null
}

private fun parseUpdate(sql: String): ParsedDml? {
    // 先尝试 JOIN 模式
    UPDATE_JOIN.matchEntire(sql)?.let { match ->
        val tableWithAlias = match.groupValues[1].trim()
        val tableName = this.cleanIdentifier(tableWithAlias.split(Regex("\\s+"))[0])
        val joinClause = match.groupValues[2].trim()
        val whereClause = match.groupValues[3].trim().ifEmpty { null }
        val backupSql = "SELECT $tableName.* FROM $tableWithAlias $joinClause" +
            (whereClause?.let { " $it" } ?: "")
        return ParsedDml(DmlType.UPDATE, tableName, whereClause, backupSql)
    }
    // 简单模式
    UPDATE_SIMPLE.matchEntire(sql)?.let { match ->
        val tableName = this.cleanIdentifier(match.groupValues[1])
        val whereClause = match.groupValues[2].trim().ifEmpty { null }
        val backupSql = "SELECT * FROM $tableName" + (whereClause?.let { " $it" } ?: "")
        return ParsedDml(DmlType.UPDATE, tableName, whereClause, backupSql)
    }
    return null
}
```

`parseInsert` stays the same except use the new `INSERT_PATTERN`.

- [ ] **Step 3: Compile and verify**

Run: `./gradlew compileKotlin`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/com/github/dmlbackup/service/SqlParser.kt
git commit -m "feat: enhance SqlParser with comment removal, JOIN/alias support, multi-statement split"
```

---

### Task 2: BackupRecord + BackupStorage Schema Changes

**Files:**
- Modify: `src/main/kotlin/com/github/dmlbackup/model/BackupRecord.kt`
- Modify: `src/main/kotlin/com/github/dmlbackup/storage/BackupStorage.kt`

- [ ] **Step 1: Add new fields to BackupRecord**

Add `primaryKeys` and `partialColumns` fields:

```kotlin
data class BackupRecord(
    val id: Long = 0,
    val createdAt: ZonedDateTime = ZonedDateTime.now(),
    val operationType: String,
    val tableName: String,
    val originalSql: String,
    val connectionInfo: String,
    val backupDataJson: String,
    val rowCount: Int,
    val status: String = "PENDING",
    /** 主键列名，JSON 数组格式如 ["id"] 或 ["order_id","product_id"]，可为 null（旧数据兼容） */
    val primaryKeys: String? = null,
    /** INSERT 时是否只指定了部分列 */
    val partialColumns: Boolean = false
)
```

- [ ] **Step 2: Update BackupStorage - WAL mode, schema migration, synchronized writes, new columns**

In `BackupStorage` init block, add WAL mode and column migration:

```kotlin
init {
    Files.createDirectories(DB_PATH.parent)
    this.getConnection().use { conn ->
        conn.createStatement().execute("PRAGMA journal_mode=WAL")
        conn.createStatement().execute(
            """
            CREATE TABLE IF NOT EXISTS backup_record (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                operation_type TEXT NOT NULL,
                table_name TEXT NOT NULL,
                original_sql TEXT NOT NULL,
                connection_info TEXT NOT NULL,
                backup_data_json TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                primary_keys TEXT,
                partial_columns INTEGER DEFAULT 0
            )
            """.trimIndent()
        )
        // 兼容旧版本升级：检测并添加缺失的列
        this.migrateIfNeeded(conn)
    }
}

private fun migrateIfNeeded(conn: Connection) {
    val meta = conn.metaData
    val columns = mutableSetOf<String>()
    val rs = meta.getColumns(null, null, "backup_record", null)
    while (rs.next()) columns.add(rs.getString("COLUMN_NAME").lowercase())
    rs.close()
    if ("primary_keys" !in columns) {
        conn.createStatement().execute("ALTER TABLE backup_record ADD COLUMN primary_keys TEXT")
    }
    if ("partial_columns" !in columns) {
        conn.createStatement().execute("ALTER TABLE backup_record ADD COLUMN partial_columns INTEGER DEFAULT 0")
    }
}
```

Update `save()` to include new columns, add `@Synchronized`:

```kotlin
@Synchronized
fun save(record: BackupRecord): Long {
    this.getConnection().use { conn ->
        val ps = conn.prepareStatement(
            """
            INSERT INTO backup_record (created_at, operation_type, table_name, original_sql,
                connection_info, backup_data_json, row_count, status, primary_keys, partial_columns)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent()
        )
        ps.setString(1, record.createdAt.format(TIME_FMT))
        ps.setString(2, record.operationType)
        ps.setString(3, record.tableName)
        ps.setString(4, record.originalSql)
        ps.setString(5, record.connectionInfo)
        ps.setString(6, record.backupDataJson)
        ps.setInt(7, record.rowCount)
        ps.setString(8, record.status)
        ps.setString(9, record.primaryKeys)
        ps.setInt(10, if (record.partialColumns) 1 else 0)
        ps.executeUpdate()

        val rs = conn.createStatement().executeQuery("SELECT last_insert_rowid()")
        rs.next()
        return rs.getLong(1)
    }
}
```

Add `@Synchronized` to `updateStatus` and `trimRecords`. Update `mapRow`:

```kotlin
private fun mapRow(rs: java.sql.ResultSet): BackupRecord {
    return BackupRecord(
        id = rs.getLong("id"),
        createdAt = ZonedDateTime.parse(rs.getString("created_at"), TIME_FMT),
        operationType = rs.getString("operation_type"),
        tableName = rs.getString("table_name"),
        originalSql = rs.getString("original_sql"),
        connectionInfo = rs.getString("connection_info"),
        backupDataJson = rs.getString("backup_data_json"),
        rowCount = rs.getInt("row_count"),
        status = rs.getString("status"),
        primaryKeys = rs.getString("primary_keys"),
        partialColumns = rs.getInt("partial_columns") == 1
    )
}
```

- [ ] **Step 3: Compile and verify**

Run: `./gradlew compileKotlin`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/com/github/dmlbackup/model/BackupRecord.kt \
       src/main/kotlin/com/github/dmlbackup/storage/BackupStorage.kt
git commit -m "feat: add primaryKeys/partialColumns to BackupRecord, WAL mode, schema migration"
```

---

### Task 3: DmlBackupSettings + DmlBackupConfigurable

**Files:**
- Modify: `src/main/kotlin/com/github/dmlbackup/settings/DmlBackupSettings.kt`
- Modify: `src/main/kotlin/com/github/dmlbackup/settings/DmlBackupConfigurable.kt`

- [ ] **Step 1: Add maxBackupRows to settings**

In `DmlBackupSettings.State`, add:

```kotlin
data class State(
    var enabled: Boolean = true,
    var maxRecords: Int = 0,
    /** 单次备份最大行数，超过则跳过备份，0 表示不限制 */
    var maxBackupRows: Int = 10000
)
```

Add property accessor:

```kotlin
var maxBackupRows: Int
    get() = myState.maxBackupRows
    set(value) { myState.maxBackupRows = value }
```

- [ ] **Step 2: Add maxBackupRows spinner to config UI**

In `DmlBackupConfigurable`, add a new field and UI row:

```kotlin
private var maxBackupRowsField: JTextField? = null
```

In `createComponent()`, after the maxRecords row, add:

```kotlin
val maxRowsRow = JPanel(FlowLayout(FlowLayout.LEFT))
maxRowsRow.add(JLabel("Max backup rows per statement (0 = unlimited):"))
maxBackupRowsField = JTextField(settings.maxBackupRows.toString(), 6)
maxRowsRow.add(maxBackupRowsField)
panel!!.add(maxRowsRow)
```

Update `isModified()`:

```kotlin
override fun isModified(): Boolean {
    val settings = DmlBackupSettings.getInstance()
    return enabledCheckBox?.isSelected != settings.enabled ||
        maxRecordsField?.text?.toIntOrNull() != settings.maxRecords ||
        maxBackupRowsField?.text?.toIntOrNull() != settings.maxBackupRows
}
```

Update `apply()` and `reset()` similarly to include `maxBackupRows`.

- [ ] **Step 3: Compile and verify**

Run: `./gradlew compileKotlin`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/com/github/dmlbackup/settings/DmlBackupSettings.kt \
       src/main/kotlin/com/github/dmlbackup/settings/DmlBackupConfigurable.kt
git commit -m "feat: add maxBackupRows setting with config UI"
```

---

### Task 4: BackupService Overhaul

**Files:**
- Modify: `src/main/kotlin/com/github/dmlbackup/service/BackupService.kt`

This is the largest change. Adds: MySQL check, COUNT precheck, SELECT FOR UPDATE, primary key detection, Gson serialization, partial columns detection.

- [ ] **Step 1: Add MySQL check utility and Gson import**

Add at the top of `BackupService`:

```kotlin
import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonNull
import com.google.gson.JsonObject
```

Add utility method:

```kotlin
fun isMySql(console: JdbcConsole): Boolean {
    val url = console.dataSource?.url ?: return false
    return url.startsWith("jdbc:mysql://") || url.startsWith("jdbc:mariadb://")
}
```

- [ ] **Step 2: Add COUNT precheck and primary key detection methods**

```kotlin
private fun countRows(remoteConn: RemoteConnection, parsed: ParsedDml): Int {
    val countSql = parsed.backupSql.replaceFirst(
        Regex("SELECT\\s+.*?\\s+FROM", RegexOption.IGNORE_CASE), "SELECT COUNT(*) FROM"
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
    val stmt = remoteConn.createStatement()
    try {
        val rs = stmt.executeQuery("SHOW KEYS FROM `$tableName` WHERE Key_name = 'PRIMARY'")
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
```

- [ ] **Step 3: Rewrite backupSelectBased with FOR UPDATE, COUNT check, PK detection, Gson**

Replace the entire `backupSelectBased` method:

```kotlin
private fun backupSelectBased(console: JdbcConsole, parsed: ParsedDml, originalSql: String, connInfo: String) {
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
                val useStmt = remoteConn.createStatement()
                useStmt.execute("USE `$schema`")
                useStmt.close()
            }

            // COUNT 预检
            val maxRows = DmlBackupSettings.getInstance().maxBackupRows
            if (maxRows > 0) {
                val count = this.countRows(remoteConn, parsed)
                if (count > maxRows) {
                    log.warn("DML Backup: row count $count exceeds limit $maxRows, skip backup for ${parsed.tableName}")
                    NotificationGroupManager.getInstance()
                        .getNotificationGroup("DML Backup")
                        .createNotification(
                            "DML Backup skipped: affected rows ($count) exceed limit ($maxRows)",
                            NotificationType.WARNING
                        )
                        .notify(null)
                    return@runProcess
                }
            }

            // 检测主键
            val primaryKeys = this.detectPrimaryKeys(remoteConn, parsed.tableName)
            val pkJson = if (primaryKeys.isNotEmpty()) {
                Gson().toJson(primaryKeys)
            } else null

            // SELECT FOR UPDATE 事务包裹
            remoteConn.setAutoCommit(false)
            try {
                val forUpdateSql = parsed.backupSql + " FOR UPDATE"
                val stmt = remoteConn.createStatement()
                try {
                    log.info("DML Backup: executing backup SQL: $forUpdateSql")
                    val rs = stmt.executeQuery(forUpdateSql)
                    val (json, rowCount) = this.resultSetToJson(rs)
                    rs.close()

                    val record = BackupRecord(
                        createdAt = ZonedDateTime.now(),
                        operationType = parsed.type.name,
                        tableName = parsed.tableName,
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
```

- [ ] **Step 4: Rewrite resultSetToJson with Gson**

```kotlin
private fun resultSetToJson(rs: RemoteResultSet): Pair<String, Int> {
    val meta = rs.metaData
    val columnCount = meta.columnCount
    val columnNames = (1..columnCount).map { meta.getColumnName(it) }

    val gson = Gson()
    val jsonArray = JsonArray()
    while (rs.next()) {
        val obj = JsonObject()
        for (i in 1..columnCount) {
            val value = rs.getObject(i)
            if (value == null) obj.add(columnNames[i - 1], JsonNull.INSTANCE)
            else obj.addProperty(columnNames[i - 1], value.toString())
        }
        jsonArray.add(obj)
    }
    return Pair(gson.toJson(jsonArray), jsonArray.size())
}
```

- [ ] **Step 5: Update backupInsert to use Gson and mark partialColumns**

```kotlin
private fun backupInsert(parsed: ParsedDml, originalSql: String, connInfo: String) {
    val columns = parsed.insertColumns ?: run {
        log.warn("DML Backup: INSERT without column list, cannot backup")
        return
    }
    val rows = parsed.insertValues ?: return

    val gson = Gson()
    val jsonArray = JsonArray()
    for (values in rows) {
        val obj = JsonObject()
        columns.forEachIndexed { idx, name ->
            val value = values.getOrNull(idx)
            if (value == null) obj.add(name, JsonNull.INSTANCE)
            else obj.addProperty(name, value)
        }
        jsonArray.add(obj)
    }

    val record = BackupRecord(
        createdAt = ZonedDateTime.now(),
        operationType = parsed.type.name,
        tableName = parsed.tableName,
        originalSql = originalSql,
        connectionInfo = connInfo,
        backupDataJson = gson.toJson(jsonArray),
        rowCount = rows.size,
        partialColumns = true  // INSERT 始终标记为部分列（无法确认是否指定了全部列）
    )

    val id = BackupStorage.save(record)
    log.info("DML Backup: saved INSERT backup id=$id, rowCount=${rows.size} for table: ${parsed.tableName}")
    BackupStorage.trimRecords(DmlBackupSettings.getInstance().maxRecords)
}
```

- [ ] **Step 6: Add required imports**

Add to imports:

```kotlin
import com.intellij.notification.NotificationGroupManager
import com.intellij.notification.NotificationType
```

- [ ] **Step 7: Compile and verify**

Run: `./gradlew compileKotlin`
Expected: BUILD SUCCESSFUL

- [ ] **Step 8: Commit**

```bash
git add src/main/kotlin/com/github/dmlbackup/service/BackupService.kt
git commit -m "feat: add MySQL check, COUNT precheck, SELECT FOR UPDATE, PK detection, Gson serialization"
```

---

### Task 5: RollbackService Overhaul

**Files:**
- Modify: `src/main/kotlin/com/github/dmlbackup/service/RollbackService.kt`

Replace custom JSON parsing with Gson. Use real primary keys from BackupRecord. Extract SQL generation from execution (transaction wrapping done at call site).

- [ ] **Step 1: Rewrite RollbackService**

Replace the entire file content:

```kotlin
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
            "INSERT INTO `$tableName` ($columns) VALUES ($values)"
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
            "UPDATE `$tableName` SET $setClauses WHERE $whereClause LIMIT 1"
        }
    }

    private fun generateDeleteSqls(tableName: String, rows: List<Map<String, String?>>): List<String> {
        return rows.map { row ->
            val whereClauses = row.entries.joinToString(" AND ") {
                if (it.value == null) "`${it.key}` IS NULL"
                else "`${it.key}` = ${this.escapeValue(it.value)}"
            }
            "DELETE FROM `$tableName` WHERE $whereClauses LIMIT 1"
        }
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
```

Note: the old `rollback()` method that took a `DatabaseConnection` is removed. Execution is now done by the UI layer which wraps in a transaction. `generateRollbackSqls()` is the new public API.

- [ ] **Step 2: Compile and verify**

Run: `./gradlew compileKotlin`
Expected: BUILD SUCCESSFUL (may show errors from UI layer referencing old API - fixed in Task 7)

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/com/github/dmlbackup/service/RollbackService.kt
git commit -m "feat: rewrite RollbackService with Gson, real PK support, extract SQL generation"
```

---

### Task 6: DmlBackupActionListener Overhaul

**Files:**
- Modify: `src/main/kotlin/com/github/dmlbackup/listener/DmlBackupActionListener.kt`

Adds: multi-SQL support, MySQL check, timeout notification.

- [ ] **Step 1: Rewrite listener with multi-SQL, MySQL check, timeout notification**

Replace the entire file content:

```kotlin
package com.github.dmlbackup.listener

import com.github.dmlbackup.service.BackupService
import com.github.dmlbackup.service.DmlType
import com.github.dmlbackup.service.SqlParser
import com.github.dmlbackup.settings.DmlBackupSettings
import com.intellij.database.console.JdbcConsole
import com.intellij.notification.NotificationGroupManager
import com.intellij.notification.NotificationType
import com.intellij.openapi.actionSystem.ActionManager
import com.intellij.openapi.actionSystem.AnAction
import com.intellij.openapi.actionSystem.AnActionEvent
import com.intellij.openapi.actionSystem.CommonDataKeys
import com.intellij.openapi.actionSystem.ex.AnActionListener
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.editor.Editor
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class DmlBackupActionListener : AnActionListener {

    private val log = Logger.getInstance(DmlBackupActionListener::class.java)

    override fun beforeActionPerformed(action: AnAction, event: AnActionEvent) {
        val actionId = ActionManager.getInstance().getId(action) ?: return
        if (actionId != "Console.Jdbc.Execute") return

        if (!DmlBackupSettings.getInstance().enabled) return

        val editor = event.getData(CommonDataKeys.EDITOR) ?: return
        val sql = this.getExecutableSql(editor)
        if (sql.isNullOrBlank()) return

        val console = JdbcConsole.findConsole(event) ?: return

        // MySQL 检查
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
                        this.notifyUser(
                            "Backup failed for ${parsed.type} on '${parsed.tableName}': ${ex.message}",
                            NotificationType.ERROR
                        )
                    }
                }
            } finally {
                latch.countDown()
            }
        }

        if (!latch.await(10, TimeUnit.SECONDS)) {
            log.warn("DML Backup: backup timed out after 10s, proceeding with action")
            this.notifyUser(
                "DML backup timed out. This operation was NOT backed up, SQL will execute normally.",
                NotificationType.WARNING
            )
        }
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
```

- [ ] **Step 2: Register notification group in plugin.xml**

Add to `plugin.xml` inside `<extensions defaultExtensionNs="com.intellij">`:

```xml
<notificationGroup id="DML Backup" displayType="BALLOON"/>
```

- [ ] **Step 3: Compile and verify**

Run: `./gradlew compileKotlin`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/com/github/dmlbackup/listener/DmlBackupActionListener.kt \
       src/main/resources/META-INF/plugin.xml
git commit -m "feat: multi-SQL support, MySQL guard, timeout notification in listener"
```

---

### Task 7: BackupHistoryToolWindowFactory - Transaction Rollback + Partial Columns Warning

**Files:**
- Modify: `src/main/kotlin/com/github/dmlbackup/ui/BackupHistoryToolWindowFactory.kt`

The UI now calls `RollbackService.generateRollbackSqls()` and executes them in a transaction.

- [ ] **Step 1: Rewrite doRollback with transaction wrapping and partial columns warning**

Replace the `doRollback()` method in `BackupHistoryPanel`:

```kotlin
private fun doRollback() {
    val record = this.getSelectedRecord() ?: return

    if (record.status == "ROLLED_BACK") {
        Messages.showWarningDialog(project, "This record has already been rolled back", "DML Backup")
        return
    }

    // INSERT 部分列警告
    if (record.partialColumns && record.operationType == "INSERT") {
        val proceed = Messages.showYesNoDialog(
            project,
            "This INSERT did not specify all columns. Rollback may not be precise " +
                "(rows with default values in unspecified columns may be incorrectly matched).\n\nContinue?",
            "Partial Columns Warning",
            Messages.getWarningIcon()
        )
        if (proceed != Messages.YES) return
    }

    val confirm = Messages.showYesNoDialog(
        project,
        "Rollback ${record.operationType} on '${record.tableName}' (${record.rowCount} rows)?\n\n${record.originalSql}",
        "Confirm Rollback",
        Messages.getQuestionIcon()
    )
    if (confirm != Messages.YES) return

    try {
        val sqls = RollbackService.generateRollbackSqls(record)
        if (sqls.isEmpty()) {
            Messages.showWarningDialog(project, "No rollback SQL generated (empty backup data)", "DML Backup")
            return
        }

        val conn = DatabaseConnectionManager.getInstance().activeConnections.firstOrNull()
            ?: throw IllegalStateException("No active database connection")

        val remoteConn = conn.remoteConnection
        remoteConn.setAutoCommit(false)
        try {
            val stmt = remoteConn.createStatement()
            for (sql in sqls) {
                log.info("Executing rollback SQL: $sql")
                stmt.executeUpdate(sql)
            }
            stmt.close()
            remoteConn.commit()
        } catch (ex: Exception) {
            remoteConn.rollback()
            throw ex
        } finally {
            remoteConn.setAutoCommit(true)
        }

        BackupStorage.updateStatus(record.id, "ROLLED_BACK")
        Messages.showInfoMessage(project, "Rollback completed! (${sqls.size} statements)", "DML Backup")
        this.loadRecords()
    } catch (e: Exception) {
        log.error("Rollback failed", e)
        Messages.showErrorDialog(project, "Rollback failed: ${e.message}", "DML Backup")
    }
}
```

- [ ] **Step 2: Update imports**

Add/update imports in the file:

```kotlin
import com.github.dmlbackup.service.RollbackService
import com.github.dmlbackup.storage.BackupStorage
import com.intellij.database.dataSource.DatabaseConnectionManager
```

Remove the old `DatabaseConnection` import if it was specifically for the old rollback API.

- [ ] **Step 3: Compile and verify**

Run: `./gradlew compileKotlin`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/com/github/dmlbackup/ui/BackupHistoryToolWindowFactory.kt
git commit -m "feat: transaction-wrapped rollback, partial columns warning in UI"
```

---

### Task 8: plugin.xml Description + SafeExecuteAction Cleanup

**Files:**
- Modify: `src/main/resources/META-INF/plugin.xml`
- Modify: `src/main/kotlin/com/github/dmlbackup/action/SafeExecuteAction.kt`

- [ ] **Step 1: Update plugin.xml description to mention MySQL-only**

```xml
<description><![CDATA[
    Automatically backup data before executing DELETE/UPDATE/INSERT statements in DataGrip (MySQL only).
    Provides one-click rollback from the backup history panel.
    <br/><br/>
    Features:
    <ul>
        <li>Automatic backup with SELECT FOR UPDATE consistency</li>
        <li>Support for JOIN-based DELETE/UPDATE statements</li>
        <li>Multi-statement SQL support</li>
        <li>Large table protection with configurable row limit</li>
        <li>Transaction-wrapped rollback with real primary key detection</li>
    </ul>
]]></description>
```

- [ ] **Step 2: Update SafeExecuteAction with MySQL check**

In `SafeExecuteAction.actionPerformed()`, add MySQL check after getting console:

```kotlin
val console = JdbcConsole.findConsole(e)
if (console != null) {
    if (!BackupService.isMySql(console)) return this.delegateOriginal(e)
    BackupService.backup(console, parsed, sql)
    log.info("DML backup completed for: ${parsed.tableName}")
} else {
    log.warn("Cannot find JdbcConsole, skip backup")
}
```

Add import:

```kotlin
import com.github.dmlbackup.service.BackupService
```

(Already imported, just ensure isMySql call compiles)

- [ ] **Step 3: Compile full project**

Run: `./gradlew compileKotlin`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
git add src/main/resources/META-INF/plugin.xml \
       src/main/kotlin/com/github/dmlbackup/action/SafeExecuteAction.kt
git commit -m "feat: MySQL-only description, SafeExecuteAction MySQL guard"
```

---

### Task 9: Final Build Verification

- [ ] **Step 1: Full clean build**

Run: `./gradlew clean build`
Expected: BUILD SUCCESSFUL

- [ ] **Step 2: Verify plugin artifact**

Run: `ls -la build/distributions/`
Expected: Plugin zip file present

- [ ] **Step 3: Final commit if any remaining changes**

```bash
git status
# If clean, no commit needed
```
