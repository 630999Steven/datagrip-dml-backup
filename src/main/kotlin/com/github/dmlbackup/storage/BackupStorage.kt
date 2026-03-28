package com.github.dmlbackup.storage

import com.github.dmlbackup.model.BackupRecord
import com.intellij.openapi.diagnostic.Logger
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/**
 * 本地 SQLite 备份存储
 */
object BackupStorage {

    private val log = Logger.getInstance(BackupStorage::class.java)
    private val DB_PATH = Paths.get(System.getProperty("user.home"), ".datagrip-dml-backup", "backup.db")
    private val TIME_FMT = DateTimeFormatter.ISO_ZONED_DATE_TIME

    init {
        Files.createDirectories(DB_PATH.parent)
        this.getConnection().use { conn ->
            val stmt = conn.createStatement()
            stmt.execute("PRAGMA journal_mode=WAL")
            stmt.execute(
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
            stmt.close()
            this.migrateIfNeeded(conn)
        }
    }

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

    fun findAll(): List<BackupRecord> {
        val result = mutableListOf<BackupRecord>()
        this.getConnection().use { conn ->
            val rs = conn.createStatement().executeQuery(
                "SELECT * FROM backup_record ORDER BY created_at DESC, id DESC"
            )
            while (rs.next()) {
                result.add(this.mapRow(rs))
            }
        }
        return result
    }

    fun findById(id: Long): BackupRecord? {
        this.getConnection().use { conn ->
            val ps = conn.prepareStatement("SELECT * FROM backup_record WHERE id = ?")
            ps.setLong(1, id)
            val rs = ps.executeQuery()
            return if (rs.next()) this.mapRow(rs) else null
        }
    }

    /**
     * 清理超出上限的旧记录
     */
    @Synchronized
    fun trimRecords(maxRecords: Int) {
        if (maxRecords <= 0) return
        this.getConnection().use { conn ->
            conn.createStatement().execute(
                "DELETE FROM backup_record WHERE id NOT IN (SELECT id FROM backup_record ORDER BY created_at DESC, id DESC LIMIT $maxRecords)"
            )
        }
    }

    @Synchronized
    fun deleteById(id: Long) {
        this.getConnection().use { conn ->
            val ps = conn.prepareStatement("DELETE FROM backup_record WHERE id = ?")
            ps.setLong(1, id)
            ps.executeUpdate()
        }
    }

    @Synchronized
    fun deleteByIds(ids: List<Long>) {
        if (ids.isEmpty()) return
        this.getConnection().use { conn ->
            val placeholders = ids.joinToString(",") { "?" }
            val ps = conn.prepareStatement("DELETE FROM backup_record WHERE id IN ($placeholders)")
            ids.forEachIndexed { idx, id -> ps.setLong(idx + 1, id) }
            ps.executeUpdate()
        }
    }

    @Synchronized
    fun updateStatus(id: Long, status: String) {
        this.getConnection().use { conn ->
            val ps = conn.prepareStatement("UPDATE backup_record SET status = ? WHERE id = ?")
            ps.setString(1, status)
            ps.setLong(2, id)
            ps.executeUpdate()
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

    private fun getConnection(): Connection {
        Class.forName("org.sqlite.JDBC")
        return DriverManager.getConnection("jdbc:sqlite:${DB_PATH}")
    }

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
}
