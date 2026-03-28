package com.github.dmlbackup.model

import java.time.ZonedDateTime

data class BackupRecord(
    val id: Long = 0,
    val createdAt: ZonedDateTime = ZonedDateTime.now(),
    /** DELETE / UPDATE */
    val operationType: String,
    val tableName: String,
    val originalSql: String,
    /** 数据库连接标识，如 host:port/schema */
    val connectionInfo: String,
    /** 备份的行数据，JSON 数组格式 */
    val backupDataJson: String,
    val rowCount: Int,
    /** PENDING / ROLLED_BACK */
    val status: String = "PENDING",
    /** 主键列名，JSON 数组格式如 ["id"] 或 ["order_id","product_id"]，可为 null（旧数据兼容） */
    val primaryKeys: String? = null,
    /** INSERT 时是否只指定了部分列 */
    val partialColumns: Boolean = false
)
