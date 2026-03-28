# DML Backup Plugin - Risk Fixes Design

## Overview

修复插件现有的 11 个功能风险问题，提升 SQL 解析健壮性、回滚准确性、大表保护、事务一致性等。仅支持 MySQL。

---

## 1. SQL 解析增强 [高风险]

**现状**：SqlParser 用 3 个正则匹配 DELETE/UPDATE/INSERT，无法处理 JOIN、子查询、注释、别名等。

**方案**：
- 预处理阶段：先移除 SQL 注释（`/* ... */` 和 `-- ...`）
- 增强 DELETE 正则：支持 `DELETE t1 FROM t1 JOIN ...` 和 `DELETE FROM t WHERE id IN (...)`
- 增强 UPDATE 正则：支持 `UPDATE t1 JOIN t2 SET ...` 和 `UPDATE t AS a SET ...`
- 解析失败时返回 null，Listener 层发 IDE 通知告知用户"SQL 过于复杂，已跳过备份"
- 对于 JOIN 类 DML，备份 SQL 改为 `SELECT t1.* FROM t1 JOIN t2 ON ... WHERE ...`

**边界**：CTE 等极端复杂 SQL 不强求支持，解析失败走通知兜底。

## 2. UPDATE 回滚主键定位 [高风险]

**现状**：RollbackService 硬编码查找 `id` 列或用第一列做 WHERE 条件。

**方案**：
- BackupService 在备份时通过 `DatabaseMetaData.getPrimaryKeys()` 获取真实主键列名
- 主键信息存入 BackupRecord（新增 `primaryKeys` 字段，JSON 数组格式如 `["id"]` 或 `["order_id","product_id"]`）
- BackupStorage 的 backup_record 表新增 `primary_keys` 列（TEXT, nullable，兼容旧数据）
- RollbackService 回滚时用真实主键构造 WHERE 条件，支持复合主键
- 旧数据（primaryKeys 为空）回退到当前逻辑

## 3. 大表保护 [高风险]

**现状**：无 WHERE 的 DELETE/UPDATE 会触发全表 SELECT，可能 OOM 或超时。

**方案**：
- DmlBackupSettings 新增 `maxBackupRows` 配置项，默认 10000
- BackupService 执行备份 SELECT 前，先执行 `SELECT COUNT(*)` 预检
- 超过阈值时跳过备份，发 IDE 通知"影响行数超过阈值 N，已跳过备份"
- 配置 UI 中增加对应的 Spinner 控件

## 4. 竞态条件 - 事务包裹 [高风险]

**现状**：备份 SELECT 和实际 DML 之间无事务保护。

**方案**：
- BackupService 中使用 `SELECT ... FOR UPDATE` 替代普通 SELECT
- 在获取连接后设置 `autoCommit = false`
- 备份完成后不立即 commit，让后续 DML 在同一事务中执行
- DML 执行完成后 commit

**实现约束**：需要改造 Listener，将连接传递给后续 DML 执行流程。如果 DataGrip 的 Console.Jdbc.Execute action 使用独立连接，则此方案退化为：备份用独立事务 + SELECT FOR UPDATE 锁定行，备份完成后 commit 释放锁，DML 紧随其后执行。两次操作之间的窗口极短，大幅降低竞态风险。

## 5. 回滚事务性 [中风险]

**现状**：多行回滚生成多条独立 SQL，部分失败导致不一致。

**方案**：
- RollbackService 生成的回滚 SQL 列表，由调用方（UI 层）在执行时包裹事务
- 执行流程：`setAutoCommit(false)` -> 逐条执行 -> `commit()`，异常时 `rollback()`
- 回滚失败时通知用户具体错误信息

## 6. JSON 解析替换 [中风险]

**现状**：自定义正则解析 JSON，边界情况多。

**方案**：
- 使用 IntelliJ Platform 内置的 Gson (`com.google.gson.Gson`)
- BackupService 中用 Gson 序列化备份数据
- RollbackService 中用 Gson 反序列化备份数据
- 移除自定义 `parseJsonArray` 和相关正则逻辑

## 7. 多条 SQL 支持 [中风险]

**现状**：编辑器内多条 SQL 只解析第一条。

**方案**：
- Listener 层按分号拆分 SQL 文本（需排除字符串内的分号）
- 逐条解析，每条 DML 独立备份
- 非 DML 语句（SELECT、DDL 等）直接跳过

## 8. INSERT 回滚精确性 [中风险]

**现状**：INSERT 只备份了显式指定的列，回滚 DELETE 时用部分列匹配可能误删。

**方案**：
- INSERT 备份时，如果 SQL 未指定全部列，在备份记录中标记 `partialColumns = true`
- BackupRecord 新增 `partialColumns` 字段（Boolean，默认 false）
- BackupStorage 新增 `partial_columns` 列（INTEGER, nullable，0/1）
- 回滚此类记录时，通知用户"INSERT 未指定全部列，回滚可能不精确，请确认"
- 用户确认后才执行回滚

## 9. SQLite 并发 [低风险]

**现状**：多 DataGrip 窗口写同一 SQLite 文件可能锁冲突。

**方案**：
- BackupStorage 初始化时执行 `PRAGMA journal_mode=WAL`
- WAL 模式允许并发读写，大幅减少锁冲突
- 写操作加 `synchronized` 块防止进程内并发问题

## 10. 超时通知 [低风险]

**现状**：10 秒超时后用户无感知。

**方案**：
- Listener 中 `latch.await()` 超时后，通过 `NotificationGroupManager` 发 IDE 气泡通知
- 通知内容："DML 备份超时，本次操作未备份，SQL 已正常执行"
- 通知级别：WARNING

## 11. 仅支持 MySQL [低风险]

**现状**：LIMIT、INFORMATION_SCHEMA 等都是 MySQL 语法，但插件未明确限制。

**方案**：
- Listener 层在执行备份前，检查 JDBC URL 是否为 MySQL（`jdbc:mysql://` 或 `jdbc:mariadb://`）
- 非 MySQL 连接直接跳过备份，不通知（静默跳过）
- plugin.xml 描述中明确标注"仅支持 MySQL"

---

## 数据库变更

backup_record 表新增两列：
```sql
ALTER TABLE backup_record ADD COLUMN primary_keys TEXT;
ALTER TABLE backup_record ADD COLUMN partial_columns INTEGER DEFAULT 0;
```

BackupStorage 初始化时检测列是否存在，不存在则 ALTER TABLE 添加（兼容升级）。

## 配置变更

DmlBackupSettings 新增：
- `maxBackupRows: Int = 10000`

DmlBackupConfigurable UI 新增：
- Spinner: "Max backup rows (0 = unlimited)"

## 依赖变更

无新增外部依赖。Gson 来自 IntelliJ Platform 内置库。
