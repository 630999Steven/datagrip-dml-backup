# DML Backup - DataGrip Plugin

DataGrip 插件，在执行 DELETE / UPDATE / INSERT 前自动备份数据，支持一键回滚。仅支持 MySQL / MariaDB。

**兼容版本：** DataGrip 2025.1 ~ 2025.3

## 功能

- **自动备份** — 在 SQL Console 或可视化编辑器中执行 DML 时，自动备份受影响的数据
- **一键回滚** — 从备份历史面板选择记录，一键生成并执行反向 SQL
- **类型感知** — 正确处理 BLOB、BIT、DECIMAL、DATETIME 等类型，回滚使用 PreparedStatement 参数绑定
- **JOIN 支持** — 支持 JOIN 型 DELETE / UPDATE 语句的备份与回滚
- **大表保护** — 可配置行数上限，超限时跳过备份并通知
- **主键检测** — 自动检测表主键，确保 UPDATE 回滚精准定位行
- **多语句支持** — 一次执行多条 DML 时逐条备份
- **数据源感知** — 按数据源和库名筛选备份记录，回滚时精确匹配原始连接
- **可视化编辑器** — 支持 DataGrip 表格编辑器的 Submit 操作备份

## 安装

### 从源码构建

```bash
./gradlew buildPlugin
```

产物在 `build/distributions/` 目录下，`.zip` 文件可通过 DataGrip → Settings → Plugins → Install Plugin from Disk 安装。

### 开发调试

```bash
./gradlew runIde
```

启动 DataGrip sandbox 并加载插件。

## 使用

1. 安装后右侧工具栏出现熊猫图标，点击打开 **DML Backup** 面板
2. 在 SQL Console 中正常执行 DELETE / UPDATE / INSERT，插件自动在后台备份
3. 备份记录显示在面板中，支持按数据源和库名筛选
4. 选中记录点击 **Rollback** 按钮或右键菜单回滚
5. 双击单元格复制内容

## 设置

Settings → Tools → DML Backup

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| Enable automatic DML backup | 启用/禁用自动备份 | 开启 |
| Max backup records | 最大保留记录数，0 = 不限制 | 0 |
| Max backup rows per statement | 单次备份最大行数，超限跳过备份 | 10000 |

## 技术栈

- Kotlin 2.1.0 / JVM 21
- IntelliJ Platform Plugin SDK
- DataGrip 2025.1.3（兼容 2025.1 - 2025.3）
- SQLite（本地持久化，存储在 `~/.datagrip-dml-backup/backup.db`）

## 架构

```
SQL Console 执行 DML
  → DmlBackupActionListener 拦截 Console.Jdbc.Execute
  → SqlParser 解析 SQL 类型、表名、WHERE 条件
  → BackupService 通过 SELECT FOR UPDATE 备份原始数据（类型感知）
  → BackupStorage 持久化到本地 SQLite
  → 原始 SQL 继续执行

回滚
  → RollbackService 从备份 JSON 生成反向 SQL（PreparedStatement 参数绑定）
  → 事务包裹执行，失败自动回滚
  → 标记记录状态为 ROLLED_BACK
```

## 技术要点

### 拦截时机与线程模型

插件通过 `AnActionListener.beforeActionPerformed` 拦截 `Console.Jdbc.Execute` 事件。该回调在 EDT（UI 线程）上同步执行，必须在返回前完成备份才能保证数据一致性。但 DataGrip 2025.1+ 禁止在 EDT 上调用 `DatabaseConnectionManager.createBlocking()`，因此备份逻辑通过 `executeOnPooledThread` 在后台线程执行，使用 `CountDownLatch` 阻塞 EDT 等待完成（最长 10 秒）。超时后通过 `AtomicBoolean` 标志位阻止迟到的备份落库，并通过 `cancelHook` 关闭数据库连接中断正在执行的查询。

### 类型感知备份

`ResultSet.getObject().toString()` 会导致 BLOB 变成 JVM 对象地址、BIT 变成 true/false、DECIMAL 丢失精度。解决方案是在备份时通过 `ResultSetMetaData.getColumnType()` 获取每列的 JDBC 类型，按类型选择提取方式（BLOB → `getBytes` + Base64、BIT → 字节转整数、DECIMAL → `getBigDecimal().toPlainString()`、时间类型 → `getString` 保留原始格式）。备份 JSON 采用 `{"__types__":{列名:JDBC类型码},"rows":[...]}` 格式，通过 JSON 首字符 `[`/`{` 区分新旧格式实现向后兼容。回滚时使用 `PreparedStatement` 按 JDBC 类型绑定参数，替代原有的字符串拼接。

### Grid 可视化编辑器的反射适配

DataGrip 的 DataGrid API 不是公开 API，通过反射访问 `DataGridUtilCore.getDatabaseTable()`、`DataAccessType.DATABASE_DATA`、`model.getValueAt()` 等方法获取表格数据。Grid 返回的时间类型是 `java.util.Date`/`java.sql.Timestamp` 对象，其 `toString()` 受 JVM 时区影响且与 JDBC 连接时区不一致（MySQL `system_time_zone=CST` 被 Java 误解为 UTC-6 而非 UTC+8）。解决方案是存储 Date 对象的 epoch millis，回滚时通过 `setTimestamp`/`setDate`/`setTime` 重建 JDBC 时间对象，利用同一 JDBC 驱动的时区转换正向反向抵消。

### Schema 定位

Console 执行 DML 时，需要确定目标 database 以便备份正确的数据。优先级链路：SQL 中显式指定的 schema（`other_db.table`）→ Console 当前选择器选中的库（`JdbcConsole.getCurrentNamespace()`）→ JDBC URL 中的默认库 → INFORMATION_SCHEMA 查询。其中 `getCurrentNamespace()` 是解决用户通过下拉框或 `USE` 语句切换库后备份到错误库的关键。

### SQL 解析

基于正则的轻量 SQL 解析器，覆盖简单 DELETE/UPDATE、JOIN 型 DELETE/UPDATE（含别名）、INSERT VALUES（含多行和 ON DUPLICATE KEY UPDATE）。解析器需要正确处理字符串字面量中的分号、括号、注释，避免误拆分或误匹配。对于 INSERT IGNORE / ON DUPLICATE KEY UPDATE 等不可靠回滚的变体，标记 `unsafeInsert` 并在回滚前警告用户。对无法解析的 DML 语句（如 `DELETE ... LIMIT`、`INSERT ... SELECT`）主动通知用户未备份，而非静默跳过。

## 已知限制

- **备份与执行不在同一事务** — 受 IntelliJ Platform API 限制，备份使用独立连接，与用户执行的 DML 不在同一事务中
- **SQL Console INSERT 无类型元数据** — INSERT 的值从 SQL 文本解析，无法获取 JDBC 类型信息，回滚使用字符串匹配（实际影响极小）
