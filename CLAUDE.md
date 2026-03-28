# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataGrip IDE plugin (Kotlin) that automatically backs up data before DELETE/UPDATE/INSERT execution and provides one-click rollback. Backups are persisted to local SQLite at `~/.datagrip-dml-backup/backup.db`.

## Build Commands

```bash
# Build plugin (produces distribution in build/)
./gradlew build

# Compile only (check for errors)
./gradlew compileKotlin

# Run DataGrip sandbox with plugin loaded
./gradlew runIde

# Build plugin distribution zip
./gradlew buildPlugin
```

- JVM target: Java 21
- Kotlin 2.1.0
- Target IDE: DataGrip 2025.1.3 (builds 251-253)

## Architecture

**Event-driven flow:** SQL execution in DataGrip console → `DmlBackupActionListener` intercepts "Console.Jdbc.Execute" via MessageBus → `SqlParser` analyzes SQL → `BackupService` captures original data → `BackupStorage` persists to SQLite → original SQL proceeds.

**Rollback flow:** User selects record in tool window → `RollbackService` generates reverse SQL (DELETE→INSERT, UPDATE→UPDATE with old values, INSERT→DELETE) → executes against active connection → marks record as ROLLED_BACK.

### Key Components

| Package | Class | Role |
|---------|-------|------|
| root | `DmlBackupStartupActivity` | Registers listener on project open |
| listener | `DmlBackupActionListener` | Intercepts SQL execution, runs backup on background thread with 10s timeout |
| service | `SqlParser` | Regex-based DML parser, extracts table/columns/WHERE clause |
| service | `BackupService` | SELECT-based backup for DELETE/UPDATE; direct parse for INSERT |
| service | `RollbackService` | Generates reverse SQL from backup JSON data |
| storage | `BackupStorage` | SQLite CRUD for backup_record table |
| model | `BackupRecord` | Data class: id, operationType, tableName, originalSql, backupDataJson, status |
| settings | `DmlBackupSettings` | App-level config: enabled flag, maxRecords |
| ui | `BackupHistoryToolWindowFactory` | Right-side panel: history table, rollback button, detail dialog |

### Design Decisions

- **No external JSON library** — custom regex-based JSON parsing in `RollbackService`
- **CountDownLatch** in listener ensures backup completes before SQL executes (max 10s)
- **SQLite embedded** via org.sqlite.JDBC — no server dependency
- Plugin XML: `src/main/resources/META-INF/plugin.xml`
