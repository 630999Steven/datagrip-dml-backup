package com.github.dmlbackup

import com.github.dmlbackup.listener.DmlBackupActionListener
import com.intellij.openapi.actionSystem.ex.AnActionListener
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.project.Project
import com.intellij.openapi.startup.ProjectActivity

class DmlBackupStartupActivity : ProjectActivity {

    private val log = Logger.getInstance(DmlBackupStartupActivity::class.java)

    override suspend fun execute(project: Project) {
        log.info("DML Backup plugin loaded for project: ${project.name}")

        // 通过 MessageBus 注册 Action 监听器
        ApplicationManager.getApplication().messageBus.connect()
            .subscribe(AnActionListener.TOPIC, DmlBackupActionListener())
        log.info("DML Backup: AnActionListener registered via MessageBus")
    }
}
