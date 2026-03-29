package com.github.dmlbackup

import com.github.dmlbackup.listener.DmlBackupActionListener
import com.intellij.openapi.actionSystem.ex.AnActionListener
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.project.Project
import com.intellij.openapi.startup.ProjectActivity
import java.util.concurrent.atomic.AtomicBoolean

class DmlBackupStartupActivity : ProjectActivity {

    private val log = Logger.getInstance(DmlBackupStartupActivity::class.java)

    companion object {
        private val registered = AtomicBoolean(false)
    }

    override suspend fun execute(project: Project) {
        log.info("DML Backup plugin loaded for project: ${project.name}")

        // 只注册一次全局 AnActionListener，避免多项目重复注册
        if (registered.compareAndSet(false, true)) {
            ApplicationManager.getApplication().messageBus.connect()
                .subscribe(AnActionListener.TOPIC, DmlBackupActionListener())
            log.info("DML Backup: AnActionListener registered via MessageBus")
        }
    }
}
