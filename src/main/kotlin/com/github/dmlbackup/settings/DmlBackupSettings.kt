package com.github.dmlbackup.settings

import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.components.PersistentStateComponent
import com.intellij.openapi.components.Service
import com.intellij.openapi.components.State
import com.intellij.openapi.components.Storage

@Service(Service.Level.APP)
@State(name = "DmlBackupSettings", storages = [Storage("dml-backup.xml")])
class DmlBackupSettings : PersistentStateComponent<DmlBackupSettings.State> {

    data class State(
        /** 是否启用自动备份 */
        var enabled: Boolean = true,
        /** 最大保留记录数，0 表示不限制 */
        var maxRecords: Int = 0,
        /** 单次备份最大行数，超过则跳过备份，0 表示不限制 */
        var maxBackupRows: Int = 10000
    )

    private var myState = State()

    override fun getState(): State = myState

    override fun loadState(state: State) {
        myState = state
    }

    var enabled: Boolean
        get() = myState.enabled
        set(value) { myState.enabled = value }

    var maxRecords: Int
        get() = myState.maxRecords
        set(value) { myState.maxRecords = value }

    var maxBackupRows: Int
        get() = myState.maxBackupRows
        set(value) { myState.maxBackupRows = value }

    companion object {
        fun getInstance(): DmlBackupSettings =
            ApplicationManager.getApplication().getService(DmlBackupSettings::class.java)
    }
}
