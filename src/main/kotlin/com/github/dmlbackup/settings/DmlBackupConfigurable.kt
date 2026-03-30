package com.github.dmlbackup.settings

import com.intellij.openapi.options.Configurable
import com.intellij.ui.components.JBCheckBox
import com.intellij.ui.components.JBTextField
import com.intellij.ui.dsl.builder.*
import javax.swing.JComponent

class DmlBackupConfigurable : Configurable {

    private val enabledCheckBox = JBCheckBox("Enable automatic DML backup")
    private val maxRecordsField = JBTextField(6)
    private val maxBackupRowsField = JBTextField(6)

    override fun getDisplayName(): String = "DML Backup"

    override fun createComponent(): JComponent {
        val settings = DmlBackupSettings.getInstance()
        enabledCheckBox.isSelected = settings.enabled
        maxRecordsField.text = settings.maxRecords.toString()
        maxBackupRowsField.text = settings.maxBackupRows.toString()

        return panel {
            row { cell(enabledCheckBox) }
            separator()
            row("Max backup records:") { cell(maxRecordsField).comment("0 = unlimited") }
            row("Max backup rows per statement:") { cell(maxBackupRowsField).comment("0 = unlimited. Exceeding this limit will skip backup.") }
        }
    }

    override fun isModified(): Boolean {
        val settings = DmlBackupSettings.getInstance()
        return enabledCheckBox.isSelected != settings.enabled ||
            maxRecordsField.text.toIntOrNull() != settings.maxRecords ||
            maxBackupRowsField.text.toIntOrNull() != settings.maxBackupRows
    }

    override fun apply() {
        val settings = DmlBackupSettings.getInstance()
        settings.enabled = enabledCheckBox.isSelected
        settings.maxRecords = maxRecordsField.text.toIntOrNull() ?: 0
        settings.maxBackupRows = maxBackupRowsField.text.toIntOrNull() ?: 10000
    }

    override fun reset() {
        val settings = DmlBackupSettings.getInstance()
        enabledCheckBox.isSelected = settings.enabled
        maxRecordsField.text = settings.maxRecords.toString()
        maxBackupRowsField.text = settings.maxBackupRows.toString()
    }
}
