package com.github.dmlbackup.settings

import com.intellij.openapi.options.Configurable
import java.awt.FlowLayout
import javax.swing.*

class DmlBackupConfigurable : Configurable {

    private var panel: JPanel? = null
    private var enabledCheckBox: JCheckBox? = null
    private var maxRecordsField: JTextField? = null
    private var maxBackupRowsField: JTextField? = null

    override fun getDisplayName(): String = "DML Backup"

    override fun createComponent(): JComponent {
        val settings = DmlBackupSettings.getInstance()

        panel = JPanel().apply { layout = BoxLayout(this, BoxLayout.Y_AXIS) }

        enabledCheckBox = JCheckBox("Enable automatic DML backup", settings.enabled)
        val checkBoxRow = JPanel(FlowLayout(FlowLayout.LEFT)).apply { add(enabledCheckBox) }
        panel!!.add(checkBoxRow)

        val maxRecordsRow = JPanel(FlowLayout(FlowLayout.LEFT))
        maxRecordsRow.add(JLabel("Max backup records (0 = unlimited):"))
        maxRecordsField = JTextField(settings.maxRecords.toString(), 6)
        maxRecordsRow.add(maxRecordsField)
        panel!!.add(maxRecordsRow)

        val maxRowsRow = JPanel(FlowLayout(FlowLayout.LEFT))
        maxRowsRow.add(JLabel("Max backup rows per statement (0 = unlimited):"))
        maxBackupRowsField = JTextField(settings.maxBackupRows.toString(), 6)
        maxRowsRow.add(maxBackupRowsField)
        panel!!.add(maxRowsRow)

        return panel!!
    }

    override fun isModified(): Boolean {
        val settings = DmlBackupSettings.getInstance()
        return enabledCheckBox?.isSelected != settings.enabled ||
            maxRecordsField?.text?.toIntOrNull() != settings.maxRecords ||
            maxBackupRowsField?.text?.toIntOrNull() != settings.maxBackupRows
    }

    override fun apply() {
        val settings = DmlBackupSettings.getInstance()
        settings.enabled = enabledCheckBox?.isSelected ?: true
        settings.maxRecords = maxRecordsField?.text?.toIntOrNull() ?: 0
        settings.maxBackupRows = maxBackupRowsField?.text?.toIntOrNull() ?: 10000
    }

    override fun reset() {
        val settings = DmlBackupSettings.getInstance()
        enabledCheckBox?.isSelected = settings.enabled
        maxRecordsField?.text = settings.maxRecords.toString()
        maxBackupRowsField?.text = settings.maxBackupRows.toString()
    }
}
