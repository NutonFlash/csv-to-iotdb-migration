package org.kreps.csvtoiotdb;

import java.sql.SQLException;
import java.util.List;

import org.kreps.csvtoiotdb.DAO.CsvSettingsDAO;
import org.kreps.csvtoiotdb.DAO.MigrationLogsDAO;
import org.kreps.csvtoiotdb.configs.csv.CsvSettings;

public class MigrationInitializer {
    private final CsvSettingsDAO csvSettingsDAO;
    private final MigrationLogsDAO logsDAO;

    public MigrationInitializer() throws SQLException {
        this.csvSettingsDAO = new CsvSettingsDAO();
        this.logsDAO = new MigrationLogsDAO();
    }

    public void initialize(List<CsvSettings> csvSettingsList) throws SQLException {
        for (CsvSettings csvSettings : csvSettingsList) {
            for (String filePath : csvSettings.getFilePaths()) {
                try {
                    csvSettingsDAO.insertOrIgnoreCsvSetting(filePath);
                    logsDAO.insertLog(null, "INFO", "Initialized migration for file: " + filePath);
                } catch (SQLException e) {
                    logsDAO.insertLog(null, "ERROR",
                            "Failed to initialize migration for file: " + filePath + " - " + e.getMessage());
                    throw e; // Propagate exception to halt the application
                }
            }
        }
    }
}