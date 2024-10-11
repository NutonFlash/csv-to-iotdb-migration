package org.kreps.csvtoiotdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.kreps.csvtoiotdb.DAO.CsvSettingsDAO;
import org.kreps.csvtoiotdb.DAO.CsvStatus;
import org.kreps.csvtoiotdb.DAO.JobStatus;
import org.kreps.csvtoiotdb.DAO.JobsDAO;
import org.kreps.csvtoiotdb.DAO.LogLevel;
import org.kreps.csvtoiotdb.DAO.MigrationLogsDAO;
import org.kreps.csvtoiotdb.DAO.RowProcessingDAO;
import org.kreps.csvtoiotdb.configs.csv.CsvSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationInitializer {
    private static final Logger logger = LoggerFactory.getLogger(MigrationInitializer.class);

    private final CsvSettingsDAO csvSettingsDAO;
    private final MigrationLogsDAO logsDAO;
    private final JobsDAO jobsDAO;
    private final RowProcessingDAO rowProcessingDAO;
    private final H2DatabaseManager dbManager;

    public MigrationInitializer(H2DatabaseManager dbManager) throws SQLException {
        this.dbManager = dbManager;
        this.csvSettingsDAO = new CsvSettingsDAO();
        this.logsDAO = new MigrationLogsDAO();
        this.jobsDAO = new JobsDAO();
        this.rowProcessingDAO = new RowProcessingDAO();
    }

    public void initialize(List<CsvSettings> csvSettingsList) throws SQLException {
        try (Connection conn = dbManager.getConnection()) {
            conn.setAutoCommit(false);
            try {
                for (CsvSettings csvSettings : csvSettingsList) {
                    for (String filePath : csvSettings.getFilePaths()) {
                        initializeOrUpdateCsvSetting(filePath, conn);
                    }
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                logger.error("Failed to initialize migration settings", e);
                throw e;
            }
        }
    }

    private void initializeOrUpdateCsvSetting(String filePath, Connection conn) throws SQLException {
        Optional<Long> existingId = csvSettingsDAO.getCsvSettingId(filePath);

        if (existingId.isPresent()) {
            long id = existingId.get();
            CsvStatus currentStatus = csvSettingsDAO.getStatus(id);

            if (currentStatus == CsvStatus.FAILED || currentStatus == CsvStatus.COMPLETED) {
                // Get failed row numbers
                Set<Integer> failedRowNumbers = rowProcessingDAO.getFailedRowNumbers(id, conn);

                if (!failedRowNumbers.isEmpty()) {
                    // Reset the status to PENDING for retry
                    csvSettingsDAO.updateStatus(id, CsvStatus.PENDING, conn);
                    csvSettingsDAO.setHasFailedRows(id, true, conn);
                    logsDAO.insertLog(id, LogLevel.INFO, "Reset migration status to PENDING for file: " + filePath
                            + " (processing failed rows only)", conn);
                } else {
                    csvSettingsDAO.setHasFailedRows(id, false, conn);
                    logsDAO.insertLog(id, LogLevel.INFO, "No failed rows to process for file: " + filePath, conn);
                }
            } else {
                logsDAO.insertLog(id, LogLevel.INFO, "Migration already initialized for file: " + filePath, conn);
            }

            // Check and update or create corresponding job
            initializeOrUpdateJob(id, filePath, conn);
        } else {
            // Insert new CSV setting
            long newId = csvSettingsDAO.insertCsvSetting(filePath, conn);
            logsDAO.insertLog(newId, LogLevel.INFO, "Initialized new migration for file: " + filePath, conn);

            // Create new job
            initializeOrUpdateJob(newId, filePath, conn);
        }
    }

    private void initializeOrUpdateJob(long csvSettingId, String filePath, Connection conn) throws SQLException {
        Optional<Long> latestJobId = jobsDAO.getLatestJobIdByCsvSettingId(csvSettingId, conn);

        if (latestJobId.isPresent()) {
            long jobId = latestJobId.get();
            JobStatus currentStatus = jobsDAO.getJobStatus(jobId, conn);

            if (currentStatus == JobStatus.FAILED || currentStatus == JobStatus.COMPLETED) {
                // Create a new job for retry
                jobsDAO.createJob(csvSettingId, conn);
                logsDAO.insertLog(csvSettingId, LogLevel.INFO, "Created new job for retry, file: " + filePath, conn);
            } else {
                // Job is still PENDING or IN_PROGRESS, no action needed
                logsDAO.insertLog(csvSettingId, LogLevel.INFO, "Existing job found for file: " + filePath, conn);
            }
        } else {
            jobsDAO.createJob(csvSettingId, conn);
            logsDAO.insertLog(csvSettingId, LogLevel.INFO, "Created new job for file: " + filePath, conn);
        }
    }
}