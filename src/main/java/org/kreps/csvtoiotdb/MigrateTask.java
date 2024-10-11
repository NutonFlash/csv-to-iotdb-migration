package org.kreps.csvtoiotdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import org.kreps.csvtoiotdb.DAO.CsvSettingsDAO;
import org.kreps.csvtoiotdb.DAO.CsvStatus;
import org.kreps.csvtoiotdb.DAO.JobStatus;
import org.kreps.csvtoiotdb.DAO.JobsDAO;
import org.kreps.csvtoiotdb.DAO.LogLevel;
import org.kreps.csvtoiotdb.DAO.MigrationLogsDAO;
import org.kreps.csvtoiotdb.DAO.RowProcessingDAO;
import org.kreps.csvtoiotdb.DAO.RowProcessingStatus;
import org.kreps.csvtoiotdb.configs.csv.CsvSettings;
import org.kreps.csvtoiotdb.converter.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a migration task that processes CSV files and writes data to
 * IoTDB.
 */
public class MigrateTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MigrateTask.class);

    private final BlockingQueue<CsvSettings> csvSettingsQueue;
    private final Converter converter;
    private final IoTDBWriter writer;
    private final int batchSize;
    private final CsvSettingsDAO csvSettingsDAO;
    private final RowProcessingDAO rowProcessingDAO;
    private final MigrationLogsDAO logsDAO;
    private final JobsDAO jobsDAO;
    private final H2DatabaseManager dbManager;

    public MigrateTask(BlockingQueue<CsvSettings> csvSettingsQueue,
            Converter converter, IoTDBWriter writer, int batchSize, H2DatabaseManager dbManager) throws SQLException {
        this.csvSettingsQueue = csvSettingsQueue;
        this.converter = converter;
        this.writer = writer;
        this.batchSize = batchSize;
        this.dbManager = dbManager;
        this.csvSettingsDAO = new CsvSettingsDAO();
        this.rowProcessingDAO = new RowProcessingDAO();
        this.logsDAO = new MigrationLogsDAO();
        this.jobsDAO = new JobsDAO();
    }

    @Override
    public void run() {
        while (true) {
            CsvSettings csvSettings = csvSettingsQueue.poll();
            if (csvSettings == null) {
                logger.info("No more CSV settings to process. Thread exiting.");
                break;
            }

            for (String filePath : csvSettings.getFilePaths()) {
                Connection conn = null;
                try {
                    conn = dbManager.getConnection();
                    conn.setAutoCommit(false);

                    Optional<Long> optionalId = csvSettingsDAO.getCsvSettingId(filePath);
                    if (!optionalId.isPresent()) {
                        logger.error("CSV setting not found in database for file: {}", filePath);
                        continue;
                    }
                    long csvSettingId = optionalId.get();

                    // Update status to IN_PROGRESS
                    csvSettingsDAO.updateStatus(csvSettingId, CsvStatus.IN_PROGRESS, conn);
                    logsDAO.insertLog(csvSettingId, LogLevel.INFO, "Migration started for file: " + filePath, conn);

                    // Check if a job already exists, if not, create a new one
                    Optional<Long> existingJobId = jobsDAO.getLatestJobIdByCsvSettingId(csvSettingId, conn);
                    long jobId;
                    if (existingJobId.isPresent()) {
                        jobId = existingJobId.get();
                        jobsDAO.updateJobStatus(jobId, JobStatus.IN_PROGRESS, null, conn);
                        logger.info("Updated existing job {} for file: {}", jobId, filePath);
                    } else {
                        jobId = jobsDAO.createJob(csvSettingId, conn);
                        logger.info("Created new job {} for file: {}", jobId, filePath);
                    }

                    conn.commit();

                    try (CSVReader csvReader = new CSVReader(csvSettings, this.batchSize, dbManager)) {
                        List<Map<String, Object>> batch;
                        while ((batch = csvReader.readBatch()) != null) {
                            processBatch(batch, csvSettingId, conn);
                            conn.commit(); // Commit after each batch
                        }
                        conn.commit(); // Commit after all batches (this will include the total rows update)

                        // Update status to COMPLETED
                        csvSettingsDAO.updateStatus(csvSettingId, CsvStatus.COMPLETED, conn);
                        logsDAO.insertLog(csvSettingId, LogLevel.INFO, "Migration completed for file: " + filePath,
                                conn);
                        jobsDAO.updateJobStatus(jobId, JobStatus.COMPLETED, null, conn);
                        logger.info("Migration completed for file: {}", filePath);
                        conn.commit();
                    } catch (IOException e) {
                        conn.rollback();
                        String errorMessage = "Error opening or reading CSV file: " + e.getMessage();
                        handleMigrationFailure(csvSettingId, filePath, errorMessage, conn);
                    }
                } catch (SQLException e) {
                    logger.error("Database error during migration task: {}", e.getMessage(), e);
                    if (conn != null) {
                        try {
                            conn.rollback();
                        } catch (SQLException rollbackEx) {
                            logger.error("Error rolling back transaction", rollbackEx);
                        }
                    }
                } finally {
                    if (conn != null) {
                        try {
                            conn.setAutoCommit(true);
                            conn.close();
                        } catch (SQLException e) {
                            logger.error("Error closing database connection", e);
                        }
                    }
                }
            }
        }
    }

    private void processBatch(List<Map<String, Object>> batch, long csvSettingId, Connection conn)
            throws SQLException, IOException {
        Map<String, List<RowData>> deviceDataMap = new HashMap<>();
        List<String> failedRowIds = new ArrayList<>();

        try {
            deviceDataMap = converter.convert(batch, csvSettingId);
        } catch (SQLException e) {
            logger.error("Error during batch conversion for csvSettingId: {}. Error: {}", csvSettingId, e.getMessage(),
                    e);
            // Mark all rows as failed if conversion fails
            failedRowIds = batch.stream()
                    .map(row -> (String) row.get("row_id"))
                    .collect(Collectors.toList());
        }

        if (!deviceDataMap.isEmpty()) {
            List<String> writeFailedRowIds = writer.writeData(deviceDataMap, csvSettingId);
            failedRowIds.addAll(writeFailedRowIds);
        }

        int successfulRows = batch.size() - failedRowIds.size();
        int failedRows = failedRowIds.size();

        // Get the latest job ID for this CSV setting
        Optional<Long> latestJobId = jobsDAO.getLatestJobIdByCsvSettingId(csvSettingId, conn);
        if (latestJobId.isPresent()) {
            long jobId = latestJobId.get();
            jobsDAO.updateJobProgress(jobId, successfulRows, failedRows, conn);
        }

        csvSettingsDAO.incrementProcessedRows(csvSettingId, successfulRows, conn);

        if (!failedRowIds.isEmpty()) {
            updateRowStatuses(csvSettingId, failedRowIds, RowProcessingStatus.FAILED,
                    "Failed to process or write row", conn);
        }

        String logMessage = String.format("Processed batch: %d successful, %d failed", successfulRows, failedRows);
        logsDAO.insertLog(csvSettingId, failedRows > 0 ? LogLevel.WARNING : LogLevel.INFO, logMessage, conn);
    }

    private void updateRowStatuses(long csvSettingId, List<String> rowIds, RowProcessingStatus status,
            String errorMessage, Connection conn) throws SQLException {
        rowProcessingDAO.updateRowStatuses(csvSettingId, rowIds, status, errorMessage, conn);
    }

    private void handleMigrationFailure(long csvSettingId, String filePath, String errorMessage, Connection conn)
            throws SQLException {
        csvSettingsDAO.updateStatus(csvSettingId, CsvStatus.FAILED, conn);
        csvSettingsDAO.updateErrorMessage(csvSettingId, errorMessage, conn);
        logsDAO.insertLog(csvSettingId, LogLevel.ERROR,
                "Migration failed for file: " + filePath + " - " + errorMessage, conn);

        // Get the latest job ID for this CSV setting
        Optional<Long> latestJobId = jobsDAO.getLatestJobIdByCsvSettingId(csvSettingId, conn);
        if (latestJobId.isPresent()) {
            long jobId = latestJobId.get();
            jobsDAO.updateJobStatus(jobId, JobStatus.FAILED, errorMessage, conn);
        }

        logger.error("Migration failed for file: {} - {}", filePath, errorMessage);
        conn.commit();
    }
}