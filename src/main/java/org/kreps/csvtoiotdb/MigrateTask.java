package org.kreps.csvtoiotdb;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;

import org.kreps.csvtoiotdb.DAO.CsvSettingsDAO;
import org.kreps.csvtoiotdb.DAO.JobsDAO;
import org.kreps.csvtoiotdb.DAO.MigrationLogsDAO;
import org.kreps.csvtoiotdb.DAO.RowProcessingDAO;
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

    public MigrateTask(BlockingQueue<CsvSettings> csvSettingsQueue,
            Converter converter, IoTDBWriter writer, int batchSize) throws SQLException {
        this.csvSettingsQueue = csvSettingsQueue;
        this.converter = converter;
        this.writer = writer;
        this.batchSize = batchSize;
        this.csvSettingsDAO = new CsvSettingsDAO();
        this.rowProcessingDAO = new RowProcessingDAO();
        this.logsDAO = new MigrationLogsDAO();
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
                try {
                    Optional<Long> optionalId = csvSettingsDAO.getCsvSettingId(filePath);
                    if (!optionalId.isPresent()) {
                        logger.error("CSV setting not found in database for file: {}", filePath);
                        continue;
                    }
                    long csvSettingId = optionalId.get();

                    // Update status to IN_PROGRESS
                    csvSettingsDAO.updateStatus(csvSettingId, "IN_PROGRESS");
                    logsDAO.insertLog(csvSettingId, "INFO", "Migration started for file: " + filePath);

                    // Create a job entry
                    JobsDAO jobsDAO = new JobsDAO();
                    long jobId = jobsDAO.createJob(filePath);

                    try (CSVReader csvReader = new CSVReader(csvSettings, this.batchSize)) {
                        List<Map<String, Object>> batch;
                        while ((batch = csvReader.readBatch()) != null) {
                            Map<String, List<RowData>> deviceDataMap = converter.convert(batch);
                            writer.writeData(deviceDataMap);
                            csvSettingsDAO.incrementProcessedRows(csvSettingId, batch.size());
                            logsDAO.insertLog(csvSettingId, "INFO", "Processed batch of size: " + batch.size());
                        }

                        // Update status to COMPLETED
                        csvSettingsDAO.updateStatus(csvSettingId, "COMPLETED");
                        logsDAO.insertLog(csvSettingId, "INFO", "Migration completed for file: " + filePath);
                        jobsDAO.updateJobStatus(jobId, "COMPLETED", null);
                        logger.info("Migration completed for file: {}", filePath);
                    } catch (Exception e) {
                        // Update status to FAILED with error message
                        csvSettingsDAO.updateStatus(csvSettingId, "FAILED");
                        csvSettingsDAO.incrementFailedRows(csvSettingId, 1, e.getMessage());
                        logsDAO.insertLog(csvSettingId, "ERROR",
                                "Migration failed for file: " + filePath + " - " + e.getMessage());
                        jobsDAO.updateJobStatus(jobId, "FAILED", e.getMessage());
                        logger.error("Migration failed for file: {} - {}", filePath, e.getMessage(), e);

                        // Optionally, add failed rows to retry queue
                        enqueueFailedRows(csvSettingId);
                    }
                } catch (SQLException e) {
                    logger.error("Database error during migration task: {}", e.getMessage(), e);
                }
            }
        }
    }

    private void enqueueFailedRows(long csvSettingId) {
        try {
            List<Integer> failedRows = rowProcessingDAO.getFailedRowNumbers(csvSettingId);
            for (int rowNumber : failedRows) {
                // Reset row status to PENDING for retry
                rowProcessingDAO.resetRowStatus(csvSettingId, rowNumber);
                // Optionally, enqueue specific CsvSettings if needed
                // Implement as per your application's design
                logsDAO.insertLog(csvSettingId, "INFO", "Enqueued row " + rowNumber + " for retry.");
            }
        } catch (SQLException e) {
            logger.error("Failed to enqueue failed rows for csv_setting_id {}: {}", csvSettingId, e.getMessage(), e);
        }
    }
}