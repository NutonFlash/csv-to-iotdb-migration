package org.kreps.csvtoiotdb;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;

import org.kreps.csvtoiotdb.DAO.CsvSettingsDAO;
import org.kreps.csvtoiotdb.DAO.MigrationLogsDAO;
import org.kreps.csvtoiotdb.DAO.RowProcessingDAO;
import org.kreps.csvtoiotdb.configs.csv.CsvSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryScheduler implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(RetryScheduler.class);

    private final CsvSettingsDAO csvSettingsDAO;
    private final RowProcessingDAO rowProcessingDAO;
    private final MigrationLogsDAO logsDAO;
    private final int maxRetryCount;
    private final long retryIntervalMillis;

    public RetryScheduler(BlockingQueue<CsvSettings> csvSettingsQueue, int maxRetryCount, long retryIntervalMillis)
            throws SQLException {
        this.csvSettingsDAO = new CsvSettingsDAO();
        this.rowProcessingDAO = new RowProcessingDAO();
        this.logsDAO = new MigrationLogsDAO();
        this.maxRetryCount = maxRetryCount;
        this.retryIntervalMillis = retryIntervalMillis;
    }

    @Override
    public void run() {
        while (true) {
            try {
                // Sleep for the specified retry interval
                Thread.sleep(retryIntervalMillis);

                // Fetch all CSV settings
                List<String> allFilePaths = csvSettingsDAO.getAllCsvSettingFilePaths();
                for (String filePath : allFilePaths) {
                    Optional<Long> optionalId = csvSettingsDAO.getCsvSettingId(filePath);
                    if (!optionalId.isPresent()) {
                        logger.warn("CSV setting not found for file: {}", filePath);
                        continue;
                    }
                    long csvSettingId = optionalId.get();

                    // Fetch eligible failed rows
                    List<Integer> rowsToRetry = rowProcessingDAO.getRowsEligibleForRetry(csvSettingId, maxRetryCount);
                    if (rowsToRetry.isEmpty()) {
                        continue;
                    }

                    logger.info("Found {} rows eligible for retry in file: {}", rowsToRetry.size(), filePath);

                    for (int rowNumber : rowsToRetry) {
                        // Update the row status to PENDING
                        rowProcessingDAO.resetRowStatus(csvSettingId, rowNumber);
                        logsDAO.insertLog(csvSettingId, "INFO", "Row " + rowNumber + " reset to PENDING for retry.");
                    }
                }
            } catch (InterruptedException e) {
                logger.info("RetryScheduler interrupted. Exiting scheduler thread.");
                Thread.currentThread().interrupt();
                break;
            } catch (SQLException e) {
                logger.error("Database error in RetryScheduler: {}", e.getMessage(), e);
            }
        }
    }
}