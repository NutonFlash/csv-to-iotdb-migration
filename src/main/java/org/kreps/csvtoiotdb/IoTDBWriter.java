package org.kreps.csvtoiotdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.kreps.csvtoiotdb.DAO.CsvSettingsDAO;
import org.kreps.csvtoiotdb.DAO.RowProcessingDAO;
import org.kreps.csvtoiotdb.DAO.RowProcessingStatus;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBDevice;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBMeasurement;
import org.kreps.csvtoiotdb.converter.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBWriter {
    private static final Logger logger = LoggerFactory.getLogger(IoTDBWriter.class);

    private final IoTDBClientManager clientManager;
    private final IoTDBSchemaValidator schemaValidator;
    private final List<IoTDBDevice> iotdbSettingsList;
    private final int maxRetries;
    private final long retryInterval;
    private final long maxBackoffTime;
    private final RowProcessingDAO rowProcessingDAO;
    private final CsvSettingsDAO csvSettingsDAO;
    private final H2DatabaseManager dbManager;

    public IoTDBWriter(IoTDBClientManager clientManager, IoTDBSchemaValidator schemaValidator,
            List<IoTDBDevice> iotdbSettingsList, int maxRetries, long retryInterval, long maxBackoffTime,
            H2DatabaseManager dbManager) throws SQLException {
        this.clientManager = clientManager;
        this.schemaValidator = schemaValidator;
        this.iotdbSettingsList = iotdbSettingsList;
        this.maxRetries = maxRetries;
        this.retryInterval = retryInterval;
        this.maxBackoffTime = maxBackoffTime;
        this.rowProcessingDAO = new RowProcessingDAO();
        this.csvSettingsDAO = new CsvSettingsDAO();
        this.dbManager = dbManager;
        logger.info("IoTDBWriter initialized with maxRetries: {}, retryInterval: {}ms, maxBackoffTime: {}ms",
                maxRetries, retryInterval, maxBackoffTime);
    }

    public List<String> writeData(Map<String, List<RowData>> deviceDataMap, long csvSettingId) throws IOException {
        List<String> failedRowIds = new ArrayList<>();
        String filePath;
        try {
            filePath = getFilePathForCsvSettingId(csvSettingId);
        } catch (IOException e) {
            logger.error("Failed to retrieve file path for csvSettingId: {}. Cannot proceed with writing data.",
                    csvSettingId, e);
            return failedRowIds;
        }

        logger.info("Starting to write data for {} devices from file: {}", deviceDataMap.size(), filePath);
        deviceDataMap.forEach(
                (fullPath, rows) -> processBatchForDevice(fullPath, rows, csvSettingId, filePath, failedRowIds));
        logger.info("Finished writing data for all devices from file: {}", filePath);
        return failedRowIds;
    }

    private String getFilePathForCsvSettingId(long csvSettingId) throws IOException {
        try {
            String filePath = csvSettingsDAO.getFilePathByCsvSettingId(csvSettingId);
            if (filePath == null || filePath.isEmpty()) {
                throw new IOException("No file path found for csvSettingId: " + csvSettingId);
            }
            return filePath;
        } catch (SQLException e) {
            logger.error("Database error while retrieving file path for csvSettingId: {}. Error: {}", csvSettingId,
                    e.getMessage(), e);
            throw new IOException("Failed to retrieve file path for csvSettingId: " + csvSettingId, e);
        }
    }

    private void processBatchForDevice(String fullPath, List<RowData> rows, long csvSettingId, String filePath,
            List<String> failedRowIds) {
        logger.debug("Processing batch for device: {}. Batch size: {}. File: {}", fullPath, rows.size(), filePath);
        IoTDBDevice ioTDBSettings = findMatchingDevice(fullPath);
        if (ioTDBSettings == null) {
            logger.warn("No matching IoTDBDevice found for path: {}. File: {}", fullPath, filePath);
            return;
        }

        try {
            validateSchema(fullPath, ioTDBSettings, filePath);
            List<String> rowIds = rows.stream().map(RowData::getRowId).collect(Collectors.toList());
            Tablet tablet = createTablet(fullPath, rows, ioTDBSettings, filePath);
            writeTablet(tablet, rows, ioTDBSettings.getIsAlignedTimeseries(), csvSettingId, filePath);

            logger.info("Successfully wrote batch for path: {}. File: {}", fullPath, filePath);
        } catch (Exception e) {
            logger.error("Failed to process batch for path: {}. File: {}. Error: {}", fullPath, filePath,
                    e.getMessage(), e);
            failedRowIds.addAll(rows.stream().map(RowData::getRowId).collect(Collectors.toList()));
            try (Connection conn = dbManager.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    for (RowData row : rows) {
                        rowProcessingDAO.updateRowStatus(csvSettingId, row.getRowId(), row.getRowNumber(),
                                RowProcessingStatus.FAILED, e.getMessage(), conn);
                    }
                    conn.commit();
                } catch (SQLException sqlEx) {
                    conn.rollback();
                    logger.error("Failed to update row statuses for csvSettingId: {}, File: {}", csvSettingId, filePath,
                            sqlEx);
                }
            } catch (SQLException connEx) {
                logger.error("Database connection error while updating row statuses", connEx);
            }
        }
    }

    private void validateSchema(String fullPath, IoTDBDevice ioTDBSettings, String filePath) throws Exception {
        logger.debug("Validating schema for device: {}. File: {}", fullPath, filePath);
        for (IoTDBMeasurement measurement : ioTDBSettings.getMeasurements()) {
            try {
                schemaValidator.ensureTimeseries(fullPath, measurement.getName(), measurement.getDataType(),
                        measurement.getEncoding(), measurement.getCompression());
            } catch (Exception e) {
                logger.error("Schema validation failed for measurement: {} in device: {}. File: {}. Error: {}",
                        measurement.getName(), fullPath, filePath, e.getMessage(), e);
                throw e;
            }
        }
        logger.debug("Schema validation successful for device: {}. File: {}", fullPath, filePath);
    }

    private IoTDBDevice findMatchingDevice(String fullPath) {
        return iotdbSettingsList.stream()
                .filter(device -> fullPath.startsWith(device.getDeviceId()))
                .findFirst()
                .orElse(null);
    }

    private Tablet createTablet(String fullPath, List<RowData> rows, IoTDBDevice ioTDBSettings, String filePath) {
        List<IoTDBMeasurement> measurements = ioTDBSettings.getMeasurements();
        List<MeasurementSchema> schemas = measurements.stream()
                .map(measurement -> new MeasurementSchema(measurement.getName(), measurement.getDataType(),
                        measurement.getEncoding(), measurement.getCompression()))
                .collect(Collectors.toList());

        logger.debug("Creating tablet for path: {}. Rows: {}, Measurements: {}. File: {}",
                fullPath, rows.size(), measurements.size(), filePath);

        Tablet tablet = new Tablet(fullPath, schemas, rows.size());

        for (int i = 0; i < rows.size(); i++) {
            RowData row = rows.get(i);
            tablet.addTimestamp(i, row.getTimestamp());
            for (IoTDBMeasurement measurement : measurements) {
                Object value = row.getMeasurements().get(measurement.getName());
                if (value != null) {
                    try {
                        tablet.addValue(measurement.getName(), i, value);
                    } catch (ClassCastException e) {
                        logger.error(
                                "Type mismatch for measurement: {} in row {} for device: {}. File: {}. Expected: {}, Got: {}. Error: {}",
                                measurement.getName(), i, fullPath, filePath, measurement.getDataType(),
                                value.getClass().getSimpleName(),
                                e.getMessage());
                        // Handle the error (e.g., skip this value or use a default)
                    }
                } else {
                    logger.warn("Missing value for measurement: {} in row {} for device: {}. File: {}",
                            measurement.getName(), i, fullPath, filePath);
                }
            }
        }

        tablet.rowSize = rows.size();
        logger.debug("Tablet created with {} rows for device: {}. File: {}", tablet.rowSize, fullPath, filePath);
        return tablet;
    }

    private void writeTablet(Tablet tablet, List<RowData> rowDataList, Boolean isAligned, long csvSettingId,
            String filePath) {
        int attempt = 0;
        long startTime = System.currentTimeMillis();

        logger.info("Attempting to write tablet for device: {}. Rows: {}, Aligned: {}. File: {}",
                tablet.deviceId, tablet.rowSize, isAligned, filePath);
        while (attempt <= this.maxRetries) {
            try (Connection conn = dbManager.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    SessionPool sessionPool = clientManager.acquireSession();
                    logger.debug("Session acquired. Inserting tablet for device: {}. File: {}", tablet.deviceId,
                            filePath);
                    if (Boolean.TRUE.equals(isAligned)) {
                        sessionPool.insertAlignedTablet(tablet);
                    } else {
                        sessionPool.insertTablet(tablet);
                    }

                    // Update row statuses to COMPLETED
                    for (RowData rowData : rowDataList) {
                        rowProcessingDAO.updateRowStatus(csvSettingId, rowData.getRowId(), rowData.getRowNumber(),
                                RowProcessingStatus.COMPLETED, null, conn);
                        logger.debug("Row {} (number {}) successfully written", rowData.getRowId(),
                                rowData.getRowNumber());
                    }

                    conn.commit();
                    long duration = System.currentTimeMillis() - startTime;
                    logger.info("Successfully inserted tablet for device: {} after {} attempts in {} ms. File: {}",
                            tablet.deviceId, (attempt + 1), duration, filePath);
                    return; // Success
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    conn.rollback();
                    logger.error("Error writing tablet for device: {} on attempt {}. File: {}. Error: {}",
                            tablet.deviceId, (attempt + 1), filePath, e.getMessage(), e);
                    // Update row statuses to RETRY or FAILED
                    for (RowData rowData : rowDataList) {
                        RowProcessingStatus status = attempt < this.maxRetries ? RowProcessingStatus.RETRY
                                : RowProcessingStatus.FAILED;
                        rowProcessingDAO.updateRowStatus(csvSettingId, rowData.getRowId(), rowData.getRowNumber(),
                                status,
                                e.getMessage(), conn);
                        logger.error("Failed to write row {} (number {}), status set to {}", rowData.getRowId(),
                                rowData.getRowNumber(), status);
                    }
                    handleRetry(attempt, e.getClass().getSimpleName(), filePath);
                } catch (SQLException e) {
                    conn.rollback();
                    logger.error("Database error while updating row statuses. File: {}. Error: {}", filePath,
                            e.getMessage(), e);
                    throw e; // Optional: decide whether to retry or fail
                }
            } catch (SQLException e) {
                logger.error("Database connection error while writing tablet. File: {}. Error: {}", filePath,
                        e.getMessage(), e);
                handleRetry(attempt, "DatabaseConnection", filePath);
            }
            attempt++;
        }

        logger.error("Failed to insert tablet for device: {} after {} attempts. File: {}",
                tablet.deviceId, (maxRetries + 1), filePath);
        // Update row statuses to FAILED
        try (Connection conn = dbManager.getConnection()) {
            conn.setAutoCommit(false);
            try {
                for (RowData rowData : rowDataList) {
                    rowProcessingDAO.updateRowStatus(csvSettingId, rowData.getRowId(), rowData.getRowNumber(),
                            RowProcessingStatus.FAILED, "Max retries reached", conn);
                    logger.error("Row {} (number {}) failed after max retries", rowData.getRowId(),
                            rowData.getRowNumber());
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                logger.error("Failed to update row statuses to FAILED for csvSettingId: {}. File: {}", csvSettingId,
                        filePath, e);
            }
        } catch (SQLException connEx) {
            logger.error("Database connection error while updating row statuses", connEx);
        }
    }

    private void handleRetry(int attempt, String errorType, String filePath) {
        if (attempt >= this.maxRetries) {
            logger.warn("Max retries reached for {}. Giving up. File: {}", errorType, filePath);
            return;
        }

        long backoffTime = calculateBackoffTime(attempt);
        logger.info("Retrying after {} ms due to {}. File: {}", backoffTime, errorType, filePath);

        try {
            Thread.sleep(backoffTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.warn("Thread interrupted during backoff. File: {}", filePath);
        }
    }

    private long calculateBackoffTime(int attempt) {
        return Math.min(this.retryInterval * (long) Math.pow(2, attempt), this.maxBackoffTime);
    }
}