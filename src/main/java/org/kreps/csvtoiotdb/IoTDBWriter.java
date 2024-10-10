package org.kreps.csvtoiotdb;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
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

    public IoTDBWriter(IoTDBClientManager clientManager, IoTDBSchemaValidator schemaValidator,
            List<IoTDBDevice> iotdbSettingsList, int maxRetries, long retryInterval, long maxBackoffTime) {
        this.clientManager = clientManager;
        this.schemaValidator = schemaValidator;
        this.iotdbSettingsList = iotdbSettingsList;
        this.maxRetries = maxRetries;
        this.retryInterval = retryInterval;
        this.maxBackoffTime = maxBackoffTime;
        logger.info("IoTDBWriter initialized with maxRetries: {}, retryInterval: {}ms, maxBackoffTime: {}ms",
                maxRetries, retryInterval, maxBackoffTime);
    }

    public void writeData(Map<String, List<RowData>> deviceDataMap) {
        logger.info("Starting to write data for {} devices", deviceDataMap.size());
        for (Map.Entry<String, List<RowData>> entry : deviceDataMap.entrySet()) {
            String fullPath = entry.getKey();
            List<RowData> rows = entry.getValue();

            IoTDBDevice ioTDBSettings = findMatchingDevice(fullPath);
            if (ioTDBSettings == null) {
                logger.warn("No matching IoTDBDevice found for path: {}. Skipping {} rows.", fullPath, rows.size());
                continue;
            }

            processBatch(fullPath, rows, ioTDBSettings);
        }
        logger.info("Finished writing data for all devices");
    }

    private void processBatch(String fullPath, List<RowData> batch, IoTDBDevice ioTDBSettings) {
        logger.debug("Processing batch for device: {}. Batch size: {}", fullPath, batch.size());
        try {
            validateSchema(fullPath, ioTDBSettings);
            Tablet tablet = createTablet(fullPath, batch, ioTDBSettings);
            writeTablet(tablet, ioTDBSettings.getIsAlignedTimeseries());
            logger.info("Successfully wrote batch of {} rows for path: {}", batch.size(), fullPath);
        } catch (Exception e) {
            logger.error("Failed to process batch for path: {}. Error: {}. Batch size: {}", 
                         fullPath, e.getMessage(), batch.size(), e);
        }
    }

    private void validateSchema(String fullPath, IoTDBDevice ioTDBSettings) throws Exception {
        logger.debug("Validating schema for device: {}", fullPath);
        for (IoTDBMeasurement measurement : ioTDBSettings.getMeasurements()) {
            try {
                schemaValidator.ensureTimeseries(fullPath, measurement.getName(), measurement.getDataType(),
                        measurement.getEncoding(), measurement.getCompression());
            } catch (Exception e) {
                logger.error("Schema validation failed for measurement: {} in device: {}. Error: {}", 
                             measurement.getName(), fullPath, e.getMessage(), e);
                throw e;
            }
        }
        logger.debug("Schema validation successful for device: {}", fullPath);
    }

    private IoTDBDevice findMatchingDevice(String fullPath) {
        return iotdbSettingsList.stream()
                .filter(device -> fullPath.startsWith(device.getDeviceId()))
                .findFirst()
                .orElse(null);
    }

    private Tablet createTablet(String fullPath, List<RowData> rows, IoTDBDevice ioTDBSettings) {
        List<IoTDBMeasurement> measurements = ioTDBSettings.getMeasurements();
        List<MeasurementSchema> schemas = measurements.stream()
                .map(measurement -> new MeasurementSchema(measurement.getName(), measurement.getDataType(),
                        measurement.getEncoding(), measurement.getCompression()))
                .collect(Collectors.toList());

        logger.debug("Creating tablet for path: {}. Rows: {}, Measurements: {}", 
                     fullPath, rows.size(), measurements.size());

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
                        logger.error("Type mismatch for measurement: {} in row {}. Expected: {}, Got: {}. Error: {}", 
                                     measurement.getName(), i, measurement.getDataType(), value.getClass().getSimpleName(), e.getMessage());
                        // Handle the error (e.g., skip this value or use a default)
                    }
                } else {
                    logger.warn("Missing value for measurement: {} in row {} for device: {}", 
                                measurement.getName(), i, fullPath);
                }
            }
        }

        tablet.rowSize = rows.size();
        logger.debug("Tablet created with {} rows for device: {}", tablet.rowSize, fullPath);
        return tablet;
    }

    private void writeTablet(Tablet tablet, Boolean isAligned) {
        int attempt = 0;
        long startTime = System.currentTimeMillis();

        logger.info("Attempting to write tablet for device: {}. Rows: {}, Aligned: {}", 
                    tablet.deviceId, tablet.rowSize, isAligned);
        while (attempt <= this.maxRetries) {
            try {
                SessionPool sessionPool = clientManager.acquireSession();
                logger.debug("Session acquired. Inserting tablet for device: {}", tablet.deviceId);
                if (Boolean.TRUE.equals(isAligned)) {
                    sessionPool.insertAlignedTablet(tablet);
                } else {
                    sessionPool.insertTablet(tablet);
                }
                long duration = System.currentTimeMillis() - startTime;
                logger.info("Successfully inserted tablet for device: {} after {} attempts in {} ms", 
                            tablet.deviceId, (attempt + 1), duration);
                return; // Success
            } catch (IoTDBConnectionException e) {
                logger.error("Connection error writing tablet for device: {} on attempt {}. Error: {}", 
                             tablet.deviceId, (attempt + 1), e.getMessage(), e);
                handleRetry(attempt, "Connection error");
            } catch (StatementExecutionException e) {
                logger.error("Execution error writing tablet for device: {} on attempt {}. Error: {}", 
                             tablet.deviceId, (attempt + 1), e.getMessage(), e);
                handleRetry(attempt, "Execution error");
            }
            attempt++;
        }

        logger.error("Failed to insert tablet for device: {} after {} attempts", tablet.deviceId, (maxRetries + 1));
        // Implement further error handling or alerting here
    }

    private void handleRetry(int attempt, String errorType) {
        if (attempt >= this.maxRetries) {
            logger.warn("Max retries reached for {}. Giving up.", errorType);
            return;
        }

        long backoffTime = calculateBackoffTime(attempt);
        logger.info("Retrying after {} ms due to {}", backoffTime, errorType);

        try {
            Thread.sleep(backoffTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.warn("Thread interrupted during backoff");
        }
    }

    private long calculateBackoffTime(int attempt) {
        return Math.min(this.retryInterval * (long) Math.pow(2, attempt), this.maxBackoffTime);
    }
}