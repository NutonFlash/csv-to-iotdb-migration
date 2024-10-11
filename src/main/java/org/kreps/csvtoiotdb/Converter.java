package org.kreps.csvtoiotdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.kreps.csvtoiotdb.DAO.RowProcessingDAO;
import org.kreps.csvtoiotdb.DAO.RowProcessingStatus;
import org.kreps.csvtoiotdb.configs.csv.CsvColumn;
import org.kreps.csvtoiotdb.configs.csv.CsvDataType;
import org.kreps.csvtoiotdb.configs.csv.CsvSettings;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBDevice;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBMeasurement;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBSettings;
import org.kreps.csvtoiotdb.converter.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Converter {
    private static final Logger logger = LoggerFactory.getLogger(Converter.class);

    private final IoTDBSettings ioTDBSettings;
    private final List<CsvSettings> csvSettingsList;
    private final Map<String, IoTDBDevice> deviceMap;
    private final Map<String, Map<String, IoTDBMeasurement>> measurementMap;
    private final Map<String, CsvColumn> csvColumnMap;
    private final RowProcessingDAO rowProcessingDAO;
    private final H2DatabaseManager dbManager;

    public Converter(IoTDBSettings ioTDBSettings, List<CsvSettings> csvSettingsList, H2DatabaseManager dbManager)
            throws SQLException {
        this.ioTDBSettings = ioTDBSettings;
        this.csvSettingsList = csvSettingsList;
        this.deviceMap = new HashMap<>();
        this.measurementMap = new HashMap<>();
        this.csvColumnMap = new HashMap<>();
        this.rowProcessingDAO = new RowProcessingDAO();
        this.dbManager = dbManager;
        initializeMaps();
        logger.info("Converter initialized with {} IoTDB devices and {} CSV settings",
                ioTDBSettings.getDevices().size(), csvSettingsList.size());
    }

    private void initializeMaps() {
        for (IoTDBDevice device : ioTDBSettings.getDevices()) {
            deviceMap.put(device.getDeviceId(), device);
            Map<String, IoTDBMeasurement> deviceMeasurements = new HashMap<>();
            for (IoTDBMeasurement measurement : device.getMeasurements()) {
                deviceMeasurements.put(measurement.getJoinKey(), measurement);
            }
            measurementMap.put(device.getDeviceId(), deviceMeasurements);
        }

        for (CsvSettings csvSettings : csvSettingsList) {
            for (CsvColumn column : csvSettings.getColumns()) {
                csvColumnMap.put(column.getJoinKey(), column);
            }
        }
        logger.debug("Initialized maps: {} devices, {} measurements, {} CSV columns",
                deviceMap.size(), measurementMap.size(), csvColumnMap.size());
    }

    public Map<String, List<RowData>> convert(List<Map<String, Object>> rows, long csvSettingId) throws SQLException {
        Map<String, List<RowData>> deviceDataMap = new HashMap<>();
        int skippedRows = 0;

        try (Connection conn = dbManager.getConnection()) {
            conn.setAutoCommit(false);
            try {
                for (Map<String, Object> row : rows) {
                    String rowId = (String) row.get("row_id");
                    int rowNumber = (Integer) row.get("row_number");
                    Long timestamp = (Long) row.get("timestamp");
                    if (timestamp == null) {
                        logger.warn("Row {} (number {}) skipped due to missing timestamp", rowId, rowNumber);
                        skippedRows++;
                        rowProcessingDAO.updateRowStatus(csvSettingId, rowId, rowNumber, RowProcessingStatus.FAILED,
                                "Missing timestamp", conn);
                        continue;
                    }

                    for (IoTDBDevice device : ioTDBSettings.getDevices()) {
                        try {
                            String fullPath = constructDevicePath(row, device);
                            Map<String, Object> measurements = extractMeasurements(row, device);

                            if (!measurements.isEmpty()) {
                                RowData rowData = new RowData(rowId, rowNumber, timestamp, measurements);
                                deviceDataMap.computeIfAbsent(fullPath, k -> new ArrayList<>()).add(rowData);
                                rowProcessingDAO.updateRowStatus(csvSettingId, rowId, rowNumber, RowProcessingStatus.PROCESSING,
                                        null, conn);
                            } else {
                                logger.debug("No measurements extracted for device: {} in row {} (number {})", 
                                        device.getDeviceId(), rowId, rowNumber);
                            }
                        } catch (IllegalStateException e) {
                            logger.error("Error processing row {} (number {}) for device: {}. Error: {}", rowId,
                                    rowNumber, device.getDeviceId(), e.getMessage());
                            rowProcessingDAO.updateRowStatus(csvSettingId, rowId, rowNumber, RowProcessingStatus.FAILED,
                                    e.getMessage(), conn);
                        }
                    }
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                logger.error("Database error during conversion: {}", e.getMessage(), e);
                throw e;
            }
        }

        logger.info("Conversion completed. Processed {} rows, skipped {} rows, resulting in {} device data entries",
                rows.size(), skippedRows, deviceDataMap.size());
        return deviceDataMap;
    }

    private String constructDevicePath(Map<String, Object> row, IoTDBDevice device) {
        String deviceId = device.getDeviceId();
        String pathColumn = device.getPathColumn();

        if (pathColumn != null && !pathColumn.isEmpty()) {
            Object pathValue = row.get(pathColumn);
            if (pathValue == null) {
                logger.error("Path column '{}' is null for device: {}", pathColumn, deviceId);
                throw new IllegalStateException("Path column '" + pathColumn + "' is null for device: " + deviceId);
            }
            return deviceId + "." + pathValue.toString();
        }

        return deviceId;
    }

    private Map<String, Object> extractMeasurements(Map<String, Object> row, IoTDBDevice device) {
        Map<String, Object> measurements = new HashMap<>();
        Map<String, IoTDBMeasurement> deviceMeasurements = measurementMap.get(device.getDeviceId());

        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String joinKey = entry.getKey();
            if (deviceMeasurements.containsKey(joinKey) && !joinKey.equals(device.getPathColumn())) {
                IoTDBMeasurement measurement = deviceMeasurements.get(joinKey);
                CsvColumn csvColumn = csvColumnMap.get(joinKey);
                Object value = convertValue(entry.getValue(), csvColumn.getType(), measurement.getDataType());
                if (value != null) {
                    measurements.put(measurement.getName(), value);
                }
            }
        }

        return measurements;
    }

    private Object convertValue(Object value, CsvDataType originalType, TSDataType targetType) {
        if (value == null) {
            return null;
        }

        try {
            return switch (originalType) {
                case DOUBLE -> switch (targetType) {
                    case DOUBLE -> ((Number) value).doubleValue();
                    case FLOAT -> {
                        double doubleValue = ((Number) value).doubleValue();
                        float floatValue = (float) doubleValue;
                        if (doubleValue != floatValue) {
                            System.out.println(
                                    "Warning: Possible loss of precision converting DOUBLE to FLOAT for value: "
                                            + value);
                        }
                        yield floatValue;
                    }
                    case INT32 -> {
                        double doubleValue = ((Number) value).doubleValue();
                        int intValue = (int) doubleValue;
                        if (doubleValue != intValue) {
                            System.out.println(
                                    "Warning: Possible loss of precision converting DOUBLE to INT32 for value: "
                                            + value);
                        }
                        yield intValue;
                    }
                    case INT64 -> {
                        double doubleValue = ((Number) value).doubleValue();
                        long longValue = (long) doubleValue;
                        if (doubleValue != longValue) {
                            System.out.println(
                                    "Warning: Possible loss of precision converting DOUBLE to INT64 for value: "
                                            + value);
                        }
                        yield longValue;
                    }
                    case TEXT -> value.toString();
                    default -> throw new IllegalArgumentException("Invalid conversion from DOUBLE to " + targetType);
                };
                case FLOAT -> switch (targetType) {
                    case DOUBLE -> ((Number) value).doubleValue();
                    case FLOAT -> ((Number) value).floatValue();
                    case INT32 -> {
                        float floatValue = ((Number) value).floatValue();
                        int intValue = (int) floatValue;
                        if (floatValue != intValue) {
                            System.out
                                    .println("Warning: Possible loss of precision converting FLOAT to INT32 for value: "
                                            + value);
                        }
                        yield intValue;
                    }
                    case INT64 -> {
                        float floatValue = ((Number) value).floatValue();
                        long longValue = (long) floatValue;
                        if (floatValue != longValue) {
                            System.out
                                    .println("Warning: Possible loss of precision converting FLOAT to INT64 for value: "
                                            + value);
                        }
                        yield longValue;
                    }
                    case TEXT -> value.toString();
                    default -> throw new IllegalArgumentException("Invalid conversion from FLOAT to " + targetType);
                };
                case INTEGER, LONG -> switch (targetType) {
                    case INT32 -> ((Number) value).intValue();
                    case INT64 -> ((Number) value).longValue();
                    case FLOAT -> ((Number) value).floatValue();
                    case DOUBLE -> ((Number) value).doubleValue();
                    case TEXT -> value.toString();
                    default -> throw new IllegalArgumentException(
                            "Invalid conversion from " + originalType + " to " + targetType);
                };
                case BOOLEAN -> switch (targetType) {
                    case BOOLEAN -> (Boolean) value;
                    case INT32 -> ((Boolean) value) ? 1 : 0;
                    case INT64 -> ((Boolean) value) ? 1L : 0L;
                    case TEXT -> value.toString();
                    default -> throw new IllegalArgumentException("Invalid conversion from BOOLEAN to " + targetType);
                };
                case TIME -> switch (targetType) {
                    case INT64 -> (Long) value;
                    case TEXT -> {
                        long timeMillis = (Long) value;
                        yield Instant.ofEpochMilli(timeMillis).toString();
                    }
                    default -> throw new IllegalArgumentException("Invalid conversion from TIME to " + targetType);
                };
                case STRING -> switch (targetType) {
                    case TEXT -> value.toString();
                    case INT32 -> Integer.parseInt(value.toString());
                    case INT64 -> Long.parseLong(value.toString());
                    case FLOAT -> Float.parseFloat(value.toString());
                    case DOUBLE -> Double.parseDouble(value.toString());
                    case BOOLEAN -> Boolean.parseBoolean(value.toString());
                    default -> throw new IllegalArgumentException("Invalid conversion from STRING to " + targetType);
                };
                default -> throw new IllegalArgumentException("Unsupported CSV data type: " + originalType);
            };
        } catch (IllegalArgumentException e) {
            System.err.println("Error converting value: " + value + " from " + originalType + " to " + targetType + ": "
                    + e.getMessage());
            return null;
        }
    }
}