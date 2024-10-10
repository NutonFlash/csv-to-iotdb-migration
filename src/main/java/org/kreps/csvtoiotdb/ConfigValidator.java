package org.kreps.csvtoiotdb;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.kreps.csvtoiotdb.configs.MigrationConfig;
import org.kreps.csvtoiotdb.configs.csv.CsvColumn;
import org.kreps.csvtoiotdb.configs.csv.CsvSettings;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBDevice;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBMeasurement;

/**
 * A utility class for validating the configuration of CSV to IoTDB migration.
 */
public class ConfigValidator {

    private static final String RESERVED_TIMESTAMP_JOINKEY = "timestamp";

    /**
     * Validates the entire migration configuration.
     *
     * @param config the migration configuration to validate
     * @throws IllegalArgumentException if any validation rule is violated
     */
    public static void validateConfig(MigrationConfig config) {
        validateCsvSettings(config.getCsvSettings());
        validateIoTDBSettings(config);
    }

    /**
     * Validates the CSV settings within the migration configuration.
     *
     * @param csvSettingsList the list of CSV settings to validate
     * @throws IllegalArgumentException if any CSV setting is invalid
     */
    private static void validateCsvSettings(List<CsvSettings> csvSettingsList) {
        Set<String> seenJoinKeys = new HashSet<>();

        for (CsvSettings csvSettings : csvSettingsList) {
            // Validate timestamp column
            CsvColumn timestampColumn = csvSettings.getTimestampColumn();
            if (timestampColumn == null) {
                throw new IllegalArgumentException("Each CSV setting must have exactly one timestamp column.");
            }

            // Validate file paths
            for (String filePath : csvSettings.getFilePaths()) {
                File file = new File(filePath);
                if (!file.exists() || !file.isFile()) {
                    throw new IllegalArgumentException("CSV file path does not exist or is not a file: " + filePath);
                }
            }

            // Check for duplicate joinKeys and reserved timestamp joinKey
            for (CsvColumn column : csvSettings.getColumns()) {
                String joinKey = column.getJoinKey();
                if (joinKey != null) {
                    if (joinKey.equalsIgnoreCase(RESERVED_TIMESTAMP_JOINKEY)) {
                        throw new IllegalArgumentException(
                                "The joinKey 'timestamp' is reserved. Please use a different joinKey.");
                    }
                    if (!seenJoinKeys.add(joinKey)) {
                        throw new IllegalArgumentException("Duplicate joinKey found in CSV settings: " + joinKey);
                    }
                }
            }
        }
    }

    /**
     * Validates the IoTDB settings within the migration configuration.
     *
     * @param config the migration configuration containing IoTDB settings
     * @throws IllegalArgumentException if any IoTDB setting is invalid
     */
    private static void validateIoTDBSettings(MigrationConfig config) {
        Set<String> csvJoinKeys = config.getCsvSettings().stream()
                .flatMap(csvSetting -> csvSetting.getColumns().stream())
                .map(CsvColumn::getJoinKey)
                .collect(Collectors.toSet());

        Set<String> usedPathColumns = config.getIotdbSettings().getDevices().stream()
                .map(IoTDBDevice::getPathColumn)
                .collect(Collectors.toSet());

        Set<String> usedJoinKeys = config.getIotdbSettings().getDevices().stream()
                .flatMap(device -> device.getMeasurements().stream())
                .map(IoTDBMeasurement::getJoinKey)
                .collect(Collectors.toSet());

        // Add pathColumns to usedJoinKeys
        usedJoinKeys.addAll(usedPathColumns);

        for (CsvSettings csvSettings : config.getCsvSettings()) {
            List<CsvColumn> pathColumns = csvSettings.getColumns().stream()
                    .filter(CsvColumn::isPathColumn)
                    .collect(Collectors.toList());

            for (CsvColumn pathColumn : pathColumns) {
                if (!usedPathColumns.contains(pathColumn.getJoinKey())) {
                    throw new IllegalArgumentException(
                            "CSV path column is not used in any device configuration: " + pathColumn.getJoinKey());
                }
            }
        }

        for (IoTDBDevice device : config.getIotdbSettings().getDevices()) {
            for (IoTDBMeasurement measurement : device.getMeasurements()) {
                if (!csvJoinKeys.contains(measurement.getJoinKey())) {
                    throw new IllegalArgumentException(
                            "Measurement joinKey does not match any CSV joinKey: " + measurement.getJoinKey());
                }
            }
        }

        // Check for unused joinKeys in CSV settings
        for (String joinKey : csvJoinKeys) {
            if (!usedJoinKeys.contains(joinKey)) {
                throw new IllegalArgumentException("Unused joinKey found in CSV settings: " + joinKey);
            }
        }

        validateDataTypeConversions(config);
    }

    private static void validateDataTypeConversions(MigrationConfig config) {
        for (IoTDBDevice device : config.getIotdbSettings().getDevices()) {
            for (IoTDBMeasurement measurement : device.getMeasurements()) {
                String joinKey = measurement.getJoinKey();
                CsvColumn csvColumn = findCsvColumnByJoinKey(config.getCsvSettings(), joinKey);
                if (csvColumn == null) {
                    throw new IllegalArgumentException("No CSV column found for joinKey: " + joinKey);
                }
                if (!isValidConversion(csvColumn.getType().toString(), measurement.getDataType())) {
                    throw new IllegalArgumentException(
                            String.format("Invalid conversion from CSV type %s to IoTDB type %s for joinKey %s",
                                    csvColumn.getType(), measurement.getDataType(), joinKey));
                }
            }
        }
    }

    private static CsvColumn findCsvColumnByJoinKey(List<CsvSettings> csvSettingsList, String joinKey) {
        for (CsvSettings csvSettings : csvSettingsList) {
            for (CsvColumn column : csvSettings.getColumns()) {
                if (joinKey.equals(column.getJoinKey())) {
                    return column;
                }
            }
        }
        return null;
    }

    private static boolean isValidConversion(String csvType, TSDataType iotdbType) {
        return switch (csvType.toUpperCase()) {
            case "DOUBLE" -> iotdbType == TSDataType.DOUBLE || iotdbType == TSDataType.FLOAT
                    || iotdbType == TSDataType.INT32 || iotdbType == TSDataType.INT64 || iotdbType == TSDataType.TEXT;
            case "FLOAT" -> iotdbType == TSDataType.DOUBLE || iotdbType == TSDataType.FLOAT
                    || iotdbType == TSDataType.INT32 || iotdbType == TSDataType.INT64 || iotdbType == TSDataType.TEXT;
            case "INTEGER" -> iotdbType == TSDataType.INT32 || iotdbType == TSDataType.INT64
                    || iotdbType == TSDataType.FLOAT || iotdbType == TSDataType.DOUBLE || iotdbType == TSDataType.TEXT;
            case "LONG" -> iotdbType == TSDataType.INT32 || iotdbType == TSDataType.INT64
                    || iotdbType == TSDataType.FLOAT || iotdbType == TSDataType.DOUBLE || iotdbType == TSDataType.TEXT;
            case "BOOLEAN" -> iotdbType == TSDataType.BOOLEAN || iotdbType == TSDataType.INT32
                    || iotdbType == TSDataType.INT64 || iotdbType == TSDataType.TEXT;
            case "TIME" -> iotdbType == TSDataType.INT64 || iotdbType == TSDataType.TEXT;
            case "STRING" -> true; // Allow conversion from STRING to any type, with appropriate parsing in Converter
            default -> false;
        };
    }
}