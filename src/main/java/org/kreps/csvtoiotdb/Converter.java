package org.kreps.csvtoiotdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBDevice;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBMeasurement;
import org.kreps.csvtoiotdb.converter.RowData;

/**
 * Converts parsed CSV rows into a format suitable for IoTDB insertion.
 */
public class Converter {
    private final List<IoTDBDevice> ioTDBSettingsList;

    /**
     * Constructs a Converter instance.
     *
     * @param ioTDBSettingsList The list of IoTDB settings to be used for
     *                          conversion.
     */
    public Converter(List<IoTDBDevice> ioTDBSettingsList) {
        this.ioTDBSettingsList = ioTDBSettingsList;
    }

    /**
     * Converts a batch of parsed CSV rows into a map grouped by device ID.
     *
     * @param rows The list of parsed CSV rows.
     * @return A map where the key is the device ID and the value is a list of
     *         RowData.
     */
    public Map<String, List<RowData>> convert(List<Map<String, Object>> rows) {
        Map<String, List<RowData>> deviceDataMap = new HashMap<>();

        for (Map<String, Object> row : rows) {
            // Extract timestamps
            Long timestamp = (Long) row.get("timestamp");

            for (IoTDBDevice ioTDBSettings : this.ioTDBSettingsList) {
                boolean match = ioTDBSettings.getMeasurements().stream()
                        .allMatch(m -> row.containsKey(m.getJoinKey()));

                if (match) {
                    Map<String, Object> measurements = new HashMap<>();
                    for (IoTDBMeasurement measurement : ioTDBSettings.getMeasurements()) {
                        Object value = row.get(measurement.getJoinKey());
                        if (value != null) {
                            Object convertedValue = convertType(value, measurement.getDataType());
                            measurements.put(measurement.getName(), convertedValue);
                        }
                    }
                    RowData rowData = new RowData(timestamp, measurements);
                    deviceDataMap.computeIfAbsent(ioTDBSettings.getDeviceId(), k -> new ArrayList<>()).add(rowData);
                }
            }
        }

        return deviceDataMap;
    }

    /**
     * Converts a value to the target IoTDB data type.
     *
     * @param value      The original value.
     * @param targetType The target IoTDB data type.
     * @return The converted value.
     */
    private Object convertType(Object value, TSDataType targetType) {
        if (value == null)
            return null;

        switch (targetType) {
            case INT32 -> {
                return ((Number) value).intValue();
            }
            case INT64 -> {
                return ((Number) value).longValue();
            }
            case FLOAT -> {
                return ((Number) value).floatValue();
            }
            case DOUBLE -> {
                return ((Number) value).doubleValue();
            }
            case BOOLEAN -> {
                return value;
            }
            case TEXT -> {
                return value.toString();
            }
            default -> throw new IllegalArgumentException("Unsupported IoTDB data type: " + targetType);
        }
    }
}
