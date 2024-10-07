package org.kreps.csvtoiotdb.converter;

import java.util.Map;

/**
 * Represents a single row of data to be inserted into IoTDB.
 */
public class RowData {
    private final long timestamp;
    private final Map<String, Object> measurements;

    /**
     * Constructs a RowData instance.
     *
     * @param timestamp    The timestamp for the data row.
     * @param measurements A map of measurement names to their corresponding values.
     */
    public RowData(long timestamp, Map<String, Object> measurements) {
        this.timestamp = timestamp;
        this.measurements = measurements;
    }

    /**
     * Gets the timestamp of the data row.
     *
     * @return The timestamp.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Gets the measurements of the data row.
     *
     * @return A map of measurement names to values.
     */
    public Map<String, Object> getMeasurements() {
        return measurements;
    }
}
