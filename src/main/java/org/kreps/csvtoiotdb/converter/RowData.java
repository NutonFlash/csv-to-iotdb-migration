package org.kreps.csvtoiotdb.converter;

import java.util.Map;

/**
 * Represents a single row of data to be inserted into IoTDB.
 */
public class RowData {
    private final String rowId;
    private final int rowNumber;
    private final long timestamp;
    private final Map<String, Object> measurements;

    /**
     * Constructs a RowData instance.
     *
     * @param rowId      The unique identifier for the data row.
     * @param rowNumber  The row number for the data row.
     * @param timestamp  The timestamp for the data row.
     * @param measurements A map of measurement names to their corresponding values.
     */
    public RowData(String rowId, int rowNumber, long timestamp, Map<String, Object> measurements) {
        this.rowId = rowId;
        this.rowNumber = rowNumber;
        this.timestamp = timestamp;
        this.measurements = measurements;
    }

    /**
     * Gets the unique identifier for the data row.
     *
     * @return The row ID.
     */
    public String getRowId() {
        return rowId;
    }

    /**
     * Gets the row number of the data row.
     *
     * @return The row number.
     */
    public int getRowNumber() {
        return rowNumber;
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
