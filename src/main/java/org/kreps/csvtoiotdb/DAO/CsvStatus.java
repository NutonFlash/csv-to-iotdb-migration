package org.kreps.csvtoiotdb.DAO;

import java.util.Arrays;

public enum CsvStatus {
    PENDING("PENDING"),
    IN_PROGRESS("IN_PROGRESS"),
    COMPLETED("COMPLETED"),
    FAILED("FAILED");

    private final String value;

    CsvStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static CsvStatus fromString(String text) {
        return Arrays.stream(CsvStatus.values())
                .filter(status -> status.value.equalsIgnoreCase(text))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No constant with text " + text + " found"));
    }

    @Override
    public String toString() {
        return this.value;
    }
}
