package org.kreps.csvtoiotdb.DAO;

import java.util.Arrays;

public enum RowProcessingStatus {
    PENDING("PENDING"),
    PROCESSING("PROCESSING"),
    COMPLETED("COMPLETED"),
    FAILED("FAILED"),
    RETRY("RETRY");

    private final String value;

    RowProcessingStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static RowProcessingStatus fromString(String text) {
        return Arrays.stream(RowProcessingStatus.values())
                .filter(status -> status.value.equalsIgnoreCase(text))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No constant with text " + text + " found"));
    }

    @Override
    public String toString() {
        return this.value;
    }
}