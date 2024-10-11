package org.kreps.csvtoiotdb.DAO;

import java.util.Arrays;

public enum LogLevel {
    INFO("INFO"),
    WARNING("WARNING"),
    ERROR("ERROR");

    private final String value;

    LogLevel(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static LogLevel fromString(String text) {
        return Arrays.stream(LogLevel.values())
                .filter(level -> level.value.equalsIgnoreCase(text))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No constant with text " + text + " found"));
    }

    @Override
    public String toString() {
        return this.value;
    }
}
