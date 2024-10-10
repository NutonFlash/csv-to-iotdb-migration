package org.kreps.csvtoiotdb.configs.csv;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

public class CsvColumn {

    private String name;
    private CsvDataType type;
    private String joinKey;
    private TimeFormatType timeFormatType;
    private String customTimeFormat;
    private boolean isPathColumn;

    // Supported CsvDataType values for validation
    private static final CsvDataType[] SUPPORTED_DATA_TYPES = {
            CsvDataType.INTEGER, CsvDataType.LONG, CsvDataType.FLOAT, CsvDataType.DOUBLE,
            CsvDataType.BOOLEAN, CsvDataType.STRING, CsvDataType.TIME
    };

    public CsvColumn() {
    }

    public CsvColumn(String name, CsvDataType type, String joinKey, TimeFormatType timeFormatType,
            String customTimeFormat, boolean isPathColumn) {

        this.name = Objects.requireNonNull(name, "Column name cannot be null");

        if (!isValidCsvDataType(type)) {
            throw new IllegalArgumentException("Invalid data type: " + type + ". Expected one of: "
                    + getSupportedDataTypes());
        }
        this.type = Objects.requireNonNull(type, "Data type cannot be null");

        this.joinKey = joinKey;
        this.timeFormatType = timeFormatType;

        if (this.type == CsvDataType.TIME) {
            validateTimeFormat();
        }

        this.isPathColumn = Boolean.TRUE.equals(isPathColumn);
    }

    private boolean isValidCsvDataType(CsvDataType type) {
        for (CsvDataType supportedType : SUPPORTED_DATA_TYPES) {
            if (supportedType == type) {
                return true;
            }
        }
        return false;
    }

    private String getSupportedDataTypes() {
        StringBuilder sb = new StringBuilder();
        for (CsvDataType dataType : SUPPORTED_DATA_TYPES) {
            sb.append(dataType.name()).append(", ");
        }
        return sb.substring(0, sb.length() - 2); // Remove last comma and space
    }

    private void validateTimeFormat() {
        if (this.timeFormatType == TimeFormatType.CUSTOM) {
            if (this.customTimeFormat == null || this.customTimeFormat.isEmpty()) {
                throw new IllegalArgumentException("Custom time format must be provided for CUSTOM parseTimeExpr.");
            }
            try {
                DateTimeFormatter.ofPattern(this.customTimeFormat); // Check if valid DateTimeFormatter pattern
            } catch (IllegalArgumentException | DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid custom time format pattern: " + this.customTimeFormat);
            }
        }
    }

    public Object parseValue(String value) {
        try {
            switch (this.type) {
                case INTEGER -> {
                    return Integer.valueOf(value);
                }
                case LONG -> {
                    return Long.valueOf(value);
                }
                case FLOAT -> {
                    return Float.valueOf(value);
                }
                case DOUBLE -> {
                    return Double.valueOf(value);
                }
                case BOOLEAN -> {
                    return Boolean.valueOf(value);
                }
                case STRING -> {
                    return value;
                }
                case TIME -> {
                    return parseTime(value);
                }
                default -> throw new IllegalArgumentException("Unsupported type: " + type);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to parse value '%s' as type '%s'. Please ensure the value is correctly formatted.",
                            value, type),
                    e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(
                    String.format("Error occurred while parsing value '%s' for type '%s'.", value, type), e);
        }
    }

    private long parseTime(String value) {
        try {
            switch (this.timeFormatType) {
                case UNIX -> {
                    return Long.parseLong(value) * 1000; // Assume UNIX timestamp in seconds
                }
                case ISO -> {
                    return ZonedDateTime.parse(value, DateTimeFormatter.ISO_DATE_TIME)
                            .toInstant()
                            .toEpochMilli();
                }
                case CUSTOM -> {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(this.customTimeFormat);
                    LocalDateTime localDateTime = LocalDateTime.parse(value, formatter);
                    return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                }
                default -> throw new IllegalArgumentException("Unsupported time format type: " + timeFormatType);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to parse time value '%s' as a UNIX timestamp.", value), e);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to parse time value '%s' using format '%s'.", value, this.timeFormatType), e);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format("Error parsing time value '%s': unsupported time format '%s'.", value,
                            this.timeFormatType),
                    e);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Unexpected error occurred while parsing time value '%s'.", value), e);
        }
    }

    public boolean isPathColumn() {
        return isPathColumn;
    }

    public void setIsPathColumn(boolean isPathColumn) {
        this.isPathColumn = isPathColumn;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public CsvDataType getType() {
        return type;
    }

    public void setType(CsvDataType type) {
        this.type = type;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public String getCustomTimeFormat() {
        return customTimeFormat;
    }

    public void setCustomTimeFormat(String customTimeFormat) {
        this.customTimeFormat = customTimeFormat;
    }

    public TimeFormatType getTimeFormatType() {
        return timeFormatType;
    }

    public void setTimeFormatType(TimeFormatType timeFormatType) {
        this.timeFormatType = timeFormatType;
    }
}