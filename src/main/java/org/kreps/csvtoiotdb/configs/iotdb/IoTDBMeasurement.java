package org.kreps.csvtoiotdb.configs.iotdb;

import java.util.EnumSet;
import java.util.Objects;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IoTDBMeasurement {
    private String name;
    private TSDataType dataType;
    private String joinKey;
    private TSEncoding encoding;
    private CompressionType compression;

    public IoTDBMeasurement() {
    }

    public IoTDBMeasurement(String name, String dataType, String joinKey) {
        this(name, dataType, joinKey, "RLE", "SNAPPY");
    }

    @JsonCreator
    public IoTDBMeasurement(@JsonProperty("name") String name, @JsonProperty("dataType") String dataType,
            @JsonProperty("joinKey") String joinKey, @JsonProperty("encoding") String encoding,
            @JsonProperty("compression") String compression) {

        this.name = Objects.requireNonNull(name, "Measurement name cannot be null");

        try {
            this.dataType = TSDataType.valueOf(Objects.requireNonNull(dataType, "Data type cannot be null"));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid dataType '%s' provided for measurement '%s'. Expected one of: %s",
                            dataType, name, getEnumValues(TSDataType.class)));
        }

        this.joinKey = joinKey;

        try {
            this.encoding = (encoding == null) ? TSEncoding.RLE : TSEncoding.valueOf(encoding);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid encoding '%s' provided for measurement '%s'. Expected one of: %s",
                            encoding, name, getEnumValues(TSEncoding.class)));
        }

        try {
            this.compression = (compression == null) ? CompressionType.SNAPPY : CompressionType.valueOf(compression);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid compression '%s' provided for measurement '%s'. Expected one of: %s",
                            compression, name, getEnumValues(CompressionType.class)));
        }
    }

    private <T extends Enum<T>> String getEnumValues(Class<T> enumClass) {
        return String.join(", ", EnumSet.allOf(enumClass).stream().map(Enum::name).toArray(String[]::new));
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public TSEncoding getEncoding() {
        return encoding;
    }

    public void setEncoding(TSEncoding encoding) {
        this.encoding = encoding;
    }

    public CompressionType getCompression() {
        return compression;
    }

    public void setCompression(CompressionType compression) {
        this.compression = compression;
    }

}
