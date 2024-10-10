package org.kreps.csvtoiotdb.configs.iotdb;

import java.util.List;

public class IoTDBDevice {
    private String deviceId;
    private List<IoTDBMeasurement> measurements;
    private String pathColumn;
    private Boolean isAlignedTimeseries;

    public IoTDBDevice() {
    }

    public IoTDBDevice(String deviceId, List<IoTDBMeasurement> measurements, String pathColumn,
            Boolean isAlignedTimeseries) {
        this.deviceId = deviceId;
        this.measurements = measurements;
        this.pathColumn = pathColumn;
        this.isAlignedTimeseries = isAlignedTimeseries;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public List<IoTDBMeasurement> getMeasurements() {
        return measurements;
    }

    public void setMeasurements(List<IoTDBMeasurement> measurements) {
        this.measurements = measurements;
    }

    public String getPathColumn() {
        return pathColumn;
    }

    public void setPathColumn(String pathColumn) {
        this.pathColumn = pathColumn;
    }

    public Boolean getIsAlignedTimeseries() {
        return isAlignedTimeseries;
    }

    public void setIsAlignedTimeseries(Boolean isAlignedTimeseries) {
        this.isAlignedTimeseries = isAlignedTimeseries;
    }
}
