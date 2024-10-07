package org.kreps.csvtoiotdb.configs.iotdb;

import java.util.List;

public class IoTDBDevice {
    private String deviceId;
    private List<IoTDBMeasurement> measurements;

    public IoTDBDevice() {
    }

    public IoTDBDevice(String deviceId, List<IoTDBMeasurement> measurements) {
        this.deviceId = deviceId;
        this.measurements = measurements;
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

}
