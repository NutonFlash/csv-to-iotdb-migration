package org.kreps.csvtoiotdb.configs.iotdb;

import java.util.List;

public class IoTDBSettings {
    private int connectionPoolSize;
    private int maxRetriesCount;
    private long retryIntervalInMs;
    private List<IoTDBConnection> connections;
    private List<IoTDBDevice> devices;

    public IoTDBSettings() {

    }

    public IoTDBSettings(int connectionPoolSize, List<IoTDBConnection> connections, List<IoTDBDevice> devices,
            int maxRetriesCount, long retryIntervalInMs) {
        this.connectionPoolSize = connectionPoolSize;
        this.connections = connections;
        this.devices = devices;
        this.maxRetriesCount = maxRetriesCount;
        this.retryIntervalInMs = retryIntervalInMs;
    }

    public List<IoTDBConnection> getConnections() {
        return connections;
    }

    public void setConnections(List<IoTDBConnection> connections) {
        this.connections = connections;
    }

    public List<IoTDBDevice> getDevices() {
        return devices;
    }

    public void setDevices(List<IoTDBDevice> devices) {
        this.devices = devices;
    }

    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    public void setConnectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
    }

    public int getMaxRetriesCount() {
        return maxRetriesCount;
    }

    public void setMaxRetriesCount(int maxRetriesCount) {
        this.maxRetriesCount = maxRetriesCount;
    }

    public long getRetryIntervalInMs() {
        return retryIntervalInMs;
    }

    public void setRetryIntervalInMs(long retryIntervalInMs) {
        this.retryIntervalInMs = retryIntervalInMs;
    }

}
