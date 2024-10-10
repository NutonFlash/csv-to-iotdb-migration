package org.kreps.csvtoiotdb.configs.iotdb;

import java.util.List;

public class IoTDBSettings {
    private int connectionPoolSize;
    private int maxRetries;
    private long retryInterval;
    private List<IoTDBConnection> connections;
    private List<IoTDBDevice> devices;
    private long maxBackoffTime;

    public IoTDBSettings() {

    }

    public IoTDBSettings(int connectionPoolSize, List<IoTDBConnection> connections, List<IoTDBDevice> devices,
            int maxRetries, long retryInterval, long maxBackoffTime) {
        this.connectionPoolSize = connectionPoolSize;
        this.connections = connections;
        this.devices = devices;
        this.maxRetries = maxRetries;
        this.retryInterval = retryInterval;
        this.maxBackoffTime = maxBackoffTime;
    }

    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    public void setConnectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
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

    public long getMaxBackoffTime() {
        return maxBackoffTime;
    }

    public void setMaxBackoffTime(long maxBackoffTime) {
        this.maxBackoffTime = maxBackoffTime;
    }

}
