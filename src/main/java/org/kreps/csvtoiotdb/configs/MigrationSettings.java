package org.kreps.csvtoiotdb.configs;

public class MigrationSettings {
    private int threadsNumber;
    private int batchSize;

    public MigrationSettings() {
    }

    public MigrationSettings(int threadsNumber, int batchSize, int maxRetries, long retryBackoffMillis) {
        this.threadsNumber = threadsNumber;
        this.batchSize = batchSize;
    }

    public int getThreadsNumber() {
        return threadsNumber;
    }

    public void setThreadsNumber(int threadsNumber) {
        this.threadsNumber = threadsNumber;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
