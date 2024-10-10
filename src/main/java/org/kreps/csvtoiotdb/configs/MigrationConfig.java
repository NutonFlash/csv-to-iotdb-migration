package org.kreps.csvtoiotdb.configs;

import java.util.List;

import org.kreps.csvtoiotdb.configs.csv.CsvSettings;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBSettings;

public class MigrationConfig {
    private List<CsvSettings> csvSettings;
    private IoTDBSettings iotdbSettings;
    private MigrationSettings migrationSettings;
    private H2Config h2Config;

    public MigrationConfig() {
    }

    public MigrationConfig(List<CsvSettings> csvSettings, IoTDBSettings iotdbSettings,
            MigrationSettings migrationSettings) {
        this.csvSettings = csvSettings;
        this.iotdbSettings = iotdbSettings;
        this.migrationSettings = migrationSettings;
    }

    public IoTDBSettings getIotdbSettings() {
        return iotdbSettings;
    }

    public void setIotdbSettings(IoTDBSettings iotdbSettings) {
        this.iotdbSettings = iotdbSettings;
    }

    public MigrationSettings getMigrationSettings() {
        return migrationSettings;
    }

    public void setMigrationSettings(MigrationSettings migrationSettings) {
        this.migrationSettings = migrationSettings;
    }

    public List<CsvSettings> getCsvSettings() {
        return csvSettings;
    }

    public void setCsvSettings(List<CsvSettings> csvSettings) {
        this.csvSettings = csvSettings;
    }

    public H2Config getH2Config() {
        return h2Config;
    }

    public void setH2Config(H2Config h2Config) {
        this.h2Config = h2Config;
    }
}
