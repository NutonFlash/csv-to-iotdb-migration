package org.kreps.csvtoiotdb;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.kreps.csvtoiotdb.configs.MigrationConfig;
import org.kreps.csvtoiotdb.configs.csv.CsvSettings;

/**
 * The main entry point for the CSV to IoTDB migration tool.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        // Load configurations using ConfigLoader
        ConfigLoader configLoader = new ConfigLoader("./config.json");
        MigrationConfig config = configLoader.getConfig();

        IoTDBClientManager clientManager = new IoTDBClientManager(config.getIotdbSettings());

        IoTDBSchemaValidator schemaValidator = new IoTDBSchemaValidator(clientManager);
        schemaValidator.validateAndCreateTimeseriesForDevices(config.getIotdbSettings().getDevices());

        ThreadManager threadManager = new ThreadManager(config.getMigrationSettings().getThreadsNumber());

        Converter converter = new Converter(config.getIotdbSettings().getDevices());

        IoTDBWriter writer = new IoTDBWriter(
                clientManager,
                config.getIotdbSettings().getMaxRetriesCount(),
                config.getIotdbSettings().getRetryIntervalInMs());

        BlockingQueue<CsvSettings> csvSettingsQueue = new LinkedBlockingQueue<>(config.getCsvSettings());

        // Submit migration tasks
        for (int i = 0; i < config.getMigrationSettings().getThreadsNumber(); i++) {
            threadManager.submitTask(
                    new MigrateTask(csvSettingsQueue, config.getIotdbSettings().getDevices(), converter, writer,
                            config.getMigrationSettings().getBatchSize()));
        }

        // Shutdown the ThreadManager after all tasks are completed
        threadManager.shutdown();

        // Close the IoTDBClientManager
        clientManager.close();

        System.out.println("CSV to IoTDB migration completed.");
    }
}
