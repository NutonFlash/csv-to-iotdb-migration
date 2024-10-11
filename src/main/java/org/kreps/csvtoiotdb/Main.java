package org.kreps.csvtoiotdb;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.kreps.csvtoiotdb.configs.MigrationConfig;
import org.kreps.csvtoiotdb.configs.csv.CsvSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        try {
            ConfigLoader configLoader = new ConfigLoader("./config.json");
            MigrationConfig config = configLoader.getConfig();

            // Validate configuration
            ConfigValidator.validateConfig(config);
            logger.info("Configuration validation passed.");

            // Initialize H2 database with CSV settings
            H2DatabaseManager.initialize(config.getH2Config());

            MigrationInitializer initializer = new MigrationInitializer(H2DatabaseManager.getInstance());
            initializer.initialize(config.getCsvSettings());
            logger.info("Migration state initialized.");

            // Initialize other components
            IoTDBClientManager clientManager = new IoTDBClientManager(config.getIotdbSettings());
            IoTDBSchemaValidator schemaValidator = new IoTDBSchemaValidator(clientManager);
            schemaValidator.validateAndCreateTimeseriesForDevices(config.getIotdbSettings().getDevices());
            logger.info("Schema validation completed.");

            ThreadManager threadManager = new ThreadManager(config.getMigrationSettings().getThreadsNumber());

            Converter converter = new Converter(config.getIotdbSettings(), config.getCsvSettings(),
                    H2DatabaseManager.getInstance());

            IoTDBWriter writer = new IoTDBWriter(
                    clientManager,
                    schemaValidator,
                    config.getIotdbSettings().getDevices(),
                    config.getIotdbSettings().getMaxRetries(),
                    config.getIotdbSettings().getRetryInterval(),
                    config.getIotdbSettings().getMaxBackoffTime(),
                    H2DatabaseManager.getInstance());
            logger.info("IoTDBWriter initialized.");

            BlockingQueue<CsvSettings> csvSettingsQueue = new LinkedBlockingQueue<>(config.getCsvSettings());

            // Submit migration tasks
            for (int i = 0; i < config.getMigrationSettings().getThreadsNumber(); i++) {
                threadManager.submitTask(
                        new MigrateTask(csvSettingsQueue, converter, writer,
                                config.getMigrationSettings().getBatchSize(),
                                H2DatabaseManager.getInstance()));
                logger.info("Submitted migration task {}", i + 1);
            }

            // Shutdown the ThreadManager after all tasks are completed
            threadManager.shutdown();
            logger.info("ThreadManager shutdown initiated.");

            // Close the IoTDBClientManager
            clientManager.close();
            logger.info("IoTDBClientManager closed.");

            logger.info("CSV to IoTDB migration completed successfully.");
        } catch (Exception e) {
            logger.error("Migration failed: {}", e.getMessage(), e);
            throw e; // Propagate exception to display to the user
        } finally {
            // Ensure the H2 Console is stopped when the application exits
            H2DatabaseManager.getInstance().shutdown();
        }
    }
}