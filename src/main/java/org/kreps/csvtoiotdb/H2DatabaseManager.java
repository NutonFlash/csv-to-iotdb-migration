package org.kreps.csvtoiotdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.Server;
import org.kreps.csvtoiotdb.configs.H2Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H2DatabaseManager {
    private static final Logger logger = LoggerFactory.getLogger(H2DatabaseManager.class);
    private static H2DatabaseManager instance;
    private static H2Config staticConfig;
    private final H2Config config;
    private Server webServer;

    private H2DatabaseManager(H2Config config) {
        this.config = config;
        initializeDatabase();
        if (config.isEnableConsole()) {
            startWebConsole();
        }
    }

    public static synchronized void initialize(H2Config config) {
        if (instance == null) {
            staticConfig = config;
            instance = new H2DatabaseManager(config);
        } else {
            throw new IllegalStateException("H2DatabaseManager is already initialized");
        }
    }

    public static synchronized H2DatabaseManager getInstance() {
        if (instance == null) {
            throw new IllegalStateException("H2DatabaseManager is not initialized. Call initialize(H2Config) first.");
        }
        return instance;
    }

    private void initializeDatabase() {
        String[] createTableStatements = {
                "CREATE TABLE IF NOT EXISTS csv_settings (" +
                        "id IDENTITY PRIMARY KEY, " +
                        "file_path VARCHAR(255) UNIQUE NOT NULL, " +
                        "status VARCHAR(50) NOT NULL, " +
                        "total_rows INT DEFAULT 0, " +
                        "processed_rows INT DEFAULT 0, " +
                        "failed_rows INT DEFAULT 0, " +
                        "last_processed_timestamp TIMESTAMP, " +
                        "error_message VARCHAR(1024)" +
                        ")",
                "CREATE TABLE IF NOT EXISTS migration_logs (" +
                        "id IDENTITY PRIMARY KEY, " +
                        "csv_setting_id BIGINT, " +
                        "log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                        "log_level VARCHAR(10), " +
                        "message VARCHAR(2048), " +
                        "FOREIGN KEY (csv_setting_id) REFERENCES csv_settings(id)" +
                        ")",
                "CREATE TABLE IF NOT EXISTS row_processing (" +
                        "id IDENTITY PRIMARY KEY, " +
                        "csv_setting_id BIGINT, " +
                        "row_number INT, " +
                        "status VARCHAR(50), " +
                        "error_message VARCHAR(1024), " +
                        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                        "retry_count INT DEFAULT 0, " +
                        "FOREIGN KEY (csv_setting_id) REFERENCES csv_settings(id)" +
                        ")",
                "CREATE TABLE IF NOT EXISTS jobs (" +
                        "id IDENTITY PRIMARY KEY, " +
                        "file_path VARCHAR(255) UNIQUE NOT NULL, " +
                        "status VARCHAR(50) NOT NULL, " +
                        "start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                        "end_time TIMESTAMP, " +
                        "error_message VARCHAR(1024)" +
                        ")"
        };

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            for (String sql : createTableStatements) {
                stmt.execute(sql);
            }
            logger.info("Database tables initialized successfully.");
        } catch (SQLException e) {
            logger.error("Error initializing database tables: {}", e.getMessage(), e);
        }
    }

    private void startWebConsole() {
        try {
            webServer = Server.createWebServer("-webPort", String.valueOf(config.getConsolePort()), "-webAllowOthers")
                    .start();
            logger.info("H2 Console started on port: {}", config.getConsolePort());
        } catch (SQLException e) {
            logger.error("Failed to start H2 Console: {}", e.getMessage(), e);
        }
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
    }

    public void shutdown() {
        if (webServer != null) {
            webServer.stop();
            logger.info("H2 Console stopped.");
        }
    }
}