# CSV to IoTDB Migration Tool

A robust and efficient tool designed to migrate data from CSV files to [Apache IoTDB](https://iotdb.apache.org/). This application ensures seamless data transfer, schema validation, and job management using an embedded H2 database.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Configuration](#configuration)
  - [Configuration File Structure](#configuration-file-structure)
    - [Configuration Fields](#configuration-fields)
      - [csvSettings](#csvsettings)
        - [timestampColumn](#timestampcolumn)
        - [columns](#columns)
        - [filePaths](#filepaths)
        - [delimiter](#delimiter)
        - [escapeCharacter](#escapecharacter)
      - [iotdbSettings](#iotdbsettings)
        - [connectionPoolSize](#connectionpoolsizes)
        - [maxRetries](#maxretries)
        - [retryInterval](#retryinterval)
        - [maxBackoffTime](#maxbackofftime)
        - [connections](#connections)
        - [devices](#devices)
          - [measurements](#measurements)
      - [h2Config](#h2config)
        - [url](#url)
        - [username](#username)
        - [password](#password)
        - [enableConsole](#enableconsole)
        - [consolePort](#consoleport)
      - [migrationSettings](#migrationsettings)
        - [threadsNumber](#threadsnumber)
        - [batchSize](#batchsize)
  - [Configuration Validation](#configuration-validation)
    - [Valid Data Types](#valid-data-types)
    - [Join Key Constraints](#join-key-constraints)
    - [Data Type Conversions](#data-type-conversions)
    - [Path Column Usage](#path-column-usage)
- [Usage](#usage)
- [Modules](#modules)
- [Database Management](#database-management)
  - [H2 Database](#h2-database)
  - [IoTDB Schema Validation](#iotdb-schema-validation)
- [Logging](#logging)
- [Error Handling](#error-handling)

## Features

- **Seamless Data Migration:** Efficiently transfers data from CSV files to IoTDB with support for large datasets.
- **Schema Validation:** Automatically validates and creates IoTDB timeseries if they do not exist.
- **Job Management:** Tracks the state of migration jobs using an embedded H2 database, ensuring reliability and consistency.
- **Retry Mechanism:** Handles failed rows by tracking and reprocessing them upon application restart.
- **Configurable Settings:** Highly customizable through a JSON configuration file to suit various migration needs.
- **Web Console:** Offers an H2 Database web console for real-time monitoring and management.

## Architecture

The application follows a modular architecture comprising the following key components:

- **Main:** Entry point of the application that initializes configurations, database connections, and migration tasks.
- **MigrationInitializer:** Prepares the migration state by initializing or updating CSV settings and jobs in the H2 database.
- **MigrateTask:** Handles the processing of CSV files, conversion of data, and writing to IoTDB.
- **Converter:** Transforms CSV data into a format suitable for IoTDB.
- **IoTDBWriter:** Manages the insertion of data into IoTDB, ensuring data integrity and handling retries.
- **H2DatabaseManager:** Manages the embedded H2 database for tracking migration jobs and states.
- **DAO Layer:** Data Access Objects (DAOs) for interacting with the H2 database tables.

## Configuration

### Configuration File Structure

The application uses a `config.json` file to manage its settings. Below is the structure and detailed explanation of each configuration parameter:

```json
{
  "csvSettings": [
    {
      "timestampColumn": {
        "name": "Timestamp",
        "type": "TIME",
        "timeFormatType": "UNIX"
      },
      "columns": [
        {
          "name": "CreatedTime",
          "type": "TIME",
          "joinKey": "createdTime",
          "timeFormatType": "CUSTOM",
          "customTimeFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
          "isPathColumn": false
        },
        {
          "name": "Tag",
          "type": "STRING",
          "joinKey": "sensorTag",
          "isPathColumn": true
        },
        {
          "name": "Value",
          "type": "DOUBLE",
          "joinKey": "sensorValue"
        }
      ],
      "filePaths": ["./tests/standard_test.csv"],
      "delimiter": ",",
      "escapeCharacter": "\""
    }
  ],
  "iotdbSettings": {
    "connectionPoolSize": 10,
    "maxRetries": 5,
    "retryInterval": 1500,
    "maxBackoffTime": 10000,
    "connections": [
      {
        "host": "192.168.0.202",
        "port": 6667,
        "username": "root",
        "password": "root"
      }
    ],
    "devices": [
      {
        "deviceId": "root.powerplant",
        "pathColumn": "sensorTag",
        "isAlignedTimeseries": true,
        "measurements": [
          {
            "name": "sensorValue",
            "dataType": "DOUBLE",
            "joinKey": "sensorValue",
            "encoding": "RLE",
            "compression": "SNAPPY"
          },
          {
            "name": "createdTime",
            "dataType": "TEXT",
            "joinKey": "createdTime",
            "encoding": "PLAIN",
            "compression": "SNAPPY"
          }
        ]
      }
    ]
  },
  "h2Config": {
    "url": "jdbc:h2:./migrationDB;AUTO_SERVER=TRUE",
    "username": "sa",
    "password": "",
    "enableConsole": true,
    "consolePort": 8082
  },
  "migrationSettings": {
    "threadsNumber": 1,
    "batchSize": 1000
  }
}
```

#### Configuration Fields

##### `csvSettings`

Defines settings for CSV file parsing. Multiple CSV settings can be specified if migrating data from different CSV files with varying schemas.

- **timestampColumn:** Specifies the column in the CSV that contains timestamp data.
  - **name** (`String`): Name of the timestamp column in the CSV.
  - **type** (`String`): Data type of the timestamp column. Must be `"TIME"`.
  - **timeFormatType** (`String`): Format of the timestamp. Valid values:
    - `"UNIX"`: Unix epoch format.
    - `"ISO"`: ISO 8601 format.
    - `"CUSTOM"`: Custom-defined format (requires additional parsing logic).

- **columns:** List of columns to be processed from the CSV.
  - **name** (`String`): Name of the column in the CSV.
  - **type** (`String`): Data type of the column. Valid values:
    - `"STRING"`
    - `"DOUBLE"`
    - `"FLOAT"`
    - `"INTEGER"`
    - `"LONG"`
    - `"BOOLEAN"`
    - `"TIME"`
  - **joinKey** (`String`): Key used to map CSV columns to IoTDB measurements. Must be unique across all CSV settings and measurements.
  - **isPathColumn** (`Boolean`, optional): Indicates if the column is used for IoTDB path segmentation. Defaults to `false` if omitted.
  - **timeFormatType** (`String`, optional): Required if the column type is `"TIME"`. Defines the format of the time value. Valid values:
    - `"UNIX"`
    - `"ISO"`
    - `"CUSTOM"`
  - **customTimeFormat** (`String`, optional): Required if `timeFormatType` is `"CUSTOM"`. Defines the custom pattern for parsing time values. Must follow Java's [DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html) patterns.
    - Example: `"yyyy-MM-dd'T'HH:mm:ss.SSSZ"`

- **filePaths:** Array of paths to the CSV files to be migrated. Each path must point to an existing, accessible CSV file.
  - Example: `["./data/sensor_data.csv"]`

- **delimiter** (`String`): Delimiter used in the CSV files.
  - Example: `","` for comma-separated values.

- **escapeCharacter** (`String`): Escape character used in the CSV files.
  - Example: `"\""` for double quotes.

##### `iotdbSettings`

Configures the connection and schema settings for IoTDB.

- **connectionPoolSize** (`Integer`): Number of connections in the pool. Determines the level of concurrency when interacting with IoTDB.
  - Example: `10`

- **maxRetries** (`Integer`): Maximum number of retry attempts for failed operations when inserting data into IoTDB.
  - Example: `5`

- **retryInterval** (`Long`): Interval between retry attempts in milliseconds.
  - Example: `1500` (1.5 seconds)

- **maxBackoffTime** (`Long`): Maximum backoff time for retries in milliseconds. Helps to prevent overwhelming IoTDB with rapid retry attempts.
  - Example: `10000` (10 seconds)

- **connections:** Array of IoTDB connection details. Multiple connections can be specified for redundancy or load balancing.
  - **host** (`String`): IoTDB server host.
    - Example: `"192.168.0.202"`
  - **port** (`Integer`): IoTDB server port.
    - Example: `6667`
  - **username** (`String`): Username for IoTDB authentication.
    - Example: `"root"`
  - **password** (`String`): Password for IoTDB authentication.
    - Example: `"root"`

- **devices:** Defines IoTDB devices and their schema.
  - **deviceId** (`String`): Identifier for the IoTDB device. Should follow IoTDB's hierarchical naming conventions (e.g., `"root.powerplant"`).
  - **pathColumn** (`String`, optional): CSV column used to determine the IoTDB path. Required if `isAlignedTimeseries` is `false`.
    - Example: `"sensorTag"`
  - **isAlignedTimeseries** (`Boolean`): Indicates if the timeseries are aligned. Aligned timeseries share the same timestamp and can offer performance benefits.
    - `true` or `false`
  - **measurements:** List of measurements (columns) for the device.
    - **name** (`String`): Name of the measurement in IoTDB.
      - Example: `"sensorValue"`
    - **dataType** (`String`): Data type in IoTDB. Valid values based on IoTDB's supported `TSDataType`:
      - `"INT32"`
      - `"INT64"`
      - `"FLOAT"`
      - `"DOUBLE"`
      - `"BOOLEAN"`
      - `"TEXT"`
    - **joinKey** (`String`): Key to map CSV columns to this measurement. Must correspond to a `joinKey` defined in `csvSettings`.
      - Example: `"sensorValue"`
    - **encoding** (`String`): Encoding type for the timeseries. Valid values based on IoTDB's `TSEncoding`:
      - `"RLE"`
      - `"PLAIN"`
      - `"GORILLA"`
      - `"SNAPPY"`
    - **compression** (`String`): Compression type for the timeseries. Valid values based on IoTDB's `CompressionType`:
      - `"SNAPPY"`
      - `"GZIP"`
      - `"LZ4"`
      - `"UNCOMPRESSED"`
      - `"PAA"`
      - `"SSA"`
      - `"DEFAULT"`
  
##### `h2Config`

Settings for the embedded H2 database, which tracks the state of migration jobs and CSV settings.

- **url** (`String`): JDBC URL for connecting to the H2 database.
  - Example: `"jdbc:h2:./migrationDB;AUTO_SERVER=TRUE"`

- **username** (`String`): Username for H2 authentication.
  - Example: `"sa"`

- **password** (`String`): Password for H2 authentication.
  - Example: `""` (empty string if no password is set)

- **enableConsole** (`Boolean`): Enables the H2 web console for real-time monitoring and management.
  - `true` or `false`

- **consolePort** (`Integer`): Port number for the H2 web console. Applicable only if `enableConsole` is `true`.
  - Example: `8082`

##### `migrationSettings`

Controls the migration process's concurrency and batching.

- **threadsNumber** (`Integer`): Number of threads to use for migration tasks. Higher numbers can increase migration speed but may lead to higher resource consumption.
  - Example: `4`

- **batchSize** (`Integer`): Number of rows to process in each batch. Balancing batch size can optimize performance and memory usage.
  - Example: `1000`

### Configuration Validation

The application includes a robust configuration validation mechanism to ensure that the provided settings are consistent and adhere to the required constraints. Below are the key validation rules and considerations:

#### Valid Data Types

- **CSV Columns (`csvSettings.columns.type`):**
  - `"STRING"`
  - `"DOUBLE"`
  - `"FLOAT"`
  - `"INTEGER"`
  - `"LONG"`
  - `"BOOLEAN"`
  - `"TIME"`

- **IoTDB Measurements (`iotdbSettings.devices.measurements.dataType`):**
  - `"INT32"`
  - `"INT64"`
  - `"FLOAT"`
  - `"DOUBLE"`
  - `"BOOLEAN"`
  - `"TEXT"`

#### Join Key Constraints

- **Uniqueness:** Each `joinKey` must be unique across all `csvSettings`. Duplicate `joinKey` values will result in a validation error.
  
- **Reserved Keywords:** The following `joinKey` values are reserved for internal usage and cannot be used:
  - `"timestamp"`
  - `"row_id"`
  - `"row_number"`
  Attempting to use any of these reserved keywords will cause the configuration to be invalid.

- **Consistency:** Every `joinKey` used in `iotdbSettings.devices.measurements` must correspond to a `joinKey` defined in `csvSettings.columns`. Similarly, each `pathColumn` in devices must match a `joinKey` in `csvSettings`.

#### Data Type Conversions

The application enforces valid data type conversions between CSV columns and IoTDB measurements. Below are the permitted conversions:

- **From CSV to IoTDB:**

  | CSV Type | Allowed IoTDB Types                                                    |
  |----------|------------------------------------------------------------------------|
  | `"DOUBLE"` | `"DOUBLE"`, `"FLOAT"`, `"INT32"`, `"INT64"`, `"TEXT"`                |
  | `"FLOAT"`  | `"DOUBLE"`, `"FLOAT"`, `"INT32"`, `"INT64"`, `"TEXT"`                |
  | `"INTEGER"`| `"INT32"`, `"INT64"`, `"FLOAT"`, `"DOUBLE"`, `"TEXT"`               |
  | `"LONG"`   | `"INT32"`, `"INT64"`, `"FLOAT"`, `"DOUBLE"`, `"TEXT"`               |
  | `"BOOLEAN"`| `"BOOLEAN"`, `"INT32"`, `"INT64"`, `"TEXT"`                          |
  | `"TIME"`   | `"INT64"`, `"TEXT"`                                                  |
  | `"STRING"` | Any IoTDB type. Allowing conversion from `"STRING"` to any supported type, with appropriate parsing in the `Converter`. |

- **Invalid Conversions:**
  - Converting non-numeric CSV types to numeric IoTDB types without explicit parsing logic.
  - Any conversions not listed in the table above will be rejected during configuration validation.

#### Path Column Usage

- **Purpose:** The `pathColumn` defines which CSV column determines the path segmentation in IoTDB. This is essential for organizing timeseries under different devices.

- **Constraints:**
  - If `pathColumn` is specified for a device, it must correspond to a `joinKey` defined in `csvSettings.columns`.
  - Devices with a `pathColumn` cannot have `isAlignedTimeseries` set to `true`.
  - Each `pathColumn` must be used in at least one device configuration.

#### Additional Validation Rules

- **File Paths:** All paths specified in `csvSettings.filePaths` must point to existing, accessible CSV files. Non-existent or inaccessible files will cause validation to fail.

- **Join Key Matching:**
  - Every `joinKey` in `iotdbSettings.devices.measurements` must match a `joinKey` in `csvSettings.columns`.
  - There must be no unused `joinKey` values in `csvSettings.columns` that are not referenced in `iotdbSettings`.

- **Data Type Compatibility:**
  - The `ConfigValidator` ensures that the data types specified in CSV columns can be appropriately mapped to the specified IoTDB measurements.

Failure to adhere to these validation rules will result in the application terminating with a detailed error message, indicating the nature of the configuration issue.

## Usage

### Prerequisites

- **Java 17:** Ensure Java Development Kit (JDK) 17 is installed.
- **Apache IoTDB:** An instance of IoTDB should be running and accessible based on the `iotdbSettings` in `config.json`.
- **H2 Database:** No separate installation needed as it is embedded within the application.

### Steps to Run

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/NutonFlash/csv-to-iotdb-migration.git
   cd csv-to-iotdb-migration
   ```

2. **Configure `config.json`:**

   Modify the `config.json` file to match your CSV files and IoTDB settings. Ensure that the `filePaths` point to your CSV files and that the IoTDB connection details are correct.

3. **Build the Application:**

   Use your preferred build tool (e.g., Maven, Gradle) to compile the application.

   ```bash
   mvn clean install
   ```

4. **Run the Application:**

   ```bash
   java -jar target/csv-to-iotdb-migration.jar
   ```

5. **Access H2 Console (Optional):**

   If `enableConsole` is set to `true` in `config.json`, access the H2 web console at `http://localhost:8082` to monitor the database.

## Modules

### 1. Main

- **Path:** `src/main/java/org/kreps/csvtoiotdb/Main.java`
- **Function:** Entry point of the application. Initializes configurations, validates them, sets up the database, and starts migration tasks.

### 2. MigrationInitializer

- **Path:** `src/main/java/org/kreps/csvtoiotdb/MigrationInitializer.java`
- **Function:** Initializes or updates CSV settings and corresponding migration jobs in the H2 database before starting the migration process.

### 3. MigrateTask

- **Path:** `src/main/java/org/kreps/csvtoiotdb/MigrateTask.java`
- **Function:** Processes CSV files by reading, converting, and writing data to IoTDB. Manages transaction commits and handles errors during migration.

### 4. Converter

- **Path:** `src/main/java/org/kreps/csvtoiotdb/Converter.java`
- **Function:** Converts CSV row data into a format suitable for IoTDB, organizing data per device and handling any data type transformations.

### 5. IoTDBWriter

- **Path:** `src/main/java/org/kreps/csvtoiotdb/IoTDBWriter.java`
- **Function:** Handles the insertion of converted data into IoTDB. Manages retries and writes data in batches for efficiency.

### 6. H2DatabaseManager

- **Path:** `src/main/java/org/kreps/csvtoiotdb/H2DatabaseManager.java`
- **Function:** Manages the embedded H2 database, including creating tables, managing connections, and providing access to DAO classes.

### 7. DAO Layer

- **Path:** `src/main/java/org/kreps/csvtoiotdb/DAO/`
- **Function:** Data Access Objects that abstract interactions with the H2 database tables.
  - **CsvSettingsDAO:** Manages CRUD operations for CSV settings.
  - **JobsDAO:** Handles migration job records.
  - **MigrationLogsDAO:** Logs migration activities and errors.
  - **RowProcessingDAO:** Tracks the status of individual row processing.

## Database Management

### H2 Database

An embedded H2 database is used to track the state of migration jobs and CSV settings. The database schema includes the following tables:

- **csv_settings:** Stores unique CSV file paths and their migration status.
- **migration_logs:** Logs activities and errors related to each migration job.
- **row_processing:** Tracks the processing status of individual rows within a CSV file.
- **jobs:** Manages migration job records, including their status and associated CSV settings.

**Key Points:**

- **Unique File Paths:** Each CSV file path in `csv_settings` is unique, ensuring that each file is processed individually.
- **Processed Rows:** The `row_processing` table tracks which rows have been successfully processed and which failed, allowing for retries on application restart.
- **Job Tracking:** The `jobs` table maintains records of each migration job, including start and end times, status, and any error messages.

### IoTDB Schema Validation

The application performs schema validation to ensure that the necessary timeseries exist in IoTDB before data insertion. If a timeseries does not exist, the application automatically creates it based on the configuration provided in `config.json`.

**Process:**

1. **Schema Validation:** On startup, the application validates the IoTDB schema against the provided device configurations.
   - For existing timeseries, it checks the consistency between the actual schema in IoTDB and the one specified in the JSON configuration.
   - If inconsistencies are found (e.g., mismatched data types, encodings, or compressions), the application raises an exception.
2. **Timeseries Creation:** For any missing timeseries, the application creates them using the specified data types, encodings, and compressions.
3. **Aligned Timeseries:** Supports the creation of aligned timeseries if `isAlignedTimeseries` is set to `true` for a device.

**Capabilities and Limitations:**

- **Automatic Schema Creation:** Simplifies the migration process by reducing manual schema setup in IoTDB.
- **Schema Consistency:** Ensures that the data types, encodings, and compressions in IoTDB match the CSV configurations, preventing data integrity issues.
  - Validates existing timeseries against the JSON configuration to catch any discrepancies.
- **Limitations:**
  - **Path Columns:** Devices configured with `pathColumn` cannot have timeseries schemas automatically created as the specific path values are determined dynamically at runtime.
  - **Custom Time Formats:** While `TIME` columns can use custom formats, implementing the necessary parsing logic is the user's responsibility.

## Logging

The application uses SLF4J with a logging backend (e.g., Logback) for logging purposes. Logs provide detailed information about the migration process, including:

- **Info Logs:** Indicate the progress of migration tasks, such as initialization, batch processing, and successful data insertion.
- **Warning Logs:** Highlight non-critical issues, such as missing IoTDB devices for certain paths.
- **Error Logs:** Capture critical failures during migration tasks, database operations, or file processing.

Logs are outputted to both the console and log files (e.g., `application.log`, `error.log`).

## Error Handling

The application includes robust error handling mechanisms to ensure reliability:

- **Database Errors:** Catches and logs SQL exceptions during database operations, rolling back transactions as necessary.
- **CSV Processing Errors:** Handles I/O exceptions during CSV file reading, marking affected rows for retry.
- **IoTDB Insertion Errors:** Manages exceptions during data insertion into IoTDB, implementing retry logic based on configured settings.
- **Migration Failures:** Updates the migration status to `FAILED` in the H2 database and logs detailed error messages for troubleshooting.

**Retry Mechanism:**

- Failed rows are tracked in the `row_processing` table.
- On application restart, the migration process identifies and reprocesses these failed rows, ensuring data consistency.
