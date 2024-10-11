package org.kreps.csvtoiotdb;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.kreps.csvtoiotdb.DAO.CsvSettingsDAO;
import org.kreps.csvtoiotdb.DAO.RowProcessingDAO;
import org.kreps.csvtoiotdb.DAO.RowProcessingStatus;
import org.kreps.csvtoiotdb.configs.csv.CsvColumn;
import org.kreps.csvtoiotdb.configs.csv.CsvSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

/**
 * Reads CSV files and processes them in batches.
 */
public class CSVReader implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CSVReader.class);

    private final CsvSettings csvSettings;
    private final int batchSize;
    private final Iterator<String> filePathIterator;
    private final CsvParser parser;
    private final Map<String, Integer> headerMap;
    private Reader currentReader;
    private boolean isParsing;
    private final AtomicBoolean isClosed;
    private String currentFilePath;
    private int currentRowNumber;
    private long currentCsvSettingId;
    private int totalRows;

    private final RowProcessingDAO rowProcessingDAO;
    private final CsvSettingsDAO csvSettingsDAO;
    private final H2DatabaseManager dbManager;

    private Set<Integer> failedRowNumbers;
    private boolean processOnlyFailedRows;

    public CSVReader(CsvSettings csvSettings, int batchSize, H2DatabaseManager dbManager)
            throws IOException, SQLException {
        this.csvSettings = csvSettings;
        this.batchSize = batchSize;
        this.filePathIterator = csvSettings.getFilePaths().iterator();
        this.parser = new CsvParser(createCsvParserSettings());
        this.headerMap = new HashMap<>();
        this.isParsing = false;
        this.isClosed = new AtomicBoolean(false);
        this.rowProcessingDAO = new RowProcessingDAO();
        this.csvSettingsDAO = new CsvSettingsDAO();
        this.dbManager = dbManager;
        this.totalRows = 0;
        this.failedRowNumbers = new HashSet<>();
        this.processOnlyFailedRows = false;
    }

    /**
     * Reads a batch of rows from the CSV files.
     *
     * @return A list of parsed rows or null if no more rows are available.
     * @throws IOException  If an I/O error occurs.
     * @throws SQLException If a database error occurs.
     */
    public List<Map<String, Object>> readBatch() throws IOException, SQLException {
        ensureNotClosed();

        List<Map<String, Object>> batch = new ArrayList<>(this.batchSize);

        try (Connection conn = dbManager.getConnection()) {
            conn.setAutoCommit(false);
            try {
                while (batch.size() < this.batchSize) {
                    if (!isParsing) {
                        if (!filePathIterator.hasNext()) {
                            return batch.isEmpty() ? null : batch;
                        }
                        openNextFile(conn);
                    }

                    parseRows(batch, conn);

                    if (!isParsing) {
                        break;
                    }
                }
                conn.commit();
            } catch (IOException | SQLException e) {
                conn.rollback();
                logger.error("Error reading batch. File: {}, Last processed row: {}, Error: {}",
                        currentFilePath, currentRowNumber, e.getMessage(), e);
                throw new IOException("Error reading batch", e);
            }
        }

        return batch.isEmpty() ? null : batch;
    }

    private synchronized void openNextFile(Connection conn) throws IOException, SQLException {
        if (isClosed.get()) {
            throw new IllegalStateException("CSVReader is closed and cannot open new files.");
        }

        closeCurrentReader();

        if (!filePathIterator.hasNext()) {
            isParsing = false;
            updateTotalRowsForCurrentFile(conn);
            return;
        }

        currentFilePath = filePathIterator.next();
        currentRowNumber = 0;
        try {
            currentReader = new FileReader(currentFilePath);
            parser.beginParsing(currentReader);
            String[] headers = parser.getContext().headers();
            updateHeaderMap(headers);
            isParsing = true;
            logger.info("Opened new CSV file: {}", currentFilePath);

            Optional<Long> optionalId = csvSettingsDAO.getCsvSettingId(currentFilePath);
            currentCsvSettingId = optionalId
                    .orElseThrow(() -> new IllegalStateException("CSV setting not found for file: " + currentFilePath));

            boolean hasFailedRows = csvSettingsDAO.hasFailedRows(currentCsvSettingId);
            if (hasFailedRows) {
                failedRowNumbers = rowProcessingDAO.getFailedRowNumbers(currentCsvSettingId, conn);
                processOnlyFailedRows = !failedRowNumbers.isEmpty();
                if (processOnlyFailedRows) {
                    logger.info("Processing only failed rows for file: {}", currentFilePath);
                }
            } else {
                processOnlyFailedRows = false;
                failedRowNumbers.clear();
            }

            // Retrieve failed row numbers for the current CSV setting
            failedRowNumbers = rowProcessingDAO.getFailedRowNumbers(currentCsvSettingId, conn);
            processOnlyFailedRows = !failedRowNumbers.isEmpty();

            if (processOnlyFailedRows) {
                logger.info("Processing only failed rows for file: {}", currentFilePath);
            }
        } catch (IOException | SQLException e) {
            logger.error("Failed to open CSV file: {}. Error: {}", currentFilePath, e.getMessage(), e);
            closeCurrentReader();
            throw e;
        }
    }

    /**
     * Parses rows from the current CSV file and adds them to the batch.
     *
     * @param batch The batch to add the parsed rows to.
     * @throws IOException  If an I/O error occurs.
     * @throws SQLException If a database error occurs.
     */
    private void parseRows(List<Map<String, Object>> batch, Connection conn) throws IOException, SQLException {
        String[] row = null;

        while (batch.size() < this.batchSize && (row = parser.parseNext()) != null) {
            currentRowNumber++;
            totalRows++;

            if (processOnlyFailedRows && !failedRowNumbers.contains(currentRowNumber)) {
                continue; // Skip rows that are not in the failed rows set
            }

            try {
                String rowId = generateRowId(currentCsvSettingId, currentFilePath, currentRowNumber);
                Map<String, Object> parsedRow = parseRow(row);
                parsedRow.put("row_id", rowId);
                parsedRow.put("row_number", currentRowNumber);
                batch.add(parsedRow);

                RowProcessingStatus status = processOnlyFailedRows ? RowProcessingStatus.RETRY
                        : RowProcessingStatus.PENDING;
                rowProcessingDAO.insertOrUpdateRowProcessing(currentCsvSettingId, rowId, currentRowNumber,
                        status, conn);
            } catch (IllegalArgumentException e) {
                logger.warn("Skipping invalid row {}: {}. File: {}", currentRowNumber, e.getMessage(), currentFilePath);
                String rowId = generateRowId(currentCsvSettingId, currentFilePath, currentRowNumber);
                rowProcessingDAO.insertOrUpdateRowProcessing(currentCsvSettingId, rowId, currentRowNumber,
                        RowProcessingStatus.FAILED, conn);
            }
        }

        if (row == null) {
            updateTotalRowsForCurrentFile(conn);
            stopParsingAndClose();
        }
    }

    /**
     * Parses a single row of data from the CSV file.
     *
     * @param row The CSV row to parse.
     * @return A map representing the parsed data.
     */
    private Map<String, Object> parseRow(String[] row) {
        Map<String, Object> parsedRow = new HashMap<>();

        try {
            CsvColumn timestampColumn = csvSettings.getTimestampColumn();
            Object parsedTimestamp = parseColumnValue(timestampColumn, row);
            parsedRow.put("timestamp", parsedTimestamp);

            for (CsvColumn column : csvSettings.getColumns()) {
                Object parsedValue = parseColumnValue(column, row);
                parsedRow.put(column.getJoinKey(), parsedValue);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Error parsing row: " + e.getMessage(), e);
        }

        return parsedRow;
    }

    /**
     * Parses the value for a specific column from the given row.
     *
     * @param column The column configuration.
     * @param row    The CSV row data.
     * @return The parsed value for the column.
     */
    private Object parseColumnValue(CsvColumn column, String[] row) {
        Integer columnIndex = headerMap.get(column.getName());
        if (columnIndex == null || columnIndex >= row.length) {
            throw new IllegalArgumentException("Missing column: " + column.getName());
        }
        try {
            return column.parseValue(row[columnIndex]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error parsing column " + column.getName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Stops parsing and closes the current reader.
     */
    private synchronized void stopParsingAndClose() {
        if (isParsing) {
            try {
                parser.stopParsing();
            } catch (Exception e) {
                logger.error("Error stopping parser", e);
            } finally {
                isParsing = false;
                closeCurrentReader();
            }
        }
    }

    /**
     * Closes the current reader and ensures it's released.
     */
    private synchronized void closeCurrentReader() {
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (IOException e) {
                logger.error("Error closing current reader", e);
            } finally {
                currentReader = null;
            }
        }
    }

    /**
     * Updates the header map with column positions from the CSV file.
     *
     * @param headers The headers from the CSV file.
     */
    private void updateHeaderMap(String[] headers) {
        headerMap.clear();
        for (int i = 0; i < headers.length; i++) {
            headerMap.put(headers[i], i);
        }
    }

    /**
     * Ensures the CSVReader is not closed.
     */
    private void ensureNotClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("CSVReader is already closed.");
        }
    }

    /**
     * Closes the CSVReader and releases all resources.
     *
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            stopParsingAndClose();
            logger.info("CSVReader closed successfully");
        }
    }

    /**
     * Creates and configures the CsvParserSettings.
     *
     * @return The configured CsvParserSettings.
     */
    private CsvParserSettings createCsvParserSettings() {
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setDelimiter(csvSettings.getDelimiter().charAt(0));
        settings.getFormat().setQuote(csvSettings.getEscapeCharacter().charAt(0));
        settings.setHeaderExtractionEnabled(true);
        return settings;
    }

    private String generateRowId(long csvSettingId, String filePath, long rowNumber) {
        String rawId = csvSettingId + ":" + filePath + ":" + rowNumber;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(rawId.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            logger.error("Error generating row ID", e);
            // Fallback to a simple concatenation if hashing fails
            return rawId.replaceAll("[^a-zA-Z0-9]", "_");
        }
    }

    private void updateTotalRowsForCurrentFile(Connection conn) throws SQLException {
        csvSettingsDAO.updateTotalRows(currentCsvSettingId, totalRows, conn);
        logger.info("Updated total rows for file: {}. Total rows: {}", currentFilePath, totalRows);
        totalRows = 0; // Reset for the next file
    }
}