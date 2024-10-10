package org.kreps.csvtoiotdb;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.kreps.csvtoiotdb.configs.csv.CsvColumn;
import org.kreps.csvtoiotdb.configs.csv.CsvSettings;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads CSV files in batches and parses them into maps of joinKey to values.
 */
public class CSVReader implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CSVReader.class);

    private final CsvSettings csvSettings;
    private final int batchSize;
    private final CsvParser parser;
    private final Iterator<String> filePathIterator;
    private Reader currentReader;
    private boolean isParsing;
    private final Map<String, Integer> headerMap;
    private final AtomicBoolean isClosed;

    /**
     * Constructs a CSVReader instance.
     *
     * @param csvSettings The configuration settings for CSV parsing.
     * @param batchSize   The number of rows to read in each batch.
     * @throws IOException If an I/O error occurs.
     */
    public CSVReader(CsvSettings csvSettings, int batchSize) throws IOException {
        this.csvSettings = Objects.requireNonNull(csvSettings, "CSV settings cannot be null.");
        this.batchSize = batchSize;

        CsvParserSettings parserSettings = createCsvParserSettings();
        this.parser = new CsvParser(parserSettings);
        this.filePathIterator = csvSettings.getFilePaths().iterator();
        this.headerMap = new HashMap<>();
        this.isClosed = new AtomicBoolean(false);
        this.isParsing = false;
    }

    /**
     * Reads the next batch of rows from the CSV files.
     *
     * @return A list of maps where each map represents a row with joinKey to value
     *         mappings,
     *         or null if no more data is available.
     * @throws IOException If an I/O error occurs.
     */
    public List<Map<String, Object>> readBatch() throws IOException {
        ensureNotClosed();

        List<Map<String, Object>> batch = new ArrayList<>(this.batchSize);

        while (batch.size() < this.batchSize) {
            if (!isParsing) {
                if (!filePathIterator.hasNext()) {
                    return batch.isEmpty() ? null : batch;
                }
                openNextFile();
            }

            parseRows(batch);

            if (!isParsing) { // Parsing stopped, no more rows to parse
                break;
            }
        }

        return batch.isEmpty() ? null : batch;
    }

    /**
     * Opens the next file and prepares the parser for reading.
     *
     * @throws IOException If an I/O error occurs.
     */
    private synchronized void openNextFile() throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("CSVReader is closed and cannot open new files.");
        }

        closeCurrentReader(); // Ensure the current reader is closed before opening a new one

        String nextFile = filePathIterator.next();
        try {
            currentReader = new FileReader(nextFile);
            parser.beginParsing(currentReader);
            String[] headers = parser.getContext().headers();
            updateHeaderMap(headers);
            isParsing = true;
            logger.info("Opened new CSV file: {}", nextFile);
        } catch (IOException e) {
            logger.error("Failed to open CSV file: {}", nextFile, e);
            closeCurrentReader(); // Ensure the reader is closed in case of an error
            isParsing = false;
            throw e;
        }
    }

    /**
     * Parses rows from the current CSV file and adds them to the batch.
     *
     * @param batch The batch to add the parsed rows to.
     * @throws IOException If an I/O error occurs.
     */
    private void parseRows(List<Map<String, Object>> batch) throws IOException {
        String[] row = null;

        while (batch.size() < this.batchSize && (row = parser.parseNext()) != null) {
            try {
                Map<String, Object> parsedRow = parseRow(row);
                batch.add(parsedRow);
            } catch (IllegalArgumentException e) {
                logger.warn("Skipping invalid row: {}", e.getMessage());
            }
        }

        if (row == null) { // No more rows in current file
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

        // Add timestamp first
        CsvColumn timestampColumn = csvSettings.getTimestampColumn();
        Object parsedTimestamp = parseColumnValue(timestampColumn, row);
        parsedRow.put("timestamp", parsedTimestamp);

        // Add other columns
        for (CsvColumn column : csvSettings.getColumns()) {
            Object parsedValue = parseColumnValue(column, row);
            parsedRow.put(column.getJoinKey(), parsedValue);
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
        return column.parseValue(row[columnIndex]);
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
}