package org.kreps.csvtoiotdb.configs.csv;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CsvSettings {
    private List<String> filePaths;
    private CsvColumn timestampColumn;
    private List<CsvColumn> columns;
    private String delimiter;
    private String escapeCharacter;

    public CsvSettings() {
    }

    public CsvSettings(List<String> filePaths, CsvColumn timestampColumn, List<CsvColumn> columns) {
        this(filePaths, timestampColumn, columns, ",", "\"");
    }

    @JsonCreator
    public CsvSettings(@JsonProperty("filePaths") List<String> filePaths,
            @JsonProperty("timestampColumn") CsvColumn timestampColumn,
            @JsonProperty("columns") List<CsvColumn> columns, @JsonProperty("delimiter") String delimiter,
            @JsonProperty("escapeCharacter") String escapeCharacter) {
        this.filePaths = filePaths;
        this.timestampColumn = timestampColumn;
        this.columns = columns;
        this.delimiter = (delimiter == null) ? "," : delimiter;
        this.escapeCharacter = (escapeCharacter == null) ? "\"" : escapeCharacter;
    }

    public List<String> getFilePaths() {
        return filePaths;
    }

    public void setFilePaths(List<String> filePaths) {
        this.filePaths = filePaths;
    }

    public List<CsvColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<CsvColumn> columns) {
        this.columns = columns;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getEscapeCharacter() {
        return escapeCharacter;
    }

    public void setEscapeCharacter(String escapeCharacter) {
        this.escapeCharacter = escapeCharacter;
    }

    public CsvColumn getTimestampColumn() {
        return timestampColumn;
    }

    public void setTimestampColumn(CsvColumn timestampColumn) {
        this.timestampColumn = timestampColumn;
    }
}
