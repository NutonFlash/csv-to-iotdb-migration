package org.kreps.csvtoiotdb.DAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.kreps.csvtoiotdb.H2DatabaseManager;

public class RowProcessingDAO {
    private final H2DatabaseManager dbManager;

    public RowProcessingDAO() {
        this.dbManager = H2DatabaseManager.getInstance();
    }

    public void insertRowProcessing(long csvSettingId, String rowId, int rowNumber, RowProcessingStatus status,
            Connection conn) throws SQLException {
        String sql = "INSERT INTO row_processing (csv_setting_id, row_id, row_number, status, created_at, updated_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setString(2, rowId);
            pstmt.setInt(3, rowNumber);
            pstmt.setString(4, status.getValue());
            pstmt.executeUpdate();
        }
    }

    public void updateRowStatus(long csvSettingId, String rowId, int rowNumber, RowProcessingStatus status, String errorMessage,
            Connection conn) throws SQLException {
        String sql = "UPDATE row_processing SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP, " +
                     "retry_count = CASE WHEN status = 'FAILED' THEN retry_count + 1 ELSE retry_count END " +
                     "WHERE csv_setting_id = ? AND row_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, status.getValue());
            pstmt.setString(2, errorMessage);
            pstmt.setLong(3, csvSettingId);
            pstmt.setString(4, rowId);
            pstmt.executeUpdate();
        }
    }

    public void updateRowStatuses(long csvSettingId, List<String> rowIds, RowProcessingStatus status,
            String errorMessage, Connection conn) throws SQLException {
        String sql = "UPDATE row_processing SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP, " +
                     "retry_count = CASE WHEN status = 'FAILED' THEN retry_count + 1 ELSE retry_count END " +
                     "WHERE csv_setting_id = ? AND row_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (String rowId : rowIds) {
                pstmt.setString(1, status.getValue());
                pstmt.setString(2, errorMessage);
                pstmt.setLong(3, csvSettingId);
                pstmt.setString(4, rowId);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    public void resetRowStatus(long csvSettingId, String rowId, Connection conn) throws SQLException {
        String sql = "UPDATE row_processing SET status = ?, error_message = NULL, updated_at = CURRENT_TIMESTAMP WHERE csv_setting_id = ? AND row_id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, RowProcessingStatus.PENDING.getValue());
            pstmt.setLong(2, csvSettingId);
            pstmt.setString(3, rowId);
            pstmt.executeUpdate();
        }
    }

    public List<String> getFailedRowIds(long csvSettingId) throws SQLException {
        String sql = "SELECT row_id FROM row_processing WHERE csv_setting_id = ? AND status = ?";
        List<String> failedRows = new ArrayList<>();

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setString(2, RowProcessingStatus.FAILED.getValue());
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                failedRows.add(rs.getString("row_id"));
            }
        }
        return failedRows;
    }

    public RowProcessingStatus getRowStatus(long csvSettingId, String rowId) throws SQLException {
        String sql = "SELECT status FROM row_processing WHERE csv_setting_id = ? AND row_id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setString(2, rowId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return RowProcessingStatus.fromString(rs.getString("status"));
            }
        }
        return null; // or throw an exception if the row doesn't exist
    }

    public String getRowErrorMessage(long csvSettingId, String rowId) throws SQLException {
        String sql = "SELECT error_message FROM row_processing WHERE csv_setting_id = ? AND row_id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setString(2, rowId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("error_message");
            }
        }
        return null; // or throw an exception if the row doesn't exist
    }

    public List<String> getRowsEligibleForRetry(long csvSettingId, int maxRetryCount, Connection conn)
            throws SQLException {
        String sql = "SELECT row_id FROM row_processing WHERE csv_setting_id = ? AND status = 'FAILED' AND retry_count < ?";
        List<String> eligibleRows = new ArrayList<>();

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setInt(2, maxRetryCount);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                eligibleRows.add(rs.getString("row_id"));
            }
        }
        return eligibleRows;
    }

    // New method to get row_number by row_id
    public int getRowNumber(long csvSettingId, String rowId) throws SQLException {
        String sql = "SELECT row_number FROM row_processing WHERE csv_setting_id = ? AND row_id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setString(2, rowId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("row_number");
            }
        }
        throw new SQLException("Row not found for csvSettingId: " + csvSettingId + " and rowId: " + rowId);
    }

    // New method to get row_id by row_number
    public String getRowId(long csvSettingId, int rowNumber) throws SQLException {
        String sql = "SELECT row_id FROM row_processing WHERE csv_setting_id = ? AND row_number = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setInt(2, rowNumber);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("row_id");
            }
        }
        throw new SQLException("Row not found for csvSettingId: " + csvSettingId + " and rowNumber: " + rowNumber);
    }

    public Set<Integer> getFailedRowNumbers(long csvSettingId, Connection conn) throws SQLException {
        Set<Integer> failedRows = new HashSet<>();
        String sql = "SELECT row_number FROM row_processing WHERE csv_setting_id = ? AND status = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, csvSettingId);
            stmt.setString(2, RowProcessingStatus.FAILED.getValue());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    failedRows.add(rs.getInt("row_number"));
                }
            }
        }
        return failedRows;
    }

    public void insertOrUpdateRowProcessing(long csvSettingId, String rowId, int rowNumber, RowProcessingStatus status,
            Connection conn) throws SQLException {
        String sql = "MERGE INTO row_processing (csv_setting_id, row_id, row_number, status) KEY (csv_setting_id, row_id) VALUES (?, ?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, csvSettingId);
            stmt.setString(2, rowId);
            stmt.setInt(3, rowNumber);
            stmt.setString(4, status.getValue());
            stmt.executeUpdate();
        }
    }
}