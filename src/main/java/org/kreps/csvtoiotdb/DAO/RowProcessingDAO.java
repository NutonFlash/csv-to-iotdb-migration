package org.kreps.csvtoiotdb.DAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.kreps.csvtoiotdb.H2DatabaseManager;

public class RowProcessingDAO {
    private final H2DatabaseManager dbManager;

    public RowProcessingDAO() throws SQLException {
        this.dbManager = H2DatabaseManager.getInstance();
    }

    public void insertRowProcessing(long csvSettingId, int rowNumber, String status) throws SQLException {
        String sql = "INSERT INTO row_processing (csv_setting_id, row_number, status) VALUES (?, ?, ?)";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setInt(2, rowNumber);
            pstmt.setString(3, status);
            pstmt.executeUpdate();
        }
    }

    public void updateRowStatus(long csvSettingId, int rowNumber, String status, String errorMessage) throws SQLException {
        String sql = "UPDATE row_processing SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP, retry_count = retry_count + 1 WHERE csv_setting_id = ? AND row_number = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, status);
            pstmt.setString(2, errorMessage);
            pstmt.setLong(3, csvSettingId);
            pstmt.setInt(4, rowNumber);
            pstmt.executeUpdate();
        }
    }

    public void resetRowStatus(long csvSettingId, int rowNumber) throws SQLException {
        String sql = "UPDATE row_processing SET status = 'PENDING', error_message = NULL, updated_at = CURRENT_TIMESTAMP WHERE csv_setting_id = ? AND row_number = ?";

        try (Connection conn = dbManager.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setInt(2, rowNumber);
            pstmt.executeUpdate();
        }
    }

    public List<Integer> getFailedRowNumbers(long csvSettingId) throws SQLException {
        String sql = "SELECT row_number FROM row_processing WHERE csv_setting_id = ? AND status = 'FAILED'";
        List<Integer> failedRows = new ArrayList<>();

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                failedRows.add(rs.getInt("row_number"));
            }
        }
        return failedRows;
    }

    public String getRowStatus(long csvSettingId, int rowNumber) throws SQLException {
        String sql = "SELECT status FROM row_processing WHERE csv_setting_id = ? AND row_number = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setInt(2, rowNumber);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("status");
            }
        }
        return null; // or throw an exception if the row doesn't exist
    }

    public String getRowErrorMessage(long csvSettingId, int rowNumber) throws SQLException {
        String sql = "SELECT error_message FROM row_processing WHERE csv_setting_id = ? AND row_number = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setInt(2, rowNumber);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("error_message");
            }
        }
        return null; // or throw an exception if the row doesn't exist
    }

    public List<Integer> getRowsEligibleForRetry(long csvSettingId, int maxRetryCount) throws SQLException {
        String sql = "SELECT row_number FROM row_processing WHERE csv_setting_id = ? AND status = 'FAILED' AND retry_count < ?";
        List<Integer> eligibleRows = new ArrayList<>();

        try (Connection conn = dbManager.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setInt(2, maxRetryCount);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                eligibleRows.add(rs.getInt("row_number"));
            }
        }
        return eligibleRows;
    }
}