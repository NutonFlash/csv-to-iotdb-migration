package org.kreps.csvtoiotdb.DAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.kreps.csvtoiotdb.H2DatabaseManager;

public class CsvSettingsDAO {
    private final H2DatabaseManager dbManager;

    public CsvSettingsDAO() throws SQLException {
        this.dbManager = H2DatabaseManager.getInstance();
    }

    public void insertCsvSetting(String filePath) throws SQLException {
        String sql = "INSERT INTO csv_settings (file_path, status) VALUES (?, 'PENDING')";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, filePath);
            pstmt.executeUpdate();
        }
    }

    public Optional<Long> getCsvSettingId(String filePath) throws SQLException {
        String sql = "SELECT id FROM csv_settings WHERE file_path = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, filePath);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return Optional.of(rs.getLong("id"));
            }
        }
        return Optional.empty();
    }

    public void updateStatus(long id, String status) throws SQLException {
        String sql = "UPDATE csv_settings SET status = ?, last_processed_timestamp = CURRENT_TIMESTAMP WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, status);
            pstmt.setLong(2, id);
            pstmt.executeUpdate();
        }
    }

    public void incrementProcessedRows(long id, int count) throws SQLException {
        String sql = "UPDATE csv_settings SET processed_rows = processed_rows + ? WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, count);
            pstmt.setLong(2, id);
            pstmt.executeUpdate();
        }
    }

    public void incrementFailedRows(long id, int count, String errorMessage) throws SQLException {
        String sql = "UPDATE csv_settings SET failed_rows = failed_rows + ?, error_message = ? WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, count);
            pstmt.setString(2, errorMessage);
            pstmt.setLong(3, id);
            pstmt.executeUpdate();
        }
    }

    public List<String> getAllCsvSettingFilePaths() throws SQLException {
        String sql = "SELECT file_path FROM csv_settings";
        List<String> filePaths = new ArrayList<>();

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                filePaths.add(rs.getString("file_path"));
            }
        }
        return filePaths;
    }

    public String getStatus(long id) throws SQLException {
        String sql = "SELECT status FROM csv_settings WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, id);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("status");
            }
        }
        return null; // or throw an exception if the setting doesn't exist
    }

    public int getProcessedRows(long id) throws SQLException {
        String sql = "SELECT processed_rows FROM csv_settings WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, id);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("processed_rows");
            }
        }
        return -1; // or throw an exception if the setting doesn't exist
    }

    public int getFailedRows(long id) throws SQLException {
        String sql = "SELECT failed_rows FROM csv_settings WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, id);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("failed_rows");
            }
        }
        return -1; // or throw an exception if the setting doesn't exist
    }

    public void deleteCsvSetting(long id) throws SQLException {
        String sql = "DELETE FROM csv_settings WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, id);
            pstmt.executeUpdate();
        }
    }

    public int getTotalCsvSettingsCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM csv_settings";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return 0;
    }

    public void insertOrIgnoreCsvSetting(String filePath) throws SQLException {
        String sql = "MERGE INTO csv_settings (file_path, status) KEY(file_path) VALUES (?, 'PENDING')";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, filePath);
            pstmt.executeUpdate();
        }
    }
}