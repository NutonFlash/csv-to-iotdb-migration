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

    public CsvSettingsDAO() {
        this.dbManager = H2DatabaseManager.getInstance();
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

    public long insertCsvSetting(String filePath, Connection conn) throws SQLException {
        String sql = "INSERT INTO csv_settings (file_path, status) VALUES (?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, filePath);
            pstmt.setString(2, CsvStatus.PENDING.getValue());
            pstmt.executeUpdate();
            ResultSet rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                return rs.getLong(1);
            }
            throw new SQLException("Failed to retrieve CSV setting ID.");
        }
    }

    public String getFilePathByCsvSettingId(long csvSettingId) throws SQLException {
        String sql = "SELECT file_path FROM csv_settings WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("file_path");
            }
        }
        throw new SQLException("No CSV setting found for id: " + csvSettingId);
    }

    public void updateStatus(long id, CsvStatus status, Connection conn) throws SQLException {
        String sql = "UPDATE csv_settings SET status = ?, last_processed_timestamp = CURRENT_TIMESTAMP WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, status.getValue());
            pstmt.setLong(2, id);
            pstmt.executeUpdate();
        }
    }

    public void incrementProcessedRows(long id, int count, Connection conn) throws SQLException {
        String sql = "UPDATE csv_settings SET processed_rows = processed_rows + ? WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, count);
            pstmt.setLong(2, id);
            pstmt.executeUpdate();
        }
    }

    public void incrementFailedRows(long id, int count, String errorMessage, Connection conn) throws SQLException {
        String sql = "UPDATE csv_settings SET failed_rows = failed_rows + ?, error_message = ? WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
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

    public CsvStatus getStatus(long id) throws SQLException {
        String sql = "SELECT status FROM csv_settings WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, id);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return CsvStatus.fromString(rs.getString("status"));
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

    public void deleteCsvSetting(long id, Connection conn) throws SQLException {
        String sql = "DELETE FROM csv_settings WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
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

    public void insertOrIgnoreCsvSetting(String filePath, Connection conn) throws SQLException {
        String sql = "MERGE INTO csv_settings (file_path, status) KEY(file_path) VALUES (?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, filePath);
            pstmt.setString(2, CsvStatus.PENDING.getValue());
            pstmt.executeUpdate();
        }
    }

    public void updateTotalRows(long id, int totalRows, Connection conn) throws SQLException {
        String sql = "UPDATE csv_settings SET total_rows = ? WHERE id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, totalRows);
            pstmt.setLong(2, id);
            pstmt.executeUpdate();
        }
    }

    public void updateErrorMessage(long id, String errorMessage, Connection conn) throws SQLException {
        String sql = "UPDATE csv_settings SET error_message = ? WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, errorMessage);
            pstmt.setLong(2, id);
            pstmt.executeUpdate();
        }
    }

    public void setHasFailedRows(long csvSettingId, boolean hasFailedRows, Connection conn) throws SQLException {
        String sql = "UPDATE csv_settings SET has_failed_rows = ? WHERE id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setBoolean(1, hasFailedRows);
            stmt.setLong(2, csvSettingId);
            stmt.executeUpdate();
        }
    }

    public boolean hasFailedRows(long csvSettingId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM row_processing WHERE csv_setting_id = ? AND status = 'FAILED'";
        try (Connection conn = dbManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, csvSettingId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }
}