package org.kreps.csvtoiotdb.DAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.kreps.csvtoiotdb.H2DatabaseManager;

public class MigrationLogsDAO {
    private final H2DatabaseManager dbManager;

    public MigrationLogsDAO() throws SQLException {
        this.dbManager = H2DatabaseManager.getInstance();
    }

    public void insertLog(Long csvSettingId, String level, String message) throws SQLException {
        String sql = "INSERT INTO migration_logs (csv_setting_id, log_level, message) VALUES (?, ?, ?)";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            if (csvSettingId != null) {
                pstmt.setLong(1, csvSettingId);
            } else {
                pstmt.setNull(1, java.sql.Types.BIGINT);
            }
            pstmt.setString(2, level);
            pstmt.setString(3, message);
            pstmt.executeUpdate();
        }
    }

    public List<String> getLogsByCsvSettingId(Long csvSettingId) throws SQLException {
        String sql = "SELECT message FROM migration_logs WHERE csv_setting_id = ? ORDER BY log_timestamp";
        List<String> logs = new ArrayList<>();

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                logs.add(rs.getString("message"));
            }
        }
        return logs;
    }

    public List<String> getLogsByLevel(String level) throws SQLException {
        String sql = "SELECT message FROM migration_logs WHERE log_level = ? ORDER BY log_timestamp";
        List<String> logs = new ArrayList<>();

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, level);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                logs.add(rs.getString("message"));
            }
        }
        return logs;
    }

    public void updateLogMessage(long logId, String newMessage) throws SQLException {
        String sql = "UPDATE migration_logs SET message = ? WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, newMessage);
            pstmt.setLong(2, logId);
            pstmt.executeUpdate();
        }
    }

    public void deleteLog(long logId) throws SQLException {
        String sql = "DELETE FROM migration_logs WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, logId);
            pstmt.executeUpdate();
        }
    }

    public void deleteLogsByCsvSettingId(Long csvSettingId) throws SQLException {
        String sql = "DELETE FROM migration_logs WHERE csv_setting_id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.executeUpdate();
        }
    }

    public int getTotalLogCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM migration_logs";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return 0;
    }
}