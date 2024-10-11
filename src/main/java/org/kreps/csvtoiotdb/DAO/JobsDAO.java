package org.kreps.csvtoiotdb.DAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;

import org.kreps.csvtoiotdb.H2DatabaseManager;

public class JobsDAO {
    private final H2DatabaseManager dbManager;

    public JobsDAO() throws SQLException {
        this.dbManager = H2DatabaseManager.getInstance();
    }

    public Optional<Long> getJobIdByCsvSettingId(long csvSettingId, Connection conn) throws SQLException {
        String sql = "SELECT id FROM jobs WHERE csv_setting_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return Optional.of(rs.getLong("id"));
            }
        }
        return Optional.empty();
    }

    public long createJob(long csvSettingId, Connection conn) throws SQLException {
        String sql = "INSERT INTO jobs (csv_setting_id, status, start_time) VALUES (?, ?, CURRENT_TIMESTAMP)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
            pstmt.setLong(1, csvSettingId);
            pstmt.setString(2, JobStatus.PENDING.getValue());
            pstmt.executeUpdate();
            ResultSet rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                return rs.getLong(1);
            }
            throw new SQLException("Failed to retrieve job ID.");
        }
    }

    public void updateJobStatus(long jobId, JobStatus status, String errorMessage, Connection conn)
            throws SQLException {
        String sql = "UPDATE jobs SET status = ?, error_message = ?, end_time = CURRENT_TIMESTAMP WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, status.getValue());
            pstmt.setString(2, errorMessage);
            pstmt.setLong(3, jobId);
            pstmt.executeUpdate();
        }
    }

    public void updateJobProgress(long jobId, int processedRows, int failedRows, Connection conn) throws SQLException {
        String sql = "UPDATE jobs SET processed_rows = processed_rows + ?, failed_rows = failed_rows + ? WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, processedRows);
            pstmt.setInt(2, failedRows);
            pstmt.setLong(3, jobId);
            pstmt.executeUpdate();
        }
    }

    public JobStatus getJobStatus(long jobId, Connection conn) throws SQLException {
        String sql = "SELECT status FROM jobs WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, jobId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return JobStatus.fromString(rs.getString("status"));
            }
        }
        return null;
    }

    public Timestamp getJobStartTime(long jobId, Connection conn) throws SQLException {
        String sql = "SELECT start_time FROM jobs WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, jobId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getTimestamp("start_time");
            }
        }
        return null;
    }

    public Timestamp getJobEndTime(long jobId, Connection conn) throws SQLException {
        String sql = "SELECT end_time FROM jobs WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, jobId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getTimestamp("end_time");
            }
        }
        return null;
    }

    public int getProcessedRows(long jobId, Connection conn) throws SQLException {
        String sql = "SELECT processed_rows FROM jobs WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, jobId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("processed_rows");
            }
        }
        return 0;
    }

    public int getFailedRows(long jobId, Connection conn) throws SQLException {
        String sql = "SELECT failed_rows FROM jobs WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, jobId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("failed_rows");
            }
        }
        return 0;
    }

    public String getErrorMessage(long jobId, Connection conn) throws SQLException {
        String sql = "SELECT error_message FROM jobs WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, jobId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("error_message");
            }
        }
        return null;
    }

    // Add a method to get the latest job for a CSV setting
    public Optional<Long> getLatestJobIdByCsvSettingId(long csvSettingId, Connection conn) throws SQLException {
        String sql = "SELECT id FROM jobs WHERE csv_setting_id = ? ORDER BY start_time DESC LIMIT 1";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, csvSettingId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return Optional.of(rs.getLong("id"));
            }
        }
        return Optional.empty();
    }
}