package org.kreps.csvtoiotdb.DAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.kreps.csvtoiotdb.H2DatabaseManager;

public class JobsDAO {
    private final H2DatabaseManager dbManager;

    public JobsDAO() throws SQLException {
        this.dbManager = H2DatabaseManager.getInstance();
    }

    public long createJob(String filePath) throws SQLException {
        String sql = "INSERT INTO jobs (file_path, status) VALUES (?, 'STARTED')";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, filePath);
            pstmt.executeUpdate();
            ResultSet rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                return rs.getLong(1);
            }
            throw new SQLException("Failed to retrieve job ID.");
        }
    }

    public void updateJobStatus(long jobId, String status, String errorMessage) throws SQLException {
        String sql = "UPDATE jobs SET status = ?, error_message = ?, end_time = CURRENT_TIMESTAMP WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, status);
            pstmt.setString(2, errorMessage);
            pstmt.setLong(3, jobId);
            pstmt.executeUpdate();
        }
    }

    public String getJobStatus(long jobId) throws SQLException {
        String sql = "SELECT status FROM jobs WHERE id = ?";

        try (Connection conn = dbManager.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, jobId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("status");
            }
        }
        return null;
    }
}