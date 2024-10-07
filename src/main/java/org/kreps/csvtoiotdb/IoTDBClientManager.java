package org.kreps.csvtoiotdb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBConnection;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBSettings;

/**
 * Manages a pool of IoTDB session connections.
 */
public class IoTDBClientManager {
    private final List<IoTDBConnection> connections;
    private final int connectionPoolSize;
    private final BlockingQueue<Session> availableSessions;
    private final Map<Session, IoTDBConnection> sessionConnectionMap;
    private final int maxRetriesCount;
    private final long retryIntervalInMs;

    /**
     * Constructs an IoTDBClientManager instance.
     *
     * @param iotdbSettings The IoTDB settings containing connection details and
     *                      pool configurations.
     * @throws IoTDBConnectionException If a connection cannot be established.
     */
    public IoTDBClientManager(IoTDBSettings iotdbSettings) throws IoTDBConnectionException {
        if (iotdbSettings.getConnections() == null || iotdbSettings.getConnections().isEmpty()) {
            throw new IllegalArgumentException("Connections list cannot be null or empty");
        }
        this.connections = iotdbSettings.getConnections();
        this.connectionPoolSize = iotdbSettings.getConnectionPoolSize();
        this.availableSessions = new LinkedBlockingQueue<>();
        this.sessionConnectionMap = new ConcurrentHashMap<>();

        this.maxRetriesCount = iotdbSettings.getMaxRetriesCount();
        this.retryIntervalInMs = iotdbSettings.getRetryIntervalInMs();

        // Initialize sessions
        for (IoTDBConnection conn : connections) {
            for (int i = 0; i < connectionPoolSize; i++) {
                Session session = new Session.Builder()
                        .host(conn.getHost())
                        .port(conn.getPort())
                        .username(conn.getUsername())
                        .password(conn.getPassword())
                        .maxRetryCount(this.maxRetriesCount)
                        .retryIntervalInMs(this.retryIntervalInMs)
                        .build();
                session.open();
                availableSessions.offer(session);
                sessionConnectionMap.put(session, conn);
            }
        }
    }

    /**
     * Acquires a session from the pool.
     *
     * @return An available IoTDB session.
     * @throws InterruptedException If interrupted while waiting.
     */
    public Session acquireSession() throws InterruptedException {
        return availableSessions.take();    
    }

    /**
     * Releases a session back to the pool.
     *
     * @param session The IoTDB session to release.
     */
    public void releaseSession(Session session) {
        if (session != null) {
            availableSessions.offer(session);
        }
    }

    /**
     * Closes all sessions and cleans up resources.
     */
    public void close() {
        for (Session session : sessionConnectionMap.keySet()) {
            try {
                session.close();
            } catch (Exception e) {
                // Replace with proper logging
                e.printStackTrace();
            }
        }
    }
}
