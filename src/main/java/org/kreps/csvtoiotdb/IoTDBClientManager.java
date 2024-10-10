package org.kreps.csvtoiotdb;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.session.pool.SessionPool;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBConnection;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBSettings;

/**
 * Manages a pool of IoTDB session connections using the native IoTDB session
 * pool.
 */
public class IoTDBClientManager {
    private final List<SessionPool> sessionPools;

    /**
     * Constructs an IoTDBClientManager instance.
     *
     * @param iotdbSettings The IoTDB settings containing connection details and
     *                      pool configurations.
     */
    public IoTDBClientManager(IoTDBSettings iotdbSettings) {
        if (iotdbSettings.getConnections() == null || iotdbSettings.getConnections().isEmpty()) {
            throw new IllegalArgumentException("Connections list cannot be null or empty");
        }

        this.sessionPools = new ArrayList<>();

        for (IoTDBConnection conn : iotdbSettings.getConnections()) {
            SessionPool sessionPool = new SessionPool.Builder()
                    .host(conn.getHost())
                    .port(conn.getPort())
                    .user(conn.getUsername())
                    .password(conn.getPassword())
                    .maxSize(iotdbSettings.getConnectionPoolSize())
                    .build();
            sessionPools.add(sessionPool);
        }
    }

    /**
     * Acquires a session from the pool.
     *
     * @return An available IoTDB session.
     */
    private int currentPoolIndex = 0;

    public synchronized SessionPool acquireSession() {
        // Round-robin strategy to acquire a session from one of the pools
        SessionPool sessionPool = sessionPools.get(currentPoolIndex);
        currentPoolIndex = (currentPoolIndex + 1) % sessionPools.size();
        return sessionPool;
    }

    /**
     * Closes all sessions and cleans up resources.
     */
    public void close() {
        for (SessionPool pool : sessionPools) {
            pool.close();
        }
    }
}
