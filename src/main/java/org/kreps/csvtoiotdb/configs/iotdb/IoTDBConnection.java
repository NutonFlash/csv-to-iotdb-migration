package org.kreps.csvtoiotdb.configs.iotdb;

public class IoTDBConnection {
    private String host;
    private int port;
    private String username;
    private String password;

    public IoTDBConnection() {
    }

    public IoTDBConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public IoTDBConnection(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
