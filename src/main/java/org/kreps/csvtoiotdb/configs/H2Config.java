package org.kreps.csvtoiotdb.configs;

public class H2Config {
    private String url;
    private String username;
    private String password;
    private boolean enableConsole;
    private int consolePort;

    public H2Config() {
    }

    public H2Config(String url, String username, String password, boolean enableConsole, int consolePort) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.enableConsole = enableConsole;
        this.consolePort = consolePort;
    }
    
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
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

    public boolean isEnableConsole() {
        return enableConsole;
    }

    public void setEnableConsole(boolean enableConsole) {
        this.enableConsole = enableConsole;
    }

    public int getConsolePort() {
        return consolePort;
    }

    public void setConsolePort(int consolePort) {
        this.consolePort = consolePort;
    }
}