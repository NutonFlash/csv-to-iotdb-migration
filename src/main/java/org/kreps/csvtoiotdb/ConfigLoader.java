package org.kreps.csvtoiotdb;

import java.io.File;
import java.io.IOException;

import org.kreps.csvtoiotdb.configs.MigrationConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ConfigLoader {
    private final MigrationConfig config;

    public ConfigLoader(String configPath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        this.config = mapper.readValue(new File(configPath), MigrationConfig.class);
    }

    public MigrationConfig getConfig() {
        return config;
    }
}
