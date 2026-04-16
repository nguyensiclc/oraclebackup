package com.oracleexporter.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public final class ConfigUtil {
    private static final String APP_DIR = ".oracle-schema-exporter";
    private static final String FILE_NAME = "config.properties";

    private ConfigUtil() {}

    public static Path configPath() {
        String home = System.getProperty("user.home");
        Path dir = Paths.get(home).resolve(APP_DIR);
        return dir.resolve(FILE_NAME);
    }

    public static Properties load() {
        Path path = configPath();
        Properties p = new Properties();
        if (!Files.exists(path)) return p;
        try (InputStream in = Files.newInputStream(path)) {
            p.load(in);
        } catch (IOException ignored) {
            // best-effort config; ignore load failures
        }
        return p;
    }

    public static void save(Properties p) {
        Path path = configPath();
        try {
            Files.createDirectories(path.getParent());
            try (OutputStream out = Files.newOutputStream(path)) {
                p.store(out, "OracleSchemaExporter config");
            }
        } catch (IOException ignored) {
            // best-effort config; ignore save failures
        }
    }
}

