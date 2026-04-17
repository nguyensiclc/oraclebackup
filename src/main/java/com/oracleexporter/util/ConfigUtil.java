package com.oracleexporter.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

    /**
     * Best-effort initialization: if config file doesn't exist, create it with provided defaults.
     * Always returns a Properties instance (loaded from disk if possible, otherwise defaults).
     */
    public static Properties loadOrCreate(Properties defaults) {
        Properties base = new Properties();
        if (defaults != null) {
            base.putAll(defaults);
        }

        Path path = configPath();
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path.getParent());
                try (OutputStream out = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)) {
                    base.store(out, "OracleSchemaExporter config");
                }
            } catch (IOException ignored) {
                // best-effort config; ignore init failures
            }
            return base;
        }

        Properties loaded = new Properties();
        loaded.putAll(base);
        try (InputStream in = Files.newInputStream(path)) {
            loaded.load(in);
        } catch (IOException ignored) {
            // best-effort config; ignore load failures and fall back to defaults
        }
        return loaded;
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

