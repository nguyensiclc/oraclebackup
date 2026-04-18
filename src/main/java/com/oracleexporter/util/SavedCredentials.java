package com.oracleexporter.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

/**
 * Persists multiple (user, password) pairs in {@link Properties} as indexed keys under {@code db.credentials.*}.
 */
public final class SavedCredentials {

    private static final String COUNT = "db.credentials.count";
    private static final String PREFIX = "db.credentials.";

    private static final String LEGACY_USER = "db.user";
    private static final String LEGACY_SAVE = "db.password.save";
    private static final String LEGACY_PASSWORD = "db.password";

    private SavedCredentials() {}

    public static void migrateLegacy(Properties p) {
        if (p.getProperty(COUNT) != null) {
            return;
        }
        String u = nv(p.getProperty(LEGACY_USER));
        if (u.isEmpty()) {
            return;
        }
        if (!Boolean.parseBoolean(p.getProperty(LEGACY_SAVE, "false"))) {
            return;
        }
        String pw = p.getProperty(LEGACY_PASSWORD, "");
        p.setProperty(COUNT, "1");
        p.setProperty(PREFIX + "0.user", u);
        p.setProperty(PREFIX + "0.password", pw != null ? pw : "");
        p.remove(LEGACY_PASSWORD);
    }

    /**
     * Loads credential pairs in index order (0 .. count-1) into {@code dest}.
     */
    public static void readInto(LinkedHashMap<String, String> dest, Properties p) {
        dest.clear();
        migrateLegacy(p);
        int n = parseNonNegativeInt(p.getProperty(COUNT, "0"), 0);
        for (int i = 0; i < n; i++) {
            String u = nv(p.getProperty(PREFIX + i + ".user"));
            if (u.isEmpty()) {
                continue;
            }
            String pw = p.getProperty(PREFIX + i + ".password");
            dest.put(u, pw != null ? pw : "");
        }
    }

    public static void writeMap(Properties p, LinkedHashMap<String, String> map) {
        removeAllCredentialKeys(p);
        int i = 0;
        for (var e : map.entrySet()) {
            p.setProperty(PREFIX + i + ".user", e.getKey());
            p.setProperty(PREFIX + i + ".password", e.getValue() != null ? e.getValue() : "");
            i++;
        }
        p.setProperty(COUNT, String.valueOf(map.size()));
    }

    private static void removeAllCredentialKeys(Properties p) {
        List<String> toRemove = new ArrayList<>();
        for (String name : p.stringPropertyNames()) {
            if (name.startsWith("db.credentials.")) {
                toRemove.add(name);
            }
        }
        for (String name : toRemove) {
            p.remove(name);
        }
    }

    private static String nv(String v) {
        return v == null ? "" : v.trim();
    }

    private static int parseNonNegativeInt(String raw, int fallback) {
        try {
            int v = Integer.parseInt(raw.trim());
            return v >= 0 ? v : fallback;
        } catch (Exception e) {
            return fallback;
        }
    }
}
