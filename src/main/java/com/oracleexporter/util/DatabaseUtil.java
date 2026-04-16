package com.oracleexporter.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

public final class DatabaseUtil {
    private DatabaseUtil() {}

    public static String buildJdbcUrl(String host, int port, String serviceName) {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(serviceName, "serviceName");
        return "jdbc:oracle:thin:@//" + host.trim() + ":" + port + "/" + serviceName.trim();
    }

    public static Connection connect(String host, int port, String serviceName, String username, String password)
            throws SQLException {
        String url = buildJdbcUrl(host, port, serviceName);
        Properties props = new Properties();
        props.setProperty("user", Objects.requireNonNull(username, "username"));
        props.setProperty("password", password == null ? "" : password);
        return DriverManager.getConnection(url, props);
    }
}

