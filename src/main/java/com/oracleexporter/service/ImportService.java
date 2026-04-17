package com.oracleexporter.service;

import com.oracleexporter.model.DbObjectType;
import com.oracleexporter.util.SqlScriptRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import java.util.function.Consumer;

public class ImportService {

    public void importFromExportFolder(Connection connection, Path exportRoot, Consumer<String> logger) throws IOException, SQLException {
        if (connection == null) throw new IllegalArgumentException("connection is null");
        if (exportRoot == null) throw new IllegalArgumentException("exportRoot is null");
        if (!Files.isDirectory(exportRoot)) throw new IllegalArgumentException("exportRoot is not a directory: " + exportRoot);
        Consumer<String> log = logger == null ? s -> {} : logger;

        // 1) TABLE DDL
        log.accept("[DDL] Importing TABLE DDL...");
        runAllSqlInDir(connection, exportRoot.resolve(typeFolder(DbObjectType.TABLE)).resolve("ddl"), log, false);

        // 2) TABLE DATA (optional)
        Path tableData = exportRoot.resolve(typeFolder(DbObjectType.TABLE)).resolve("data");
        if (Files.isDirectory(tableData)) {
            log.accept("[DATA] Importing TABLE data...");
            runAllSqlInDir(connection, tableData, log, true);
        }

        // 3) Views, MViews, Procedures DDL
        log.accept("[DDL] Importing VIEW DDL...");
        runAllSqlInDir(connection, exportRoot.resolve(typeFolder(DbObjectType.VIEW)).resolve("ddl"), log, false);
        log.accept("[DDL] Importing MATERIALIZED_VIEW DDL...");
        runAllSqlInDir(connection, exportRoot.resolve(typeFolder(DbObjectType.MATERIALIZED_VIEW)).resolve("ddl"), log, false);
        log.accept("[DDL] Importing PROCEDURE DDL...");
        runAllSqlInDir(connection, exportRoot.resolve(typeFolder(DbObjectType.PROCEDURE)).resolve("ddl"), log, false);
    }

    private void runAllSqlInDir(Connection connection, Path dir, Consumer<String> log, boolean isData) throws IOException, SQLException {
        if (!Files.isDirectory(dir)) return;
        try (Stream<Path> s = Files.list(dir)) {
            List<Path> files = s
                    .filter(p -> Files.isRegularFile(p))
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".sql"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString().toLowerCase()))
                    .toList();
            for (Path f : files) {
                String name = f.getFileName().toString();
                String objName = name.toUpperCase(Locale.ROOT).endsWith(".SQL")
                        ? name.substring(0, name.length() - 4)
                        : name;

                if (isData) {
                    Savepoint sp = connection.setSavepoint("sp_" + objName);
                    try {
                        log.accept("[DATA] " + objName + " ...");
                        SqlScriptRunner.runFile(connection, f);
                        log.accept("[DATA][OK] " + objName);
                    } catch (SQLException ex) {
                        try {
                            connection.rollback(sp);
                        } catch (Exception ignored) {}
                        log.accept("[DATA][FAIL] " + objName + " :: " + safeSqlError(ex));
                        // skip this table, continue next
                    } finally {
                        try {
                            connection.releaseSavepoint(sp);
                        } catch (Exception ignored) {}
                    }
                } else {
                    try {
                        log.accept("[DDL] " + objName + " ...");
                        SqlScriptRunner.runFile(connection, f);
                        log.accept("[DDL][OK] " + objName);
                    } catch (SQLException ex) {
                        log.accept("[DDL][FAIL] " + objName + " :: " + safeSqlError(ex));
                        // continue next file
                    }
                }
            }
        }
    }

    private static String safeSqlError(SQLException ex) {
        if (ex == null) return "Unknown SQL error";
        String msg = ex.getMessage();
        String code = ex.getSQLState();
        int err = ex.getErrorCode();
        String base = (msg == null || msg.isBlank()) ? ex.getClass().getSimpleName() : msg;
        if (code != null && !code.isBlank()) {
            return base + " (SQLState=" + code + ", ErrorCode=" + err + ")";
        }
        return base + " (ErrorCode=" + err + ")";
    }

    private static String typeFolder(DbObjectType type) {
        return switch (type) {
            case TABLE -> "table";
            case VIEW -> "view";
            case MATERIALIZED_VIEW -> "materialized_view";
            case PROCEDURE -> "procedure";
        };
    }
}

