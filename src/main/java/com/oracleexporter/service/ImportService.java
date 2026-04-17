package com.oracleexporter.service;

import com.oracleexporter.model.DbObjectType;
import com.oracleexporter.util.SqlScriptRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class ImportService {

    public void importFromExportFolder(Connection connection, Path exportRoot) throws IOException, SQLException {
        if (connection == null) throw new IllegalArgumentException("connection is null");
        if (exportRoot == null) throw new IllegalArgumentException("exportRoot is null");
        if (!Files.isDirectory(exportRoot)) throw new IllegalArgumentException("exportRoot is not a directory: " + exportRoot);

        // 1) TABLE DDL
        runAllSqlInDir(connection, exportRoot.resolve(typeFolder(DbObjectType.TABLE)).resolve("ddl"));

        // 2) TABLE DATA (optional)
        Path tableData = exportRoot.resolve(typeFolder(DbObjectType.TABLE)).resolve("data");
        if (Files.isDirectory(tableData)) {
            runAllSqlInDir(connection, tableData);
        }

        // 3) Views, MViews, Procedures DDL
        runAllSqlInDir(connection, exportRoot.resolve(typeFolder(DbObjectType.VIEW)).resolve("ddl"));
        runAllSqlInDir(connection, exportRoot.resolve(typeFolder(DbObjectType.MATERIALIZED_VIEW)).resolve("ddl"));
        runAllSqlInDir(connection, exportRoot.resolve(typeFolder(DbObjectType.PROCEDURE)).resolve("ddl"));
    }

    private void runAllSqlInDir(Connection connection, Path dir) throws IOException, SQLException {
        if (!Files.isDirectory(dir)) return;
        try (Stream<Path> s = Files.list(dir)) {
            List<Path> files = s
                    .filter(p -> Files.isRegularFile(p))
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".sql"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString().toLowerCase()))
                    .toList();
            for (Path f : files) {
                SqlScriptRunner.runFile(connection, f);
            }
        }
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

