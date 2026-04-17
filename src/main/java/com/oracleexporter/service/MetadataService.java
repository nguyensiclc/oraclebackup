package com.oracleexporter.service;

import com.oracleexporter.model.ColumnInfo;
import com.oracleexporter.model.DbObject;
import com.oracleexporter.model.DbObjectType;
import com.oracleexporter.model.TableInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class MetadataService {

    public boolean isSchemaEmpty(Connection connection) throws SQLException {
        String sql = """
                SELECT COUNT(*)
                FROM USER_OBJECTS
                WHERE OBJECT_TYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'PROCEDURE')
                  AND OBJECT_NAME NOT LIKE 'BIN$%'
                """;
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1) == 0;
        }
    }

    public List<DbObject> listObjects(Connection connection) throws SQLException {
        List<DbObject> objects = new ArrayList<>();

        String sql = """
                SELECT object_type, object_name
                FROM (
                    SELECT 'TABLE' AS object_type, TABLE_NAME AS object_name FROM USER_TABLES
                    UNION ALL
                    SELECT 'VIEW' AS object_type, VIEW_NAME AS object_name FROM USER_VIEWS
                    UNION ALL
                    SELECT 'MATERIALIZED_VIEW' AS object_type, MVIEW_NAME AS object_name FROM USER_MVIEWS
                    UNION ALL
                    SELECT 'PROCEDURE' AS object_type, OBJECT_NAME AS object_name
                    FROM USER_OBJECTS
                    WHERE OBJECT_TYPE = 'PROCEDURE'
                )
                ORDER BY object_type, object_name
                """;

        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String type = rs.getString(1);
                String name = rs.getString(2);
                DbObjectType t = parseType(type);
                if (t != null && name != null && !name.isBlank()) {
                    objects.add(new DbObject(t, name));
                }
            }
        }

        return objects;
    }

    public List<String> listTables(Connection connection) throws SQLException {
        List<String> tables = new ArrayList<>();

        String sql = "SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME";
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String tableName = rs.getString(1);
                if (tableName != null) tables.add(tableName);
            }
        }

        return tables;
    }

    public String loadObjectDdl(Connection connection, String schema, DbObject object) throws SQLException {
        if (connection == null) throw new IllegalArgumentException("connection is null");
        if (object == null) throw new IllegalArgumentException("object is null");

        String owner = (schema == null || schema.isBlank()) ? connection.getSchema() : schema.trim();
        if (owner == null || owner.isBlank()) {
            owner = null; // let DBMS_METADATA default if possible
        }

        String ddl = tryLoadDdlFromDbmsMetadata(connection, owner, object);
        if (ddl != null && !ddl.isBlank()) return ddl;

        // Fallbacks when DBMS_METADATA is not accessible.
        return switch (object.getType()) {
            case VIEW -> fallbackViewDdl(connection, object.getName());
            case MATERIALIZED_VIEW -> fallbackMviewDdl(connection, object.getName());
            case PROCEDURE -> fallbackProcedureDdl(connection, object.getName());
            case TABLE -> null; // tables are handled by existing TableInfo-based exporter
        };
    }

    public TableInfo loadTableInfo(Connection connection, String tableName) throws SQLException {
        String normalized = tableName == null ? null : tableName.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException("tableName is blank");
        }

        String t = normalized.toUpperCase(Locale.ROOT);
        Set<String> pkCols = loadPrimaryKeyColumns(connection, t);

        TableInfo tableInfo = new TableInfo(t);

        String sql = """
                SELECT
                    c.COLUMN_ID,
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.DATA_LENGTH,
                    c.DATA_PRECISION,
                    c.DATA_SCALE,
                    c.NULLABLE,
                    cc.COMMENTS
                FROM USER_TAB_COLUMNS c
                LEFT JOIN USER_COL_COMMENTS cc
                    ON cc.TABLE_NAME = c.TABLE_NAME
                   AND cc.COLUMN_NAME = c.COLUMN_NAME
                WHERE c.TABLE_NAME = ?
                ORDER BY c.COLUMN_ID
                """;

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, t);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String colName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    Integer len = getNullableInt(rs, "DATA_LENGTH");
                    Integer precision = getNullableInt(rs, "DATA_PRECISION");
                    Integer scale = getNullableInt(rs, "DATA_SCALE");
                    String nullableFlag = rs.getString("NULLABLE");
                    boolean nullable = "Y".equalsIgnoreCase(nullableFlag);
                    String comment = rs.getString("COMMENTS");

                    boolean isPk = colName != null && pkCols.contains(colName.toUpperCase(Locale.ROOT));

                    tableInfo.addColumn(new ColumnInfo(
                            colName,
                            dataType,
                            len,
                            precision,
                            scale,
                            nullable,
                            isPk,
                            comment
                    ));
                }
            }
        }

        return tableInfo;
    }

    private Set<String> loadPrimaryKeyColumns(Connection connection, String tableNameUpper) throws SQLException {
        String sql = """
                SELECT acc.COLUMN_NAME
                FROM USER_CONSTRAINTS ac
                JOIN USER_CONS_COLUMNS acc
                  ON acc.CONSTRAINT_NAME = ac.CONSTRAINT_NAME
                WHERE ac.CONSTRAINT_TYPE = 'P'
                  AND ac.TABLE_NAME = ?
                """;

        Set<String> cols = new HashSet<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableNameUpper);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String c = rs.getString("COLUMN_NAME");
                    if (c != null) cols.add(c.toUpperCase(Locale.ROOT));
                }
            }
        }
        return cols;
    }

    private static Integer getNullableInt(ResultSet rs, String column) throws SQLException {
        int val = rs.getInt(column);
        return rs.wasNull() ? null : val;
    }

    private static DbObjectType parseType(String raw) {
        if (raw == null) return null;
        String u = raw.trim().toUpperCase(Locale.ROOT);
        return switch (u) {
            case "TABLE" -> DbObjectType.TABLE;
            case "VIEW" -> DbObjectType.VIEW;
            case "MATERIALIZED_VIEW" -> DbObjectType.MATERIALIZED_VIEW;
            case "PROCEDURE" -> DbObjectType.PROCEDURE;
            default -> null;
        };
    }

    private static String tryLoadDdlFromDbmsMetadata(Connection connection, String schema, DbObject object) {
        String objType = switch (object.getType()) {
            case VIEW -> "VIEW";
            case MATERIALIZED_VIEW -> "MATERIALIZED_VIEW";
            case PROCEDURE -> "PROCEDURE";
            case TABLE -> "TABLE";
        };

        // Using DBMS_METADATA.GET_DDL(object_type, name, schema)
        String sql = (schema == null)
                ? "SELECT DBMS_METADATA.GET_DDL(?, ?) FROM dual"
                : "SELECT DBMS_METADATA.GET_DDL(?, ?, ?) FROM dual";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, objType);
            ps.setString(2, object.getName());
            if (schema != null) {
                ps.setString(3, schema.toUpperCase(Locale.ROOT));
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String ddl;
                    if (rs.getMetaData().getColumnType(1) == Types.CLOB) {
                        ddl = rs.getString(1);
                    } else {
                        ddl = rs.getString(1);
                    }
                    return ddl;
                }
            }
        } catch (Exception ignored) {
            return null;
        }
        return null;
    }

    private static String fallbackViewDdl(Connection connection, String viewNameUpper) throws SQLException {
        String sql = "SELECT TEXT FROM USER_VIEWS WHERE VIEW_NAME = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, viewNameUpper);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                String text = rs.getString(1);
                if (text == null) return null;
                return "CREATE OR REPLACE VIEW " + quoteIdent(viewNameUpper) + " AS\n" + text.trim() + ";\n";
            }
        }
    }

    private static String fallbackMviewDdl(Connection connection, String mviewNameUpper) throws SQLException {
        String sql = "SELECT QUERY FROM USER_MVIEWS WHERE MVIEW_NAME = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, mviewNameUpper);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                String query = rs.getString(1);
                if (query == null) return null;
                return "CREATE MATERIALIZED VIEW " + quoteIdent(mviewNameUpper) + " AS\n" + query.trim() + ";\n";
            }
        }
    }

    private static String fallbackProcedureDdl(Connection connection, String procNameUpper) throws SQLException {
        String sql = """
                SELECT TEXT
                FROM USER_SOURCE
                WHERE TYPE = 'PROCEDURE'
                  AND NAME = ?
                ORDER BY LINE
                """;
        StringBuilder sb = new StringBuilder();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, procNameUpper);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String line = rs.getString(1);
                    if (line != null) sb.append(line);
                }
            }
        }
        String out = sb.toString().trim();
        return out.isEmpty() ? null : (out + "\n/\n");
    }

    private static String quoteIdent(String ident) {
        if (ident == null) return "\"\"";
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }
}

