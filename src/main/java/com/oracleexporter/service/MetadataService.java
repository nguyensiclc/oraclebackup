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

    /**
     * True when the connected user schema has no objects (except recycle-bin trash {@code BIN$%}).
     */
    public boolean isSchemaEmpty(Connection connection) throws SQLException {
        return countUserObjectsExcludingRecycleBin(connection) == 0;
    }

    /**
     * Ensures the import target schema is empty: no tables, views, procedures, sequences, etc.
     * Recycle-bin objects ({@code BIN$%}) are ignored.
     *
     * @throws IllegalStateException if any user object exists
     */
    public void requireSchemaEmptyForImport(Connection connection) throws SQLException {
        int total = countUserObjectsExcludingRecycleBin(connection);
        if (total == 0) {
            return;
        }
        String schema = currentSchemaLabel(connection);
        String examples = formatSampleObjects(connection, 12);
        String msg = "Schema \"" + schema + "\" is not empty (" + total
                + " object(s)). Import only runs into an empty schema.";
        if (!examples.isBlank()) {
            msg += " Examples: " + examples + (total > 12 ? " ..." : "");
        }
        throw new IllegalStateException(msg);
    }

    private static int countUserObjectsExcludingRecycleBin(Connection connection) throws SQLException {
        String sql = """
                SELECT COUNT(*)
                FROM USER_OBJECTS
                WHERE OBJECT_NAME NOT LIKE 'BIN$%'
                """;
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static String currentSchemaLabel(Connection connection) throws SQLException {
        try {
            String s = connection.getSchema();
            if (s != null && !s.isBlank()) {
                return s.trim();
            }
        } catch (Exception ignored) {}
        return "(current user)";
    }

    private static String formatSampleObjects(Connection connection, int limit) throws SQLException {
        if (limit <= 0) return "";
        String sql = """
                SELECT object_type, object_name
                FROM USER_OBJECTS
                WHERE OBJECT_NAME NOT LIKE 'BIN$%'
                ORDER BY object_type, object_name
                FETCH FIRST ? ROWS ONLY
                """;
        StringBuilder sb = new StringBuilder();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (sb.length() > 0) sb.append("; ");
                    sb.append(rs.getString(1)).append(' ').append(rs.getString(2));
                }
            }
        }
        return sb.toString();
    }

    public List<DbObject> listObjects(Connection connection) throws SQLException {
        List<DbObject> objects = new ArrayList<>();
        /** Index names that implement PRIMARY KEY constraints — skip as separate INDEX exports (already in CREATE TABLE). */
        Set<String> pkBackingIndexes = loadPrimaryKeyBackingIndexNames(connection);

        String sql = """
                SELECT object_type, object_name, subobject_name
                FROM user_objects
                WHERE object_name NOT LIKE 'BIN$%'
                ORDER BY object_type, object_name, subobject_name NULLS LAST
                """;

        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String ot = rs.getString(1);
                String on = rs.getString(2);
                String sub = rs.getString(3);
                DbObjectType t = parseOracleObjectType(ot);
                if (t == null || on == null || on.isBlank()) {
                    continue;
                }
                if (t == DbObjectType.INDEX_PARTITION || t == DbObjectType.TABLE_PARTITION) {
                    if (sub == null || sub.isBlank()) {
                        continue;
                    }
                }
                if ((t == DbObjectType.INDEX || t == DbObjectType.INDEX_PARTITION)
                        && pkBackingIndexes.contains(on.trim().toUpperCase(Locale.ROOT))) {
                    continue;
                }
                objects.add(new DbObject(t, on, sub));
            }
        }

        return objects;
    }

    /**
     * Names of indexes that enforce PRIMARY KEY constraints. Exporting them again produces CREATE INDEX DDL
     * that collides with the unique index created by {@code CREATE TABLE ... PRIMARY KEY} on import (ORA-00955).
     */
    private static Set<String> loadPrimaryKeyBackingIndexNames(Connection connection) throws SQLException {
        Set<String> names = new HashSet<>();
        String sql = """
                SELECT DISTINCT i.index_name
                FROM user_indexes i
                INNER JOIN user_constraints uc
                        ON uc.table_name = i.table_name
                       AND uc.constraint_type = 'P'
                       AND (
                            (uc.index_name IS NOT NULL AND uc.index_name = i.index_name)
                         OR (uc.index_name IS NULL AND uc.constraint_name = i.index_name)
                       )
                """;
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String n = rs.getString(1);
                if (n != null && !n.isBlank()) {
                    names.add(n.trim().toUpperCase(Locale.ROOT));
                }
            }
        }
        return names;
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
            case FUNCTION -> fallbackSourceTypeDdl(connection, "FUNCTION", object.getName());
            case PACKAGE -> fallbackSourceTypeDdl(connection, "PACKAGE", object.getName());
            case PACKAGE_BODY -> fallbackSourceTypeDdl(connection, "PACKAGE BODY", object.getName());
            case SYNONYM -> fallbackSynonymDdl(connection, object.getName());
            case TABLE -> null;
            default -> null;
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
                    c.CHAR_LENGTH,
                    c.CHAR_USED,
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
                    Integer dataLen = getNullableInt(rs, "DATA_LENGTH");
                    Integer charLen = getNullableInt(rs, "CHAR_LENGTH");
                    String charUsed = rs.getString("CHAR_USED");
                    Integer precision = getNullableInt(rs, "DATA_PRECISION");
                    Integer scale = getNullableInt(rs, "DATA_SCALE");
                    String nullableFlag = rs.getString("NULLABLE");
                    boolean nullable = "Y".equalsIgnoreCase(nullableFlag);
                    String comment = rs.getString("COMMENTS");

                    boolean isPk = colName != null && pkCols.contains(colName.toUpperCase(Locale.ROOT));

                    Integer len = resolveDeclaredLength(dataType, dataLen, charLen, charUsed);
                    boolean charSemantics = isOracleVarcharCharSemantics(dataType, charUsed);
                    tableInfo.addColumn(new ColumnInfo(
                            colName,
                            dataType,
                            len,
                            precision,
                            scale,
                            nullable,
                            isPk,
                            charSemantics,
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

    private static Integer resolveDeclaredLength(String dataType, Integer dataLen, Integer charLen, String charUsed) {
        if (dataType == null) return dataLen;
        String u = dataType.trim().toUpperCase(Locale.ROOT);
        if (!u.contains("CHAR")) return dataLen;
        // Oracle: if CHAR_USED = 'C' then length is in characters; use CHAR_LENGTH.
        if (charUsed != null && charUsed.trim().equalsIgnoreCase("C")) {
            return (charLen != null && charLen > 0) ? charLen : dataLen;
        }
        // BYTE semantics or unknown -> keep DATA_LENGTH
        return dataLen;
    }

    /** {@code CHAR_USED = 'C'} for {@code VARCHAR2}/{@code CHAR} — DDL must include {@code CHAR} keyword. */
    private static boolean isOracleVarcharCharSemantics(String dataType, String charUsed) {
        if (dataType == null || charUsed == null) return false;
        String u = dataType.trim().toUpperCase(Locale.ROOT);
        if (!"VARCHAR2".equals(u) && !"CHAR".equals(u)) {
            return false;
        }
        return charUsed.trim().equalsIgnoreCase("C");
    }

    private static DbObjectType parseOracleObjectType(String raw) {
        if (raw == null) return null;
        return switch (raw.trim().toUpperCase(Locale.ROOT)) {
            case "TABLE" -> DbObjectType.TABLE;
            case "VIEW" -> DbObjectType.VIEW;
            case "MATERIALIZED VIEW" -> DbObjectType.MATERIALIZED_VIEW;
            case "SYNONYM" -> DbObjectType.SYNONYM;
            case "PROCEDURE" -> DbObjectType.PROCEDURE;
            case "FUNCTION" -> DbObjectType.FUNCTION;
            case "PACKAGE" -> DbObjectType.PACKAGE;
            case "PACKAGE BODY" -> DbObjectType.PACKAGE_BODY;
            case "SEQUENCE" -> DbObjectType.SEQUENCE;
            case "INDEX" -> DbObjectType.INDEX;
            case "INDEX PARTITION" -> DbObjectType.INDEX_PARTITION;
            case "TABLE PARTITION" -> DbObjectType.TABLE_PARTITION;
            case "LOB" -> DbObjectType.LOB;
            case "TYPE" -> DbObjectType.TYPE;
            case "DATABASE LINK" -> DbObjectType.DATABASE_LINK;
            default -> null;
        };
    }

    private static String tryLoadDdlFromDbmsMetadata(Connection connection, String schema, DbObject object) {
        String metaType = toMetadataApiType(object.getType());
        if (metaType == null) {
            return null;
        }
        String owner = schema == null || schema.isBlank() ? null : schema.toUpperCase(Locale.ROOT);

        if (object.getType() == DbObjectType.INDEX_PARTITION
                || object.getType() == DbObjectType.TABLE_PARTITION) {
            String sub = object.getSubObjectName();
            if (sub != null && !sub.isBlank()) {
                String d = executeGetDdl(connection, metaType, sub, owner);
                if (ddlNonBlank(d)) {
                    return d;
                }
                d = executeGetDdl(connection, metaType, object.getName() + "." + sub, owner);
                if (ddlNonBlank(d)) {
                    return d;
                }
            }
        }

        return executeGetDdl(connection, metaType, object.getName(), owner);
    }

    private static boolean ddlNonBlank(String ddl) {
        return ddl != null && !ddl.isBlank();
    }

    private static String toMetadataApiType(DbObjectType t) {
        return switch (t) {
            case TABLE -> "TABLE";
            case VIEW -> "VIEW";
            case MATERIALIZED_VIEW -> "MATERIALIZED_VIEW";
            case SYNONYM -> "SYNONYM";
            case PROCEDURE -> "PROCEDURE";
            case FUNCTION -> "FUNCTION";
            case PACKAGE -> "PACKAGE";
            case PACKAGE_BODY -> "PACKAGE_BODY";
            case SEQUENCE -> "SEQUENCE";
            case INDEX -> "INDEX";
            case INDEX_PARTITION -> "INDEX_PARTITION";
            case TABLE_PARTITION -> "TABLE_PARTITION";
            case LOB -> "LOB";
            case TYPE -> "TYPE";
            case DATABASE_LINK -> "DB_LINK";
        };
    }

    private static String executeGetDdl(Connection connection, String objectType, String objectName, String schemaUpper) {
        String sql = (schemaUpper == null)
                ? "SELECT DBMS_METADATA.GET_DDL(?, ?) FROM dual"
                : "SELECT DBMS_METADATA.GET_DDL(?, ?, ?) FROM dual";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, objectType);
            ps.setString(2, objectName);
            if (schemaUpper != null) {
                ps.setString(3, schemaUpper);
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
        return fallbackSourceTypeDdl(connection, "PROCEDURE", procNameUpper);
    }

    private static String fallbackSourceTypeDdl(Connection connection, String sourceType, String nameUpper) throws SQLException {
        String sql = """
                SELECT TEXT
                FROM USER_SOURCE
                WHERE TYPE = ?
                  AND NAME = ?
                ORDER BY LINE
                """;
        StringBuilder sb = new StringBuilder();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, sourceType);
            ps.setString(2, nameUpper);
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

    private static String fallbackSynonymDdl(Connection connection, String synonymNameUpper) throws SQLException {
        String sql = """
                SELECT table_owner, table_name, db_link
                FROM user_synonyms
                WHERE synonym_name = ?
                """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, synonymNameUpper);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                String toOwner = rs.getString(1);
                String toTable = rs.getString(2);
                String dblink = rs.getString(3);
                if (toTable == null) return null;
                String target = (toOwner != null && !toOwner.isBlank())
                        ? (quoteIdent(toOwner.trim()) + "." + quoteIdent(toTable.trim()))
                        : quoteIdent(toTable.trim());
                if (dblink != null && !dblink.isBlank()) {
                    target += "@" + dblink.trim();
                }
                return "CREATE OR REPLACE SYNONYM " + quoteIdent(synonymNameUpper) + " FOR " + target + ";\n";
            }
        }
    }

    private static String quoteIdent(String ident) {
        if (ident == null) return "\"\"";
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }
}

