package com.oracleexporter.service;

import com.oracleexporter.model.ColumnInfo;
import com.oracleexporter.model.TableInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class MetadataService {

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
}

