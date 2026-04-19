package com.oracleexporter.service;

import com.oracleexporter.model.ColumnInfo;
import com.oracleexporter.model.TableInfo;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Clob;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.concurrent.CancellationException;

public class ExportService {

    private static final Charset UTF8 = StandardCharsets.UTF_8;
    /** Matches Oracle mask {@code YYYY-MM-DD HH24:MI:SS} for {@code TO_DATE(..., '...')}. */
    private static final DateTimeFormatter DATE_TIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String ORACLE_DATE_TIME_MASK = "YYYY-MM-DD HH24:MI:SS";

    public Path exportSql(TableInfo table, Path file) throws IOException {
        String ddl = toCreateTableSql(table);
        try (BufferedWriter w = Files.newBufferedWriter(file, UTF8)) {
            w.write(ddl);
        }
        return file;
    }

    public Path exportRawSql(String sql, Path file) throws IOException {
        if (sql == null) throw new IllegalArgumentException("sql is null");
        try (BufferedWriter w = Files.newBufferedWriter(file, UTF8)) {
            w.write(sql);
            if (!sql.endsWith("\n")) {
                w.newLine();
            }
        }
        return file;
    }

    /**
     * @param maxRows {@code null} or non-positive = export all rows; otherwise cap at this many rows (Oracle {@code FETCH FIRST}).
     * @param cancelled if non-null, {@link BooleanSupplier#getAsBoolean()} = true means stop; checked every 500 output rows
     */
    public Path exportDataSql(Connection connection, String tableName, Path file, Integer maxRows) throws IOException, SQLException {
        return exportDataSql(connection, tableName, file, maxRows, null);
    }

    public Path exportDataSql(
            Connection connection, String tableName, Path file, Integer maxRows, BooleanSupplier cancelled)
            throws IOException, SQLException {
        if (connection == null) throw new IllegalArgumentException("connection is null");
        if (tableName == null || tableName.isBlank()) throw new IllegalArgumentException("tableName is blank");

        String t = tableName.trim().toUpperCase(Locale.ROOT);
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(quoteIdent(t));
        boolean limited = maxRows != null && maxRows > 0;
        if (limited) {
            sql.append(" FETCH FIRST ").append(maxRows).append(" ROWS ONLY");
        }

        try (BufferedWriter w = Files.newBufferedWriter(file, UTF8);
             PreparedStatement ps = connection.prepareStatement(sql.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = ps.executeQuery()) {

            ps.setFetchSize(1000);

            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();

            String colList = buildQuotedColumnList(md, colCount);
            String insertPrefix = "INSERT INTO " + quoteIdent(t) + " (" + colList + ") VALUES (";

            long row = 0;
            while (rs.next()) {
                if (row % 500 == 0 && cancelled != null && cancelled.getAsBoolean()) {
                    throw new CancellationException("Operation stopped");
                }
                w.write(insertPrefix);
                for (int i = 1; i <= colCount; i++) {
                    if (i > 1) w.write(", ");
                    int jdbcType = md.getColumnType(i);
                    w.write(toSqlLiteral(rs, i, jdbcType));
                }
                w.write(");");
                w.newLine();
                row++;

                if (row % 500 == 0) {
                    w.flush();
                }
            }

            w.newLine();
            w.write("COMMIT;");
            w.newLine();
        }

        return file;
    }

    public String toCreateTableSql(TableInfo table) {
        String tableName = table.getName();
        List<ColumnInfo> cols = table.getColumns();

        String pkClause = buildPkClause(tableName, cols);

        String body = cols.stream()
                .map(c -> "  " + quoteIdent(c.getName()) + " " + formatType(c) + (c.isNullable() ? "" : " NOT NULL"))
                .collect(Collectors.joining(",\n"));

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(quoteIdent(tableName)).append(" (\n");
        sb.append(body);
        if (pkClause != null) {
            sb.append(",\n  ").append(pkClause);
        }
        sb.append("\n);\n\n");

        for (ColumnInfo c : cols) {
            if (c.getComment() != null && !c.getComment().isBlank()) {
                sb.append("COMMENT ON COLUMN ")
                        .append(quoteIdent(tableName))
                        .append(".")
                        .append(quoteIdent(c.getName()))
                        .append(" IS '")
                        .append(escapeSqlString(c.getComment()))
                        .append("';\n");
            }
        }
        sb.append("\n");

        return sb.toString();
    }

    private static String buildPkClause(String tableName, List<ColumnInfo> cols) {
        List<String> pkCols = cols.stream()
                .filter(ColumnInfo::isPrimaryKey)
                .map(ColumnInfo::getName)
                .filter(n -> n != null && !n.isBlank())
                .map(ExportService::quoteIdent)
                .toList();

        if (pkCols.isEmpty()) return null;
        return "CONSTRAINT " + quoteIdent(("PK_" + tableName).toUpperCase(Locale.ROOT)) +
                " PRIMARY KEY (" + String.join(", ", pkCols) + ")";
    }

    private static String formatType(ColumnInfo c) {
        String dt = c.getDataType();
        if (dt == null) return "";
        String u = dt.toUpperCase(Locale.ROOT);

        // VARCHAR2/CHAR: always emit CHAR semantics in DDL so length is in characters (not bytes).
        if ("VARCHAR2".equals(u) || "CHAR".equals(u)) {
            Integer len = c.getLength();
            if (len != null && len > 0) {
                return u + "(" + len + " CHAR)";
            }
            return u;
        }

        // NVARCHAR2, NCHAR: length is always in national-character units
        if (u.contains("CHAR")) {
            Integer len = c.getLength();
            if (len != null && len > 0) return u + "(" + len + ")";
            return u;
        }

        if ("NUMBER".equals(u)) {
            Integer p = c.getPrecision();
            Integer s = c.getScale();
            if (p == null) return "NUMBER";
            if (s == null || s == 0) return "NUMBER(" + p + ")";
            return "NUMBER(" + p + "," + s + ")";
        }

        if ("FLOAT".equals(u)) {
            Integer p = c.getPrecision();
            if (p != null) return "FLOAT(" + p + ")";
            return "FLOAT";
        }

        if ("RAW".equals(u) || "UROWID".equals(u)) {
            Integer len = c.getLength();
            if (len != null && len > 0) return u + "(" + len + ")";
        }

        return u;
    }

    private static String quoteIdent(String ident) {
        if (ident == null) return "\"\"";
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }

    private static String escapeSqlString(String s) {
        return s == null ? "" : s.replace("'", "''");
    }

    private static String buildQuotedColumnList(ResultSetMetaData md, int colCount) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= colCount; i++) {
            if (i > 1) sb.append(", ");
            sb.append(quoteIdent(md.getColumnName(i)));
        }
        return sb.toString();
    }

    private static String toSqlLiteral(ResultSet rs, int index, int jdbcType) throws SQLException, IOException {
        Object value = rs.getObject(index);
        if (value == null) return "NULL";

        // Numbers
        if (value instanceof Number n) {
            return n.toString();
        }

        // Boolean (rare in Oracle, but keep safe)
        if (value instanceof Boolean b) {
            return b ? "1" : "0";
        }

        // Date/time: TO_DATE with YYYY-MM-DD HH24:MI:SS for DATE and TIMESTAMP columns
        if (jdbcType == Types.DATE && value instanceof java.sql.Date d) {
            LocalDateTime ldt = d.toLocalDate().atStartOfDay();
            return toDateLiteral(ldt);
        }
        if ((jdbcType == Types.TIMESTAMP || jdbcType == Types.TIMESTAMP_WITH_TIMEZONE) && value instanceof Timestamp ts) {
            return toDateLiteral(ts.toLocalDateTime());
        }
        if (value instanceof OffsetDateTime odt) {
            return toDateLiteral(odt.toLocalDateTime());
        }
        if (value instanceof ZonedDateTime zdt) {
            return toDateLiteral(zdt.toLocalDateTime());
        }
        if (value instanceof LocalDateTime ldt) {
            return toDateLiteral(ldt);
        }

        // RAW/BLOB -> hextoraw
        if (jdbcType == Types.BINARY || jdbcType == Types.VARBINARY || jdbcType == Types.LONGVARBINARY || jdbcType == Types.BLOB) {
            byte[] bytes;
            if (value instanceof byte[] b) bytes = b;
            else return "NULL";
            return "HEXTORAW('" + toHex(bytes) + "')";
        }

        if (jdbcType == Types.CLOB || value instanceof Clob) {
            String s = rs.getString(index);
            return "'" + escapeSqlString(s) + "'";
        }

        // Default treat as string (VARCHAR2, CHAR, etc.)
        return "'" + escapeSqlString(rs.getString(index)) + "'";
    }

    private static String toDateLiteral(LocalDateTime ldt) {
        LocalDateTime t = ldt.truncatedTo(ChronoUnit.SECONDS);
        return "TO_DATE('" + DATE_TIME_FMT.format(t) + "', '" + ORACLE_DATE_TIME_MASK + "')";
    }

    private static String toHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "";
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString().toUpperCase(Locale.ROOT);
    }

}

