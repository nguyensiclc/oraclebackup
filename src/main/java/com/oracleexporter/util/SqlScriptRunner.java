package com.oracleexporter.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.BooleanSupplier;

import java.util.concurrent.CancellationException;

public final class SqlScriptRunner {
    private SqlScriptRunner() {}

    public static void runFile(Connection connection, Path file) throws IOException, SQLException {
        runFile(connection, file, null);
    }

    /**
     * @param cancelled when {@code true} before each statement, throws {@link CancellationException}
     */
    public static void runFile(Connection connection, Path file, BooleanSupplier cancelled) throws IOException, SQLException {
        if (connection == null) throw new IllegalArgumentException("connection is null");
        if (file == null) throw new IllegalArgumentException("file is null");
        String sql = Files.readString(file, StandardCharsets.UTF_8);
        runSql(connection, sql, cancelled);
    }

    public static void runSql(Connection connection, String script) throws SQLException {
        runSql(connection, script, null);
    }

    public static void runSql(Connection connection, String script, BooleanSupplier cancelled) throws SQLException {
        if (connection == null) throw new IllegalArgumentException("connection is null");
        if (script == null) throw new IllegalArgumentException("script is null");

        List<String> statements = splitStatements(script);
        try (Statement st = connection.createStatement()) {
            for (String s : statements) {
                throwIfCancelled(cancelled);
                String trimmed = s.trim();
                if (trimmed.isEmpty()) continue;

                String effective = stripLeadingCommentsAndBlankLines(trimmed);
                if (effective.isEmpty()) continue;

                String u = effective.toUpperCase(Locale.ROOT);
                // Common SQL*Plus directives that should not be executed via JDBC
                if (u.startsWith("SET ")
                        || u.startsWith("SPOOL ")
                        || u.equals("SPOOL")
                        || u.startsWith("PROMPT ")
                        || u.equals("PROMPT")
                        || u.equals("EXIT")
                        || u.equals("QUIT")) {
                    continue;
                }

                String jdbcSql = stripTrailingStatementDelimiter(effective);
                if (jdbcSql.isEmpty()) continue;
                st.execute(jdbcSql);
            }
        }
    }

    /**
     * Runs a SQL script file, batching {@code INSERT} statements to reduce round-trips.
     * Other statements (e.g. {@code COMMIT}) are executed immediately after flushing any pending inserts.
     *
     * @param insertBatchSize max inserts per {@link Statement#executeBatch()}; if {@code <= 0}, delegates to {@link #runFile}.
     */
    public static void runFileWithInsertBatch(Connection connection, Path file, int insertBatchSize) throws IOException, SQLException {
        runFileWithInsertBatch(connection, file, insertBatchSize, null);
    }

    public static void runFileWithInsertBatch(Connection connection, Path file, int insertBatchSize, BooleanSupplier cancelled)
            throws IOException, SQLException {
        if (connection == null) throw new IllegalArgumentException("connection is null");
        if (file == null) throw new IllegalArgumentException("file is null");
        if (insertBatchSize <= 0) {
            runFile(connection, file, cancelled);
            return;
        }
        String sql = Files.readString(file, StandardCharsets.UTF_8);
        runSqlWithInsertBatch(connection, sql, insertBatchSize, cancelled);
    }

    /**
     * Like {@link #runSql} but batches consecutive {@code INSERT} statements.
     */
    public static void runSqlWithInsertBatch(Connection connection, String script, int insertBatchSize) throws SQLException {
        runSqlWithInsertBatch(connection, script, insertBatchSize, null);
    }

    public static void runSqlWithInsertBatch(Connection connection, String script, int insertBatchSize, BooleanSupplier cancelled)
            throws SQLException {
        if (connection == null) throw new IllegalArgumentException("connection is null");
        if (script == null) throw new IllegalArgumentException("script is null");
        if (insertBatchSize <= 0) {
            runSql(connection, script, cancelled);
            return;
        }

        List<String> statements = splitStatements(script);
        try (Statement st = connection.createStatement()) {
            int pending = 0;
            for (String s : statements) {
                throwIfCancelled(cancelled);
                String trimmed = s.trim();
                if (trimmed.isEmpty()) continue;

                String effective = stripLeadingCommentsAndBlankLines(trimmed);
                if (effective.isEmpty()) continue;

                String u = effective.toUpperCase(Locale.ROOT);
                if (u.startsWith("SET ")
                        || u.startsWith("SPOOL ")
                        || u.equals("SPOOL")
                        || u.startsWith("PROMPT ")
                        || u.equals("PROMPT")
                        || u.equals("EXIT")
                        || u.equals("QUIT")) {
                    continue;
                }

                String jdbcSql = stripTrailingStatementDelimiter(effective);
                if (jdbcSql.isEmpty()) continue;

                String kind = jdbcSql.trim().toUpperCase(Locale.ROOT);
                if (kind.startsWith("INSERT")) {
                    st.addBatch(jdbcSql);
                    pending++;
                    if (pending >= insertBatchSize) {
                        st.executeBatch();
                        pending = 0;
                    }
                } else {
                    if (pending > 0) {
                        st.executeBatch();
                        pending = 0;
                    }
                    st.execute(jdbcSql);
                }
            }
            if (pending > 0) {
                st.executeBatch();
            }
        }
    }

    /**
     * JDBC expects raw SQL without SQL*Plus terminators.
     * Our splitter keeps ';' as part of statement, so strip it here.
     */
    private static String stripTrailingStatementDelimiter(String s) {
        String out = s.trim();
        while (out.endsWith(";")) {
            out = out.substring(0, out.length() - 1).trim();
        }
        return out;
    }

    private static String stripLeadingCommentsAndBlankLines(String s) {
        int i = 0;
        int n = s.length();
        while (i < n) {
            // skip whitespace
            while (i < n && Character.isWhitespace(s.charAt(i))) i++;
            if (i >= n) return "";

            // skip line comment
            if (i + 1 < n && s.charAt(i) == '-' && s.charAt(i + 1) == '-') {
                int nl = s.indexOf('\n', i + 2);
                if (nl < 0) return "";
                i = nl + 1;
                continue;
            }

            // skip block comment
            if (i + 1 < n && s.charAt(i) == '/' && s.charAt(i + 1) == '*') {
                int end = s.indexOf("*/", i + 2);
                if (end < 0) return "";
                i = end + 2;
                continue;
            }

            break;
        }
        return s.substring(i).trim();
    }

    /**
     * Splits SQL script into executable statements.
     * - Normal SQL ends with ';' (outside quotes/comments)
     * - PL/SQL blocks are delimited by a line containing only '/' (SQL*Plus style)
     */
    private static List<String> splitStatements(String script) {
        List<String> out = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;
        boolean plsqlMode = false;

        int len = script.length();
        for (int i = 0; i < len; i++) {
            char c = script.charAt(i);
            char next = (i + 1 < len) ? script.charAt(i + 1) : '\0';

            if (inLineComment) {
                current.append(c);
                if (c == '\n') inLineComment = false;
                continue;
            }
            if (inBlockComment) {
                current.append(c);
                if (c == '*' && next == '/') {
                    current.append(next);
                    i++;
                    inBlockComment = false;
                }
                continue;
            }

            if (!inSingleQuote && !inDoubleQuote) {
                if (c == '-' && next == '-') {
                    current.append(c).append(next);
                    i++;
                    inLineComment = true;
                    continue;
                }
                if (c == '/' && next == '*') {
                    current.append(c).append(next);
                    i++;
                    inBlockComment = true;
                    continue;
                }
            }

            if (!inDoubleQuote && c == '\'') {
                current.append(c);
                if (inSingleQuote) {
                    if (next == '\'') {
                        current.append(next);
                        i++;
                    } else {
                        inSingleQuote = false;
                    }
                } else {
                    inSingleQuote = true;
                }
                continue;
            }
            if (!inSingleQuote && c == '"') {
                inDoubleQuote = !inDoubleQuote;
                current.append(c);
                continue;
            }

            current.append(c);

            if (!inSingleQuote && !inDoubleQuote) {
                if (!plsqlMode && c == ';') {
                    out.add(current.toString());
                    current.setLength(0);
                    continue;
                }

                // Detect start of a PL/SQL create procedure/package/function block by keyword.
                // We'll switch to plsqlMode until we see a line containing only "/".
                if (!plsqlMode) {
                    String tail = tailUpper(current, 80);
                    if (tail.contains("CREATE OR REPLACE PROCEDURE")
                            || tail.contains("CREATE OR REPLACE FUNCTION")
                            || tail.contains("CREATE OR REPLACE PACKAGE")
                            || tail.contains("CREATE OR REPLACE TRIGGER")) {
                        plsqlMode = true;
                    }
                }

                if (plsqlMode && c == '\n') {
                    int lineStart = lastIndexOf(current, '\n', current.length() - 2) + 1;
                    String line = current.substring(lineStart).trim();
                    if (line.equals("/")) {
                        // Remove the trailing "/" line from the statement
                        current.setLength(lineStart);
                        out.add(current.toString());
                        current.setLength(0);
                        plsqlMode = false;
                    }
                }
            }
        }

        if (!current.toString().trim().isEmpty()) {
            out.add(current.toString());
        }
        return out;
    }

    private static int lastIndexOf(StringBuilder sb, char c, int fromIndex) {
        for (int i = Math.min(fromIndex, sb.length() - 1); i >= 0; i--) {
            if (sb.charAt(i) == c) return i;
        }
        return -1;
    }

    private static String tailUpper(StringBuilder sb, int maxChars) {
        int start = Math.max(0, sb.length() - maxChars);
        return sb.substring(start).toUpperCase(Locale.ROOT);
    }

    private static void throwIfCancelled(BooleanSupplier cancelled) {
        if (cancelled == null) {
            return;
        }
        if (cancelled.getAsBoolean()) {
            throw new CancellationException("Operation stopped");
        }
    }
}

