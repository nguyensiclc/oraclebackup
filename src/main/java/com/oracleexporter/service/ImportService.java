package com.oracleexporter.service;

import com.oracleexporter.model.DbObjectType;
import com.oracleexporter.util.SqlScriptRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.concurrent.CancellationException;

public class ImportService {

    /** Matches {@link ExportService} flush cadence; JDBC insert batch size. */
    private static final int DATA_INSERT_BATCH_SIZE = 500;

    private static final DbObjectType[] DDL_IMPORT_ORDER = {
            DbObjectType.DATABASE_LINK,
            DbObjectType.TYPE,
            DbObjectType.SEQUENCE,
            DbObjectType.TABLE,
            DbObjectType.TABLE_PARTITION,
            DbObjectType.INDEX,
            DbObjectType.INDEX_PARTITION,
            DbObjectType.LOB,
            DbObjectType.VIEW,
            DbObjectType.MATERIALIZED_VIEW,
            DbObjectType.FUNCTION,
            DbObjectType.PROCEDURE,
            DbObjectType.PACKAGE,
            DbObjectType.PACKAGE_BODY,
            DbObjectType.SYNONYM,
    };

    public static final class ImportOptions {
        private final boolean skipTableDdl;
        private final BooleanSupplier cancelled;
        /** {@code null} = import every type in {@link #DDL_IMPORT_ORDER} (legacy). */
        private final Set<DbObjectType> importTypes;

        public ImportOptions(boolean skipTableDdl, BooleanSupplier cancelled) {
            this(skipTableDdl, cancelled, null);
        }

        public ImportOptions(boolean skipTableDdl, BooleanSupplier cancelled, Set<DbObjectType> importTypes) {
            this.skipTableDdl = skipTableDdl;
            this.cancelled = cancelled;
            // null = import all types (legacy); empty or non-empty EnumSet = explicit selection
            this.importTypes = importTypes == null ? null : EnumSet.copyOf(importTypes);
        }

        public boolean isSkipTableDdl() {
            return skipTableDdl;
        }

        boolean shouldImport(DbObjectType type) {
            if (importTypes == null) {
                return true;
            }
            return importTypes.contains(type);
        }
    }

    public void importFromExportFolder(Connection connection, Path exportRoot, Consumer<String> logger)
            throws IOException, SQLException {
        importFromExportFolder(connection, exportRoot, new ImportOptions(false, null, null), logger);
    }

    public void importFromExportFolder(
            Connection connection, Path exportRoot, ImportOptions options, Consumer<String> logger)
            throws IOException, SQLException {
        if (connection == null) throw new IllegalArgumentException("connection is null");
        if (exportRoot == null) throw new IllegalArgumentException("exportRoot is null");
        if (options == null) throw new IllegalArgumentException("options is null");
        if (!Files.isDirectory(exportRoot)) {
            throw new IllegalArgumentException("exportRoot is not a directory: " + exportRoot);
        }
        Consumer<String> log = logger == null ? s -> {} : logger;
        BooleanSupplier cancelled = options.cancelled;

        for (DbObjectType t : DDL_IMPORT_ORDER) {
            if (isCancelled(cancelled)) {
                log.accept("[STOP] Import stopped before " + t.name() + " phase.");
                return;
            }
            if (!options.shouldImport(t)) {
                log.accept("[IMPORT] Skipping " + t.name() + " (not selected).");
                continue;
            }
            try {
                if (t == DbObjectType.TABLE) {
                    if (options.isSkipTableDdl()) {
                        log.accept("[DDL] Skipping TABLE DDL (data-only import)...");
                    } else {
                        log.accept("[DDL] Importing TABLE DDL (existing tables are skipped)...");
                        runAllSqlInDir(
                                connection,
                                exportRoot.resolve(typeFolder(t)).resolve("ddl"),
                                log,
                                false,
                                cancelled,
                                true,
                                false);
                    }

                    Path tableData = exportRoot.resolve(typeFolder(DbObjectType.TABLE)).resolve("data");
                    if (Files.isDirectory(tableData)) {
                        log.accept("[DATA] Importing TABLE data (non-empty tables are skipped)...");
                        runAllSqlInDir(
                                connection,
                                tableData,
                                log,
                                true,
                                cancelled,
                                false,
                                true);
                    }
                } else {
                    log.accept("[DDL] Importing " + t.name() + " DDL...");
                    runAllSqlInDir(
                            connection,
                            exportRoot.resolve(typeFolder(t)).resolve("ddl"),
                            log,
                            false,
                            cancelled,
                            false,
                            false);
                }
            } catch (IOException ex) {
                log.accept("[IMPORT][FAIL] " + t.name() + " :: " + safeIoError(ex));
            } catch (SQLException ex) {
                log.accept("[IMPORT][FAIL] " + t.name() + " :: " + safeSqlError(ex));
            } catch (CancellationException ex) {
                log.accept("[STOP] Import stopped.");
                return;
            }
        }
    }

    /**
     * @param skipTableDdlIfExists when {@code true} (table/ddl only), skip file if {@link #tableExists} (name from filename).
     * @param skipDataIfTableNotEmpty when {@code true} (table/data only), skip file if table has rows (avoid duplicate PK).
     */
    private void runAllSqlInDir(
            Connection connection,
            Path dir,
            Consumer<String> log,
            boolean isData,
            BooleanSupplier cancelled,
            boolean skipTableDdlIfExists,
            boolean skipDataIfTableNotEmpty)
            throws IOException, SQLException {
        if (!Files.isDirectory(dir)) return;
        if (isCancelled(cancelled)) {
            return;
        }

        try (Stream<Path> s = Files.list(dir)) {
            List<Path> files = s
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".sql"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString().toLowerCase(Locale.ROOT)))
                    .toList();
            for (Path f : files) {
                if (isCancelled(cancelled)) {
                    throw new CancellationException("Operation stopped");
                }
                String name = f.getFileName().toString();
                String objName = name.toUpperCase(Locale.ROOT).endsWith(".SQL")
                        ? name.substring(0, name.length() - 4)
                        : name;

                if (isData) {
                    if (skipDataIfTableNotEmpty) {
                        if (!tableExists(connection, objName)) {
                            log.accept("[DATA][SKIP] " + objName + " — table does not exist");
                            continue;
                        }
                        if (!isTableEmpty(connection, objName)) {
                            log.accept("[DATA][SKIP] " + objName + " — table is not empty (avoids duplicate primary key)");
                            continue;
                        }
                    }
                    Savepoint sp = connection.setSavepoint("sp_" + objName);
                    try {
                        log.accept("[DATA] " + objName + " ...");
                        SqlScriptRunner.runFileWithInsertBatch(
                                connection, f, DATA_INSERT_BATCH_SIZE, cancelled);
                        log.accept("[DATA][OK] " + objName);
                    } catch (CancellationException e) {
                        try {
                            connection.rollback(sp);
                        } catch (Exception ignored) {
                        }
                        throw e;
                    } catch (SQLException ex) {
                        try {
                            connection.rollback(sp);
                        } catch (Exception ignored) {
                        }
                        log.accept("[DATA][FAIL] " + objName + " :: " + safeSqlError(ex));
                    } catch (IOException ex) {
                        try {
                            connection.rollback(sp);
                        } catch (Exception ignored) {
                        }
                        log.accept("[DATA][FAIL] " + objName + " :: " + safeIoError(ex));
                    } finally {
                        try {
                            connection.releaseSavepoint(sp);
                        } catch (Exception ignored) {
                        }
                    }
                } else {
                    try {
                        if (skipTableDdlIfExists && tableExists(connection, objName)) {
                            log.accept("[DDL][SKIP] " + objName + " — table already exists");
                            continue;
                        }
                        log.accept("[DDL] " + objName + " ...");
                        SqlScriptRunner.runFile(connection, f, cancelled);
                        log.accept("[DDL][OK] " + objName);
                    } catch (CancellationException e) {
                        throw e;
                    } catch (SQLException ex) {
                        log.accept("[DDL][FAIL] " + objName + " :: " + safeSqlError(ex));
                    } catch (IOException ex) {
                        log.accept("[DDL][FAIL] " + objName + " :: " + safeIoError(ex));
                    }
                }
            }
        }
    }

    private static boolean isCancelled(BooleanSupplier cancelled) {
        return cancelled != null && cancelled.getAsBoolean();
    }

    private static boolean tableExists(Connection connection, String objectNameFromFile) throws SQLException {
        if (objectNameFromFile == null || objectNameFromFile.isBlank()) {
            return false;
        }
        String tableName = objectNameFromFile.trim();
        String sql = "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    private static boolean isTableEmpty(Connection connection, String objectNameFromFile) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + quoteOracleIdentifier(objectNameFromFile.trim());
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return rs.next() && rs.getLong(1) == 0;
        }
    }

    /** Same rules as export DDL quoting: double-quote, escape interior quotes. */
    private static String quoteOracleIdentifier(String ident) {
        if (ident == null || ident.isBlank()) {
            return "\"\"";
        }
        return "\"" + ident.replace("\"", "\"\"") + "\"";
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

    private static String safeIoError(IOException ex) {
        return Objects.toString(ex.getMessage(), ex.getClass().getSimpleName());
    }

    public static String typeFolder(DbObjectType type) {
        return switch (type) {
            case TABLE -> "table";
            case VIEW -> "view";
            case MATERIALIZED_VIEW -> "materialized_view";
            case SYNONYM -> "synonym";
            case PROCEDURE -> "procedure";
            case FUNCTION -> "function";
            case INDEX -> "index";
            case INDEX_PARTITION -> "index_partition";
            case LOB -> "lob";
            case PACKAGE -> "package";
            case PACKAGE_BODY -> "package_body";
            case SEQUENCE -> "sequence";
            case TABLE_PARTITION -> "table_partition";
            case TYPE -> "type";
            case DATABASE_LINK -> "database_link";
        };
    }
}
