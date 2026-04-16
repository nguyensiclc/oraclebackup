package com.oracleexporter.gui;

import com.oracleexporter.model.ColumnInfo;
import com.oracleexporter.model.TableInfo;
import com.oracleexporter.service.ExportService;
import com.oracleexporter.service.MetadataService;
import com.oracleexporter.util.ConfigUtil;
import com.oracleexporter.util.DatabaseUtil;
import javafx.application.Platform;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.stage.DirectoryChooser;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.List;
import java.util.Properties;

public class MainController {

    private static final String CFG_HOST = "db.host";
    private static final String CFG_PORT = "db.port";
    private static final String CFG_SERVICE = "db.service";
    private static final String CFG_USER = "db.user";
    private static final String CFG_EXPORT_DIR = "export.baseDir";

    private static final DateTimeFormatter EXPORT_TS_FMT = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm");

    @FXML private TextField hostField;
    @FXML private TextField portField;
    @FXML private TextField serviceField;
    @FXML private TextField userField;
    @FXML private PasswordField passwordField;

    @FXML private Button connectButton;
    @FXML private Button disconnectButton;
    @FXML private TextField exportFolderField;
    @FXML private Button browseExportFolderButton;
    @FXML private TextField tableFilterField;
    @FXML private ListView<String> tablesList;

    @FXML private TableView<ColumnInfo> columnTable;
    @FXML private TableColumn<ColumnInfo, String> colName;
    @FXML private TableColumn<ColumnInfo, String> colType;
    @FXML private TableColumn<ColumnInfo, Number> colLen;
    @FXML private TableColumn<ColumnInfo, Number> colPrec;
    @FXML private TableColumn<ColumnInfo, Number> colScale;
    @FXML private TableColumn<ColumnInfo, Boolean> colNullable;
    @FXML private TableColumn<ColumnInfo, Boolean> colPk;
    @FXML private TableColumn<ColumnInfo, String> colComment;

    @FXML private CheckBox includeDmlCheck;
    @FXML private Button loadPreviewButton;
    @FXML private Button exportButton;
    @FXML private Button exportAllButton;

    @FXML private ProgressBar progressBar;
    @FXML private Label statusLabel;

    private final MetadataService metadataService = new MetadataService();
    private final ExportService exportService = new ExportService();

    private Connection connection;
    private TableInfo currentTable;
    private String selectedTableName;
    private String connectedSchema;
    private Path exportBaseDir;
    private boolean operationInProgress;
    private final ObservableList<String> allTables = FXCollections.observableArrayList();
    private FilteredList<String> filteredTables;
    private final Properties config = ConfigUtil.load();

    @FXML
    public void initialize() {
        loadConfigToFields();

        includeDmlCheck.setSelected(false);

        exportButton.setDisable(true);
        exportAllButton.setDisable(true);
        disconnectButton.setDisable(true);
        loadPreviewButton.setDisable(true);

        columnTable.setItems(FXCollections.observableArrayList());
        columnTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);

        colName.setCellValueFactory(c -> new SimpleStringProperty(nv(c.getValue().getName())));
        colType.setCellValueFactory(c -> new SimpleStringProperty(nv(c.getValue().getDataType())));
        colLen.setCellValueFactory(c -> new SimpleIntegerProperty(nni(c.getValue().getLength())));
        colPrec.setCellValueFactory(c -> new SimpleIntegerProperty(nni(c.getValue().getPrecision())));
        colScale.setCellValueFactory(c -> new SimpleIntegerProperty(nni(c.getValue().getScale())));
        colNullable.setCellValueFactory(c -> new SimpleBooleanProperty(c.getValue().isNullable()));
        colPk.setCellValueFactory(c -> new SimpleBooleanProperty(c.getValue().isPrimaryKey()));
        colComment.setCellValueFactory(c -> new SimpleStringProperty(nv(c.getValue().getComment())));

        colNullable.setCellFactory(tc -> new BoolBadgeCell("NULL", "NOT NULL"));
        colPk.setCellFactory(tc -> new BoolBadgeCell("PK", ""));

        filteredTables = new FilteredList<>(allTables, t -> true);
        tablesList.setItems(filteredTables);
        tablesList.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);

        hostField.textProperty().addListener((obs, o, n) -> saveFieldsToConfig());
        portField.textProperty().addListener((obs, o, n) -> saveFieldsToConfig());
        serviceField.textProperty().addListener((obs, o, n) -> saveFieldsToConfig());
        userField.textProperty().addListener((obs, o, n) -> saveFieldsToConfig());

        tableFilterField.textProperty().addListener((obs, oldV, newV) -> applyTableFilter(newV));
        tablesList.getSelectionModel().selectedItemProperty().addListener((obs, oldV, newV) -> {
            selectedTableName = (newV == null) ? null : newV.trim();
            currentTable = null;
            columnTable.getItems().clear();
            disableWhileBusy(false);
        });

        setStatus("Ready.");
        progressBar.setProgress(0);
    }

    @FXML
    private void onBrowseExportFolder() {
        if (operationInProgress) return;
        DirectoryChooser chooser = new DirectoryChooser();
        chooser.setTitle("Choose export folder");
        if (exportBaseDir != null && Files.isDirectory(exportBaseDir)) {
            chooser.setInitialDirectory(exportBaseDir.toFile());
        }
        var selected = chooser.showDialog(browseExportFolderButton.getScene().getWindow());
        if (selected == null) return;

        exportBaseDir = selected.toPath();
        exportFolderField.setText(exportBaseDir.toString());
        saveFieldsToConfig();
        disableWhileBusy(false);
    }

    @FXML
    private void onConnect() {
        if (operationInProgress) return;
        if (isConnected()) {
            showError("Already connected", "Please disconnect before connecting again.");
            return;
        }
        operationInProgress = true;
        disableWhileBusy(true);
        saveFieldsToConfig();

        Task<List<String>> task = new Task<>() {
            @Override
            protected List<String> call() throws Exception {
                updateMessage("Connecting...");
                updateProgress(-1, 1);

                String host = hostField.getText();
                int port = Integer.parseInt(portField.getText().trim());
                String service = serviceField.getText();
                String user = userField.getText();
                String pass = passwordField.getText();

                Connection conn = DatabaseUtil.connect(host, port, service, user, pass);
                List<String> tables = metadataService.listTables(conn);

                connection = conn;
                return tables;
            }
        };

        bindTask(task);

        task.setOnSucceeded(e -> {
            List<String> tables = task.getValue();
            allTables.setAll(tables);
            applyTableFilter(tableFilterField.getText());
            tablesList.getSelectionModel().clearSelection();
            selectedTableName = null;
            currentTable = null;
            connectedSchema = safeSchemaName(resolveCurrentSchema(connection));
            columnTable.getItems().clear();
            setStatus("Connected. Loaded " + tables.size() + " tables.");
            setConnectedUi(true);
            operationInProgress = false;
            disableWhileBusy(false);
        });

        task.setOnFailed(e -> {
            Throwable ex = task.getException();
            showError("Connect failed", ex == null ? "Unknown error" : ex.getMessage());
            setStatus("Connect failed.");
            setConnectedUi(false);
            operationInProgress = false;
            disableWhileBusy(false);
        });

        new Thread(task, "connect-task").start();
    }

    @FXML
    private void onDisconnect() {
        if (operationInProgress) return;
        disableWhileBusy(true);
        shutdown();
        allTables.clear();
        applyTableFilter(tableFilterField.getText());
        columnTable.getItems().clear();
        selectedTableName = null;
        connectedSchema = null;
        setStatus("Disconnected.");
        setConnectedUi(false);
        disableWhileBusy(false);
    }

    @FXML
    private void onLoadPreview() {
        if (operationInProgress) return;
        String t = getSelectedTableName();
        if (t == null) {
            showError("No table selected", "Please select a table first.");
            return;
        }
        loadTableMetadata(t);
    }

    private void loadTableMetadata(String tableName) {
        if (tableName == null || tableName.isBlank()) return;
        if (connection == null) {
            showError("Not connected", "Please connect first.");
            return;
        }

        operationInProgress = true;
        disableWhileBusy(true);

        String t = tableName.trim().toUpperCase(Locale.ROOT);
        Task<TableInfo> task = new Task<>() {
            @Override
            protected TableInfo call() throws Exception {
                updateMessage("Loading metadata for " + t + "...");
                updateProgress(-1, 1);
                return metadataService.loadTableInfo(connection, t);
            }
        };

        bindTask(task);

        task.setOnSucceeded(e -> {
            currentTable = task.getValue();
            columnTable.getItems().setAll(currentTable.getColumns());
            setStatus("Loaded table " + currentTable.getName() + " (" + currentTable.getColumns().size() + " columns).");
            exportButton.setDisable(false);
            operationInProgress = false;
            disableWhileBusy(false);
        });

        task.setOnFailed(e -> {
            Throwable ex = task.getException();
            showError("Load metadata failed", ex == null ? "Unknown error" : ex.getMessage());
            setStatus("Load metadata failed.");
            operationInProgress = false;
            disableWhileBusy(false);
        });

        new Thread(task, "load-table-task").start();
    }

    @FXML
    private void onExport() {
        if (operationInProgress) return;
        String tableName = getSelectedTableName();
        if (tableName == null) {
            showError("No table selected", "Please select a table first.");
            return;
        }
        if (!isConnected()) {
            showError("Not connected", "Please connect first.");
            return;
        }
        if (exportBaseDir == null || !Files.isDirectory(exportBaseDir)) {
            showError("Export folder not set", "Please choose an export folder first.");
            return;
        }
        if (connectedSchema == null || connectedSchema.isBlank()) {
            connectedSchema = safeSchemaName(resolveCurrentSchema(connection));
        }

        boolean includeDml = includeDmlCheck.isSelected();

        LocalDateTime now = LocalDateTime.now();
        String exportFolderName = connectedSchema + "_" + EXPORT_TS_FMT.format(now);
        Path exportRoot = exportBaseDir.resolve(exportFolderName);
        Path ddlDir = exportRoot.resolve("ddl");
        Path dataDir = exportRoot.resolve("data");

        operationInProgress = true;
        disableWhileBusy(true);

        Task<Path> task = new Task<>() {
            @Override
            protected Path call() throws Exception {
                updateMessage("Preparing export folders...");
                updateProgress(-1, 1);

                Files.createDirectories(ddlDir);
                Files.createDirectories(dataDir);

                TableInfo tableInfo = metadataService.loadTableInfo(connection, tableName);

                Path ddlTarget = ddlDir.resolve(tableName + ".sql");
                updateMessage("Exporting DDL to " + ddlTarget.getFileName() + "...");
                exportService.exportSql(tableInfo, ddlTarget);

                if (includeDml) {
                    Path dataTarget = dataDir.resolve(tableName + ".sql");
                    updateMessage("Exporting DATA to " + dataTarget.getFileName() + "...");
                    exportService.exportDataSql(connection, tableName, dataTarget);
                }

                return exportRoot;
            }
        };

        bindTask(task);

        task.setOnSucceeded(e -> {
            setStatus("Exported to folder: " + task.getValue());
            operationInProgress = false;
            disableWhileBusy(false);
        });
        task.setOnFailed(e -> {
            Throwable ex = task.getException();
            showError("Export failed", ex == null ? "Unknown error" : ex.getMessage());
            setStatus("Export failed.");
            operationInProgress = false;
            disableWhileBusy(false);
        });

        new Thread(task, "export-task").start();
    }

    @FXML
    private void onExportAll() {
        if (operationInProgress) return;
        if (!isConnected()) {
            showError("Not connected", "Please connect first.");
            return;
        }
        if (exportBaseDir == null || !Files.isDirectory(exportBaseDir)) {
            showError("Export folder not set", "Please choose an export folder first.");
            return;
        }
        if (allTables.isEmpty()) {
            showError("No tables", "No tables loaded.");
            return;
        }
        if (connectedSchema == null || connectedSchema.isBlank()) {
            connectedSchema = safeSchemaName(resolveCurrentSchema(connection));
        }

        boolean includeDml = includeDmlCheck.isSelected();

        LocalDateTime now = LocalDateTime.now();
        String exportFolderName = connectedSchema + "_" + EXPORT_TS_FMT.format(now);
        Path exportRoot = exportBaseDir.resolve(exportFolderName);
        Path ddlDir = exportRoot.resolve("ddl");
        Path dataDir = exportRoot.resolve("data");

        operationInProgress = true;
        disableWhileBusy(true);

        Task<Path> task = new Task<>() {
            @Override
            protected Path call() throws Exception {
                updateMessage("Preparing export folders...");
                updateProgress(0, allTables.size());

                Files.createDirectories(ddlDir);
                Files.createDirectories(dataDir);

                int total = allTables.size();
                for (int i = 0; i < total; i++) {
                    String tableName = allTables.get(i);
                    if (tableName == null || tableName.isBlank()) continue;

                    updateProgress(i, total);
                    updateMessage("Exporting " + tableName + " (" + (i + 1) + "/" + total + ")...");

                    TableInfo tableInfo = metadataService.loadTableInfo(connection, tableName);
                    exportService.exportSql(tableInfo, ddlDir.resolve(tableName + ".sql"));

                    if (includeDml) {
                        exportService.exportDataSql(connection, tableName, dataDir.resolve(tableName + ".sql"));
                    }
                }

                updateProgress(total, total);
                updateMessage("Done.");
                return exportRoot;
            }
        };

        bindTask(task);
        task.setOnSucceeded(e -> {
            setStatus("Exported to folder: " + task.getValue());
            operationInProgress = false;
            disableWhileBusy(false);
        });
        task.setOnFailed(e -> {
            Throwable ex = task.getException();
            showError("Export failed", ex == null ? "Unknown error" : ex.getMessage());
            setStatus("Export failed.");
            operationInProgress = false;
            disableWhileBusy(false);
        });

        new Thread(task, "export-all-task").start();
    }

    private void bindTask(Task<?> task) {
        statusLabel.textProperty().unbind();
        progressBar.progressProperty().unbind();
        statusLabel.textProperty().bind(task.messageProperty());
        progressBar.progressProperty().bind(task.progressProperty());
    }

    private void disableWhileBusy(boolean busy) {
        boolean connected = isConnected();
        boolean hasSelection = getSelectedTableName() != null;
        boolean hasExportDir = exportBaseDir != null && Files.isDirectory(exportBaseDir);
        connectButton.setDisable(busy || connected);
        disconnectButton.setDisable(busy || !connected);
        browseExportFolderButton.setDisable(busy);
        tablesList.setDisable(busy || !connected);
        tableFilterField.setDisable(busy || !connected);
        loadPreviewButton.setDisable(busy || !connected || !hasSelection);
        exportButton.setDisable(busy || !connected || !hasSelection || !hasExportDir);
        exportAllButton.setDisable(busy || !connected || allTables.isEmpty() || !hasExportDir);
    }

    private void setConnectedUi(boolean connected) {
        hostField.setDisable(connected);
        portField.setDisable(connected);
        serviceField.setDisable(connected);
        userField.setDisable(connected);
        passwordField.setDisable(connected);
        disableWhileBusy(false);
    }

    private boolean isConnected() {
        try {
            return connection != null && !connection.isClosed();
        } catch (Exception e) {
            return false;
        }
    }

    private void applyTableFilter(String filter) {
        String f = filter == null ? "" : filter.trim().toUpperCase(Locale.ROOT);
        filteredTables.setPredicate(t -> {
            if (t == null) return false;
            if (f.isEmpty()) return true;
            return t.toUpperCase(Locale.ROOT).contains(f);
        });
    }

    private void loadConfigToFields() {
        hostField.setText(config.getProperty(CFG_HOST, ""));
        portField.setText(config.getProperty(CFG_PORT, "1521"));
        serviceField.setText(config.getProperty(CFG_SERVICE, ""));
        userField.setText(config.getProperty(CFG_USER, ""));

        String exportDir = config.getProperty(CFG_EXPORT_DIR, "");
        if (!exportDir.isBlank()) {
            Path p = Paths.get(exportDir);
            if (Files.isDirectory(p)) {
                exportBaseDir = p;
                exportFolderField.setText(exportBaseDir.toString());
            }
        }
    }

    private void saveFieldsToConfig() {
        config.setProperty(CFG_HOST, nv(hostField.getText()).trim());
        config.setProperty(CFG_PORT, nv(portField.getText()).trim());
        config.setProperty(CFG_SERVICE, nv(serviceField.getText()).trim());
        config.setProperty(CFG_USER, nv(userField.getText()).trim());
        if (exportBaseDir != null) {
            config.setProperty(CFG_EXPORT_DIR, exportBaseDir.toString());
        }
        ConfigUtil.save(config);
    }

    private static String resolveCurrentSchema(Connection connection) {
        try {
            String schema = connection.getSchema();
            if (schema != null && !schema.isBlank()) return schema;
        } catch (Exception ignored) {}

        String sql = "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual";
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                String s = rs.getString(1);
                if (s != null && !s.isBlank()) return s;
            }
        } catch (Exception ignored) {}

        return "SCHEMA";
    }

    private static String safeSchemaName(String schema) {
        if (schema == null || schema.isBlank()) return "SCHEMA";
        return schema.trim().replaceAll("[^A-Za-z0-9_\\-]", "_").toUpperCase(Locale.ROOT);
    }

    private void setStatus(String msg) {
        Platform.runLater(() -> {
            statusLabel.textProperty().unbind();
            statusLabel.setText(msg);
            progressBar.progressProperty().unbind();
            progressBar.setProgress(0);
        });
    }

    private void showError(String title, String message) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle(title);
            alert.setHeaderText(title);
            alert.setContentText(message);
            alert.showAndWait();
        });
    }

    public void shutdown() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception ignored) {
            // best-effort cleanup on app exit
        } finally {
            connection = null;
            currentTable = null;
            selectedTableName = null;
            connectedSchema = null;
        }
    }

    private String getSelectedTableName() {
        if (selectedTableName == null || selectedTableName.isBlank()) return null;
        return selectedTableName.trim().toUpperCase(Locale.ROOT);
    }


    private static String nv(String v) {
        return v == null ? "" : v;
    }

    private static int nni(Integer v) {
        return v == null ? 0 : v;
    }

    private static class BoolBadgeCell extends TableCell<ColumnInfo, Boolean> {
        private final String trueText;
        private final String falseText;

        private BoolBadgeCell(String trueText, String falseText) {
            this.trueText = trueText;
            this.falseText = falseText;
        }

        @Override
        protected void updateItem(Boolean item, boolean empty) {
            super.updateItem(item, empty);
            if (empty || item == null) {
                setText(null);
                getStyleClass().removeAll("badge", "badge-green", "badge-gray");
                return;
            }
            boolean v = item;
            String text = v ? trueText : falseText;
            if (text == null || text.isBlank()) {
                setText("");
                getStyleClass().removeAll("badge", "badge-green", "badge-gray");
                return;
            }
            setText(text);
            getStyleClass().removeAll("badge-green", "badge-gray");
            getStyleClass().addAll("badge", v ? "badge-green" : "badge-gray");
        }
    }
}

