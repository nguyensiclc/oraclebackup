package com.oracleexporter.gui;

import com.oracleexporter.model.ColumnInfo;
import com.oracleexporter.model.DbObject;
import com.oracleexporter.model.DbObjectType;
import com.oracleexporter.model.TableInfo;
import com.oracleexporter.service.ExportService;
import com.oracleexporter.service.ImportService;
import com.oracleexporter.service.MetadataService;
import com.oracleexporter.util.ConfigUtil;
import com.oracleexporter.util.DatabaseUtil;
import com.oracleexporter.util.SavedCredentials;
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
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

public class MainController {

    private static final String CFG_HOST = "db.host";
    private static final String CFG_PORT = "db.port";
    private static final String CFG_SERVICE = "db.service";
    private static final String CFG_USER = "db.user";
    private static final String CFG_SAVE_PASSWORD = "db.password.save";
    private static final String CFG_EXPORT_DIR = "export.baseDir";
    private static final String CFG_IMPORT_DIR = "import.baseDir";
    private static final String CFG_DATA_MAX_ROWS = "export.data.maxRows";
    private static final String CFG_DATA_ALL_ROWS = "export.data.allRows";

    private static final DateTimeFormatter EXPORT_TS_FMT = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm");

    @FXML private TextField hostField;
    @FXML private TextField portField;
    @FXML private TextField serviceField;
    @FXML private ComboBox<String> userCombo;
    @FXML private PasswordField passwordField;
    @FXML private CheckBox savePasswordCheck;

    @FXML private Button connectButton;
    @FXML private Button disconnectButton;
    @FXML private TextField exportFolderField;
    @FXML private Button browseExportFolderButton;
    @FXML private TextField importFolderField;
    @FXML private Button browseImportFolderButton;
    @FXML private ComboBox<String> objectTypeCombo;
    @FXML private ListView<DbObject> objectList;

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
    @FXML private Spinner<Integer> maxDataRowsSpinner;
    @FXML private CheckBox exportAllDataCheck;
    @FXML private Button loadPreviewButton;
    @FXML private Button exportButton;
    @FXML private Button exportAllButton;
    @FXML private Button importButton;

    @FXML private ProgressBar progressBar;
    @FXML private Label statusLabel;
    @FXML private TextArea logArea;
    @FXML private Button clearLogButton;

    private final MetadataService metadataService = new MetadataService();
    private final ExportService exportService = new ExportService();
    private final ImportService importService = new ImportService();

    private Connection connection;
    private TableInfo currentTable;
    private DbObject selectedObject;
    private String connectedSchema;
    private Path exportBaseDir;
    private Path importDir;
    private boolean operationInProgress;
    private final ObservableList<DbObject> allObjects = FXCollections.observableArrayList();
    private FilteredList<DbObject> filteredObjects;

    private boolean updatingSelection;
    private boolean suppressObjectTypeComboListener;
    private boolean suppressCredentialSync;
    /** Saved (user → password) pairs; order matches config file index order. */
    private final LinkedHashMap<String, String> savedCredentials = new LinkedHashMap<>();
    private final Properties config = new Properties();

    @FXML
    public void initialize() {
        config.putAll(ConfigUtil.loadOrCreate(defaultConfig()));

        maxDataRowsSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(1, Integer.MAX_VALUE, 100));
        maxDataRowsSpinner.focusedProperty().addListener((obs, o, focused) -> {
            if (!focused) commitIntegerSpinner(maxDataRowsSpinner);
        });

        loadConfigToFields();

        includeDmlCheck.setSelected(false);

        includeDmlCheck.selectedProperty().addListener((obs, o, n) -> {
            refreshDataExportControlsState();
            saveFieldsToConfig();
        });
        exportAllDataCheck.selectedProperty().addListener((obs, o, n) -> {
            refreshDataExportControlsState();
            saveFieldsToConfig();
        });
        maxDataRowsSpinner.valueProperty().addListener((obs, o, n) -> saveFieldsToConfig());

        exportButton.setDisable(true);
        exportAllButton.setDisable(true);
        disconnectButton.setDisable(true);
        loadPreviewButton.setDisable(true);
        if (importButton != null) importButton.setDisable(true);

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

        filteredObjects = new FilteredList<>(allObjects, o -> true);
        buildObjectTypeCombo();
        refreshObjectTypeCombo();

        setupObjectList(objectList, filteredObjects);
        if (objectList != null) {
            objectList.setPlaceholder(new Label("Connect to database to view objects."));
        }
        if (columnTable != null) {
            columnTable.setPlaceholder(new Label("Connect to database to view table structure."));
        }

        hostField.textProperty().addListener((obs, o, n) -> saveFieldsToConfig());
        portField.textProperty().addListener((obs, o, n) -> saveFieldsToConfig());
        serviceField.textProperty().addListener((obs, o, n) -> saveFieldsToConfig());
        wireCredentialUi();
        passwordField.textProperty().addListener((obs, o, n) -> {
            if (suppressCredentialSync) {
                return;
            }
            saveFieldsToConfig();
        });
        if (savePasswordCheck != null) {
            savePasswordCheck.selectedProperty().addListener((obs, o, n) -> saveFieldsToConfig());
        }

        wireObjectSelection();

        setStatus("Ready.");
        progressBar.setProgress(0);

        refreshDataExportControlsState();
        decorateToolbarButtons();
    }

    private void decorateToolbarButtons() {
        if (connectButton != null) {
            connectButton.setGraphic(textGraphic("🔗"));
        }
        if (disconnectButton != null) {
            disconnectButton.setGraphic(textGraphic("⏻"));
        }
        if (browseExportFolderButton != null) {
            browseExportFolderButton.setGraphic(textGraphic("📁"));
        }
        if (browseImportFolderButton != null) {
            browseImportFolderButton.setGraphic(textGraphic("📁"));
        }
        if (loadPreviewButton != null) {
            loadPreviewButton.setGraphic(textGraphic("👁"));
        }
        if (exportButton != null) {
            exportButton.setGraphic(textGraphic("⬇"));
        }
        if (exportAllButton != null) {
            exportAllButton.setGraphic(textGraphic("⬇"));
        }
        if (importButton != null) {
            importButton.setGraphic(textGraphic("⬆"));
        }
    }

    private static Label textGraphic(String symbol) {
        Label g = new Label(symbol);
        g.getStyleClass().add("btn-emoji-icon");
        return g;
    }

    private void buildObjectTypeCombo() {
        if (objectTypeCombo == null) {
            return;
        }
        objectTypeCombo.setEditable(false);
        objectTypeCombo.getItems().clear();
        objectTypeCombo.getItems().add("All (0)");
        for (DbObjectType t : DbObjectType.values()) {
            objectTypeCombo.getItems().add(t.name() + " (0)");
        }
        objectTypeCombo.getSelectionModel().selectFirst();
        objectTypeCombo.valueProperty().addListener((obs, o, n) -> {
            if (suppressObjectTypeComboListener) {
                return;
            }
            applyObjectFilter();
            if (objectList != null) {
                objectList.refresh();
            }
        });
    }

    /** Refreshes dropdown labels with counts from {@link #allObjects}, keeping the selected type. */
    private void refreshObjectTypeCombo() {
        if (objectTypeCombo == null) {
            return;
        }
        String prevKey = getSelectedTypeFilterKey();
        int total = allObjects.size();
        int[] counts = new int[DbObjectType.values().length];
        for (DbObject o : allObjects) {
            if (o != null) {
                counts[o.getType().ordinal()]++;
            }
        }
        suppressObjectTypeComboListener = true;
        try {
            ObservableList<String> items = FXCollections.observableArrayList();
            items.add("All (" + total + ")");
            for (DbObjectType t : DbObjectType.values()) {
                items.add(t.name() + " (" + counts[t.ordinal()] + ")");
            }
            objectTypeCombo.getItems().setAll(items);
            selectObjectTypeComboByKey(prevKey);
        } finally {
            suppressObjectTypeComboListener = false;
        }
    }

    private void selectObjectTypeComboByKey(String typeKey) {
        if (objectTypeCombo == null) {
            return;
        }
        String want = typeKey == null ? "All" : typeKey;
        for (String item : objectTypeCombo.getItems()) {
            if ("All".equals(want) && item.startsWith("All (")) {
                objectTypeCombo.getSelectionModel().select(item);
                return;
            }
            if (!"All".equals(want) && item.startsWith(want + " (")) {
                objectTypeCombo.getSelectionModel().select(item);
                return;
            }
        }
        objectTypeCombo.getSelectionModel().selectFirst();
    }

    private String getSelectedTypeFilterKey() {
        if (objectTypeCombo == null) {
            return "All";
        }
        String v = objectTypeCombo.getValue();
        if (v == null || v.startsWith("All (")) {
            return "All";
        }
        int p = v.lastIndexOf(" (");
        if (p <= 0) {
            return "All";
        }
        return v.substring(0, p);
    }

    private void setupObjectList(ListView<DbObject> listView, FilteredList<DbObject> items) {
        if (listView == null) return;
        listView.setItems(items);
        listView.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);
        listView.setCellFactory(lv -> new ListCell<>() {
            @Override
            protected void updateItem(DbObject item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                    setGraphic(null);
                    return;
                }
                setGraphic(null);
                setText(formatObjectListRow(item));
            }
        });
    }

    private String formatObjectListRow(DbObject item) {
        boolean allTypes = "All".equals(getSelectedTypeFilterKey());
        if (allTypes) {
            return item.getType() + "  " + item.getDisplayLabel();
        }
        return item.getDisplayLabel();
    }

    private void wireObjectSelection() {
        if (objectList == null) return;
        objectList.getSelectionModel().selectedItemProperty().addListener((obs, oldV, newV) -> {
            if (updatingSelection) return;
            selectedObject = newV;
            if (newV == null) return;
            currentTable = null;
            columnTable.getItems().clear();
            disableWhileBusy(false);
        });
    }

    private static Properties defaultConfig() {
        Properties p = new Properties();
        p.setProperty(CFG_HOST, "");
        p.setProperty(CFG_PORT, "1521");
        p.setProperty(CFG_SERVICE, "");
        p.setProperty(CFG_USER, "");
        p.setProperty(CFG_SAVE_PASSWORD, "false");
        p.setProperty(CFG_EXPORT_DIR, "");
        p.setProperty(CFG_IMPORT_DIR, "");
        p.setProperty(CFG_DATA_MAX_ROWS, "100");
        p.setProperty(CFG_DATA_ALL_ROWS, "false");
        return p;
    }

    private void refreshDataExportControlsState() {
        boolean includeData = includeDmlCheck.isSelected();
        boolean allRows = exportAllDataCheck.isSelected();
        maxDataRowsSpinner.setDisable(!includeData || allRows);
        exportAllDataCheck.setDisable(!includeData);
    }

    /**
     * @return {@code null} = no row limit (all rows); otherwise max rows per table for INSERT export.
     */
    private Integer resolveDataExportMaxRows() {
        if (exportAllDataCheck.isSelected()) {
            return null;
        }
        int n = maxDataRowsSpinner.getValue();
        return n > 0 ? Integer.valueOf(n) : null;
    }

    private static void commitIntegerSpinner(Spinner<Integer> spinner) {
        if (!spinner.isEditable()) return;
        String text = spinner.getEditor().getText();
        try {
            int v = Integer.parseInt(text.trim());
            SpinnerValueFactory.IntegerSpinnerValueFactory fac =
                    (SpinnerValueFactory.IntegerSpinnerValueFactory) spinner.getValueFactory();
            int min = fac.getMin();
            int max = fac.getMax();
            v = Math.max(min, Math.min(max, v));
            fac.setValue(v);
        } catch (NumberFormatException ignored) {
            spinner.getValueFactory().setValue(spinner.getValueFactory().getValue());
        }
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
    private void onBrowseImportFolder() {
        if (operationInProgress) return;
        DirectoryChooser chooser = new DirectoryChooser();
        chooser.setTitle("Choose import folder (export root)");
        if (importDir != null && Files.isDirectory(importDir)) {
            chooser.setInitialDirectory(importDir.toFile());
        }
        var selected = chooser.showDialog(browseImportFolderButton.getScene().getWindow());
        if (selected == null) return;

        importDir = selected.toPath();
        importFolderField.setText(importDir.toString());
        saveFieldsToConfig();
        disableWhileBusy(false);
    }

    @FXML
    private void onClearLog() {
        if (logArea != null) {
            logArea.clear();
        }
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

        Task<List<DbObject>> task = new Task<>() {
            @Override
            protected List<DbObject> call() throws Exception {
                updateMessage("Connecting...");
                updateProgress(-1, 1);

                String host = hostField.getText();
                int port = Integer.parseInt(portField.getText().trim());
                String service = serviceField.getText();
                String user = getUsername();
                String pass = passwordField.getText();

                Connection conn = DatabaseUtil.connect(host, port, service, user, pass);
                List<DbObject> objects = metadataService.listObjects(conn);

                connection = conn;
                return objects;
            }
        };

        bindTask(task);

        task.setOnSucceeded(e -> {
            List<DbObject> objects = task.getValue();
            allObjects.setAll(objects);
            refreshObjectTypeCombo();
            applyObjectFilter();
            clearAllSelections();
            selectedObject = null;
            currentTable = null;
            connectedSchema = safeSchemaName(resolveCurrentSchema(connection));
            columnTable.getItems().clear();
            setConnectedUi(true);
            operationInProgress = false;
            disableWhileBusy(false);
            persistSavedCredentialsAfterSuccessfulConnect();
            saveFieldsToConfig();
            setStatus("Connected. Loaded " + objects.size() + " objects.");
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
        allObjects.clear();
        refreshObjectTypeCombo();
        applyObjectFilter();
        columnTable.getItems().clear();
        selectedObject = null;
        connectedSchema = null;
        setStatus("Disconnected.");
        setConnectedUi(false);
        disableWhileBusy(false);
    }

    @FXML
    private void onLoadPreview() {
        if (operationInProgress) return;
        DbObject obj = getSelectedObject();
        if (obj == null) {
            showError("No table selected", "Please select a table first.");
            return;
        }
        if (obj.getType() != DbObjectType.TABLE) {
            showError("Preview not available", "Preview is available for TABLE only. You can still export DDL for this object.");
            return;
        }
        loadTableMetadata(obj.getName());
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
        DbObject obj = getSelectedObject();
        if (obj == null) {
            showError("No object selected", "Please select an object first.");
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
        final Integer dataMaxRows = (includeDml && obj.getType() == DbObjectType.TABLE) ? resolveDataExportMaxRows() : null;

        LocalDateTime now = LocalDateTime.now();
        String exportFolderName = connectedSchema + "_" + EXPORT_TS_FMT.format(now);
        Path exportRoot = exportBaseDir.resolve(exportFolderName);
        Path typeRoot = exportRoot.resolve(ImportService.typeFolder(obj.getType()));
        Path ddlDir = typeRoot.resolve("ddl");
        Path dataDir = typeRoot.resolve("data");

        operationInProgress = true;
        disableWhileBusy(true);

        Task<Path> task = new Task<>() {
            @Override
            protected Path call() throws Exception {
                updateMessage("Preparing export folders...");
                updateProgress(-1, 1);

                Files.createDirectories(ddlDir);
                Files.createDirectories(dataDir);

                if (obj.getType() == DbObjectType.TABLE) {
                    TableInfo tableInfo = metadataService.loadTableInfo(connection, obj.getName());
                    Path ddlTarget = ddlDir.resolve(obj.fileBaseName() + ".sql");
                    updateMessage("Exporting DDL to " + ddlTarget.getFileName() + "...");
                    exportService.exportSql(tableInfo, ddlTarget);

                    if (includeDml) {
                        Path dataTarget = dataDir.resolve(obj.fileBaseName() + ".sql");
                        updateMessage("Exporting DATA to " + dataTarget.getFileName() + "...");
                        exportService.exportDataSql(connection, obj.getName(), dataTarget, dataMaxRows);
                    }
                } else {
                    String ddl = metadataService.loadObjectDdl(connection, connectedSchema, obj);
                    if (ddl == null || ddl.isBlank()) {
                        throw new IllegalStateException("Could not load DDL for " + obj.getType() + " " + obj.getName());
                    }
                    Path ddlTarget = ddlDir.resolve(obj.fileBaseName() + ".sql");
                    updateMessage("Exporting DDL to " + ddlTarget.getFileName() + "...");
                    exportService.exportRawSql(ddl, ddlTarget);
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
        List<DbObject> objectsToExport = resolveObjectsForExportAll();
        if (objectsToExport.isEmpty()) {
            showError("No objects", "No objects loaded.");
            return;
        }
        if (connectedSchema == null || connectedSchema.isBlank()) {
            connectedSchema = safeSchemaName(resolveCurrentSchema(connection));
        }

        boolean includeDml = includeDmlCheck.isSelected();
        final Integer dataMaxRows = includeDml ? resolveDataExportMaxRows() : null;

        LocalDateTime now = LocalDateTime.now();
        String exportFolderName = connectedSchema + "_" + EXPORT_TS_FMT.format(now);
        Path exportRoot = exportBaseDir.resolve(exportFolderName);

        operationInProgress = true;
        disableWhileBusy(true);

        Task<Path> task = new Task<>() {
            @Override
            protected Path call() throws Exception {
                updateMessage("Preparing export folders...");
                updateProgress(0, objectsToExport.size());

                int total = objectsToExport.size();
                for (int i = 0; i < total; i++) {
                    DbObject obj = objectsToExport.get(i);
                    if (obj == null) continue;

                    updateProgress(i, total);
                    updateMessage("Exporting " + obj.getType() + " " + obj.getDisplayLabel() + " (" + (i + 1) + "/" + total + ")...");

                    Path typeRoot = exportRoot.resolve(ImportService.typeFolder(obj.getType()));
                    Path ddlDir = typeRoot.resolve("ddl");
                    Path dataDir = typeRoot.resolve("data");
                    Files.createDirectories(ddlDir);
                    Files.createDirectories(dataDir);

                    if (obj.getType() == DbObjectType.TABLE) {
                        TableInfo tableInfo = metadataService.loadTableInfo(connection, obj.getName());
                        exportService.exportSql(tableInfo, ddlDir.resolve(obj.fileBaseName() + ".sql"));

                        if (includeDml) {
                            exportService.exportDataSql(connection, obj.getName(), dataDir.resolve(obj.fileBaseName() + ".sql"), dataMaxRows);
                        }
                    } else {
                        String ddl = metadataService.loadObjectDdl(connection, connectedSchema, obj);
                        if (ddl == null || ddl.isBlank()) {
                            throw new IllegalStateException("Could not load DDL for " + obj.getType() + " " + obj.getDisplayLabel());
                        }
                        Path ddlTarget = ddlDir.resolve(obj.fileBaseName() + ".sql");
                        exportService.exportRawSql(ddl, ddlTarget);
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

    @FXML
    private void onImport() {
        if (operationInProgress) return;
        if (!isConnected()) {
            showError("Not connected", "Please connect first.");
            return;
        }
        if (importDir == null || !Files.isDirectory(importDir)) {
            showError("Import folder not set", "Please choose an import folder first.");
            return;
        }

        operationInProgress = true;
        disableWhileBusy(true);
        clearLog();
        appendLog("[IMPORT] Root folder: " + importDir);

        Task<Path> task = new Task<>() {
            @Override
            protected Path call() throws Exception {
                updateMessage("Checking schema is empty...");
                updateProgress(-1, 1);
                metadataService.requireSchemaEmptyForImport(connection);

                updateMessage("Importing from folder...");
                boolean prevAutoCommit = true;
                try {
                    prevAutoCommit = connection.getAutoCommit();
                } catch (SQLException ignored) {}

                try {
                    try {
                        connection.setAutoCommit(false);
                    } catch (SQLException ignored) {}
                    importService.importFromExportFolder(connection, importDir, MainController.this::appendLog);
                    try {
                        connection.commit();
                    } catch (SQLException ignored) {}
                } finally {
                    try {
                        connection.setAutoCommit(prevAutoCommit);
                    } catch (SQLException ignored) {}
                }

                return importDir;
            }
        };

        bindTask(task);
        task.setOnSucceeded(e -> {
            setStatus("Imported from folder: " + task.getValue());
            operationInProgress = false;
            disableWhileBusy(false);
        });
        task.setOnFailed(e -> {
            Throwable ex = task.getException();
            showError("Import failed", ex == null ? "Unknown error" : ex.getMessage());
            setStatus("Import failed.");
            operationInProgress = false;
            disableWhileBusy(false);
        });

        new Thread(task, "import-task").start();
    }

    /** Objects to export when using Export ALL, respecting type filter and excluding empty list. */
    private List<DbObject> resolveObjectsForExportAll() {
        String typeKey = getSelectedTypeFilterKey();
        if ("All".equals(typeKey)) {
            return List.copyOf(allObjects);
        }
        try {
            DbObjectType t = DbObjectType.valueOf(typeKey);
            return allObjects.stream().filter(o -> o.getType() == t).toList();
        } catch (IllegalArgumentException e) {
            return List.copyOf(allObjects);
        }
    }

    private void bindTask(Task<?> task) {
        statusLabel.textProperty().unbind();
        progressBar.progressProperty().unbind();
        statusLabel.textProperty().bind(task.messageProperty());
        progressBar.progressProperty().bind(task.progressProperty());
    }

    private void disableWhileBusy(boolean busy) {
        boolean connected = isConnected();
        DbObject obj = getSelectedObject();
        boolean hasSelection = obj != null;
        boolean hasTableSelection = obj != null && obj.getType() == DbObjectType.TABLE;
        boolean hasExportDir = exportBaseDir != null && Files.isDirectory(exportBaseDir);
        boolean hasImportDir = importDir != null && Files.isDirectory(importDir);
        connectButton.setDisable(busy || connected);
        disconnectButton.setDisable(busy || !connected);
        browseExportFolderButton.setDisable(busy);
        if (browseImportFolderButton != null) browseImportFolderButton.setDisable(busy);
        if (objectList != null) objectList.setDisable(busy || !connected);
        if (objectTypeCombo != null) objectTypeCombo.setDisable(busy || !connected);
        loadPreviewButton.setDisable(busy || !connected || !hasTableSelection);
        exportButton.setDisable(busy || !connected || !hasSelection || !hasExportDir);
        exportAllButton.setDisable(busy || !connected || allObjects.isEmpty() || !hasExportDir);
        if (importButton != null) importButton.setDisable(busy || !connected || !hasImportDir);
        refreshDataExportControlsState();
    }

    private void setConnectedUi(boolean connected) {
        hostField.setDisable(connected);
        portField.setDisable(connected);
        serviceField.setDisable(connected);
        if (userCombo != null) {
            userCombo.setDisable(connected);
        }
        passwordField.setDisable(connected);
        if (savePasswordCheck != null) {
            savePasswordCheck.setDisable(connected);
        }
        disableWhileBusy(false);
    }

    private boolean isConnected() {
        try {
            return connection != null && !connection.isClosed();
        } catch (Exception e) {
            return false;
        }
    }

    private void applyObjectFilter() {
        String typeKey = getSelectedTypeFilterKey();
        filteredObjects.setPredicate(o -> {
            if (o == null) {
                return false;
            }
            if (!"All".equals(typeKey) && !o.getType().name().equals(typeKey)) {
                return false;
            }
            return true;
        });
    }

    private void clearAllSelections() {
        updatingSelection = true;
        try {
            if (objectList != null) objectList.getSelectionModel().clearSelection();
        } finally {
            updatingSelection = false;
        }
    }

    private void loadConfigToFields() {
        hostField.setText(config.getProperty(CFG_HOST, ""));
        portField.setText(config.getProperty(CFG_PORT, "1521"));
        serviceField.setText(config.getProperty(CFG_SERVICE, ""));

        SavedCredentials.readInto(savedCredentials, config);
        refreshUserComboItems();
        String lastUser = config.getProperty(CFG_USER, "");
        suppressCredentialSync = true;
        try {
            if (userCombo != null) {
                userCombo.getEditor().setText(lastUser);
            }
            String key = lastUser.trim();
            if (savedCredentials.containsKey(key)) {
                passwordField.setText(savedCredentials.get(key));
            } else {
                passwordField.clear();
            }
            boolean savePwd = Boolean.parseBoolean(config.getProperty(CFG_SAVE_PASSWORD, "false"));
            if (savePasswordCheck != null) {
                savePasswordCheck.setSelected(savePwd);
            }
        } finally {
            suppressCredentialSync = false;
        }

        String exportDir = config.getProperty(CFG_EXPORT_DIR, "");
        if (!exportDir.isBlank()) {
            Path p = Paths.get(exportDir);
            if (Files.isDirectory(p)) {
                exportBaseDir = p;
                exportFolderField.setText(exportBaseDir.toString());
            }
        }

        String importBase = config.getProperty(CFG_IMPORT_DIR, "");
        if (!importBase.isBlank()) {
            Path p = Paths.get(importBase);
            if (Files.isDirectory(p)) {
                importDir = p;
                importFolderField.setText(importDir.toString());
            }
        }

        exportAllDataCheck.setSelected(Boolean.parseBoolean(config.getProperty(CFG_DATA_ALL_ROWS, "false")));
        int maxRows = parsePositiveInt(config.getProperty(CFG_DATA_MAX_ROWS, "100"), 100);
        maxDataRowsSpinner.getValueFactory().setValue(maxRows);
    }

    /**
     * Persists non-credential settings. User/password pairs are written only after a successful connect
     * (see {@link #persistSavedCredentialsAfterSuccessfulConnect()}).
     */
    private void saveFieldsToConfig() {
        config.setProperty(CFG_HOST, nv(hostField.getText()).trim());
        config.setProperty(CFG_PORT, nv(portField.getText()).trim());
        config.setProperty(CFG_SERVICE, nv(serviceField.getText()).trim());
        String user = getUsername();
        config.setProperty(CFG_USER, user);
        boolean savePwd = savePasswordCheck != null && savePasswordCheck.isSelected();
        config.setProperty(CFG_SAVE_PASSWORD, Boolean.toString(savePwd));

        if (exportBaseDir != null) {
            config.setProperty(CFG_EXPORT_DIR, exportBaseDir.toString());
        }
        if (importDir != null) {
            config.setProperty(CFG_IMPORT_DIR, importDir.toString());
        }
        config.setProperty(CFG_DATA_ALL_ROWS, Boolean.toString(exportAllDataCheck.isSelected()));
        config.setProperty(CFG_DATA_MAX_ROWS, Integer.toString(maxDataRowsSpinner.getValue()));
        ConfigUtil.save(config);
    }

    /** Call only after {@link DatabaseUtil#connect} and metadata load succeed. */
    private void persistSavedCredentialsAfterSuccessfulConnect() {
        if (savePasswordCheck == null || !savePasswordCheck.isSelected()) {
            return;
        }
        String user = getUsername();
        if (user.isEmpty()) {
            return;
        }
        savedCredentials.put(user, passwordField.getText() == null ? "" : passwordField.getText());
        SavedCredentials.writeMap(config, savedCredentials);
        config.remove("db.password");
        refreshUserComboItems();
    }

    private String getUsername() {
        if (userCombo == null) {
            return "";
        }
        return nv(userCombo.getEditor().getText()).trim();
    }

    private void refreshUserComboItems() {
        if (userCombo == null) {
            return;
        }
        userCombo.getItems().setAll(new ArrayList<>(savedCredentials.keySet()));
    }

    private void wireCredentialUi() {
        if (userCombo == null) {
            return;
        }
        userCombo.setEditable(true);
        userCombo.valueProperty().addListener((obs, o, n) -> {
            if (suppressCredentialSync || n == null || n.isBlank()) {
                return;
            }
            String u = n.trim();
            if (savedCredentials.containsKey(u)) {
                applyPasswordForSavedUser(u);
            }
            saveFieldsToConfig();
        });
        userCombo.getEditor().textProperty().addListener((obs, o, n) -> {
            if (suppressCredentialSync) {
                return;
            }
            String u = n == null ? "" : n.trim();
            if (!u.isEmpty() && savedCredentials.containsKey(u)) {
                applyPasswordForSavedUser(u);
            }
        });
        userCombo.getEditor().focusedProperty().addListener((obs, was, focused) -> {
            if (!focused && !suppressCredentialSync) {
                saveFieldsToConfig();
            }
        });
    }

    private void applyPasswordForSavedUser(String user) {
        suppressCredentialSync = true;
        try {
            passwordField.setText(savedCredentials.get(user));
        } finally {
            suppressCredentialSync = false;
        }
    }

    private static int parsePositiveInt(String raw, int fallback) {
        try {
            int v = Integer.parseInt(raw.trim());
            return v > 0 ? v : fallback;
        } catch (Exception e) {
            return fallback;
        }
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

    private void appendLog(String line) {
        if (line == null) return;
        Platform.runLater(() -> {
            if (logArea == null) return;
            String l = line.endsWith("\n") ? line : (line + "\n");
            logArea.appendText(l);
        });
    }

    private void clearLog() {
        Platform.runLater(() -> {
            if (logArea != null) logArea.clear();
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
            selectedObject = null;
            connectedSchema = null;
        }
    }

    private DbObject getSelectedObject() {
        if (objectList != null) {
            DbObject fromList = objectList.getSelectionModel().getSelectedItem();
            if (fromList != null) return fromList;
        }
        return selectedObject;
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

