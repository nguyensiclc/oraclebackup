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
import javafx.scene.text.Text;
import javafx.scene.text.TextFlow;
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
import java.util.concurrent.CancellationException;

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
    private static final String CFG_IMPORT_DATA_ONLY = "import.dataOnly";

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
    @FXML private CheckBox importDataOnlyCheck;
    @FXML private Button loadPreviewButton;
    @FXML private Button exportButton;
    @FXML private Button exportAllButton;
    @FXML private Button importButton;
    @FXML private Button stopButton;

    @FXML private ProgressBar progressBar;
    @FXML private Label statusLabel;
    @FXML private ScrollPane logScroll;
    @FXML private TextFlow logTextFlow;
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
    /** Set while export, export-all, or import runs; may be {@link Task#cancel() cancelled. */
    private Task<?> activeCancellableTask;
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
        if (importDataOnlyCheck != null) {
            importDataOnlyCheck.selectedProperty().addListener((obs, o, n) -> saveFieldsToConfig());
        }
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
        if (stopButton != null) {
            stopButton.setGraphic(textGraphic("⏹"));
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
        p.setProperty(CFG_IMPORT_DATA_ONLY, "false");
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
        clearLog();
    }

    @FXML
    private void onStop() {
        Task<?> t = activeCancellableTask;
        if (t == null || t.isDone()) {
            return;
        }
        appendLog("[STOP] Stopping (partial work is kept)…");
        t.cancel();
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

        operationInProgress = true;
        boolean includeDml = includeDmlCheck.isSelected();
        final Integer dataMaxRows = (includeDml && obj.getType() == DbObjectType.TABLE) ? resolveDataExportMaxRows() : null;
        final DbObject exportObj = obj;

        LocalDateTime now = LocalDateTime.now();
        String exportFolderName = connectedSchema + "_" + EXPORT_TS_FMT.format(now);
        Path exportRoot = exportBaseDir.resolve(exportFolderName);
        Path typeRoot = exportRoot.resolve(ImportService.typeFolder(exportObj.getType()));
        Path ddlDir = typeRoot.resolve("ddl");
        Path dataDir = typeRoot.resolve("data");

        clearLogNow();
        appendLog("[EXPORT] Folder: " + exportRoot);

        Task<Path> task = new Task<>() {
            @Override
            protected Path call() throws Exception {
                updateMessage("Preparing export folders...");
                updateProgress(-1, 1);

                Files.createDirectories(ddlDir);
                Files.createDirectories(dataDir);
                if (isCancelled()) {
                    return exportRoot;
                }

                if (exportObj.getType() == DbObjectType.TABLE) {
                    boolean ddlOk = false;
                    if (!isCancelled()) {
                        try {
                            TableInfo tableInfo = metadataService.loadTableInfo(connection, exportObj.getName());
                            if (isCancelled()) {
                                return exportRoot;
                            }
                            Path ddlTarget = ddlDir.resolve(exportObj.fileBaseName() + ".sql");
                            updateMessage("Exporting DDL to " + ddlTarget.getFileName() + "...");
                            exportService.exportSql(tableInfo, ddlTarget);
                            appendLog("[EXPORT][OK] TABLE DDL " + exportObj.getDisplayLabel());
                            ddlOk = true;
                        } catch (Exception ex) {
                            appendLog("[EXPORT][FAIL] TABLE DDL " + exportObj.getDisplayLabel() + " :: " + exportErrorMessage(ex));
                        }
                    }
                    if (includeDml && ddlOk && !isCancelled()) {
                        try {
                            Path dataTarget = dataDir.resolve(exportObj.fileBaseName() + ".sql");
                            updateMessage("Exporting DATA to " + dataTarget.getFileName() + "...");
                            exportService.exportDataSql(
                                    connection, exportObj.getName(), dataTarget, dataMaxRows, this::isCancelled);
                            if (!isCancelled()) {
                                appendLog("[EXPORT][OK] TABLE DATA " + exportObj.getDisplayLabel());
                            } else {
                                appendLog("[EXPORT][STOP] TABLE DATA " + exportObj.getDisplayLabel() + " (file may be partial)");
                            }
                        } catch (CancellationException ex) {
                            appendLog("[EXPORT][STOP] TABLE DATA " + exportObj.getDisplayLabel() + " (file may be partial)");
                        } catch (Exception ex) {
                            appendLog("[EXPORT][FAIL] TABLE DATA " + exportObj.getDisplayLabel() + " :: " + exportErrorMessage(ex));
                        }
                    }
                } else {
                    if (isCancelled()) {
                        return exportRoot;
                    }
                    try {
                        String ddl = metadataService.loadObjectDdl(connection, connectedSchema, exportObj);
                        if (isCancelled()) {
                            return exportRoot;
                        }
                        if (ddl == null || ddl.isBlank()) {
                            appendLog("[EXPORT][FAIL] " + exportObj.getType() + " " + exportObj.getDisplayLabel()
                                    + " :: Could not load DDL (empty)");
                        } else {
                            Path ddlTarget = ddlDir.resolve(exportObj.fileBaseName() + ".sql");
                            updateMessage("Exporting DDL to " + ddlTarget.getFileName() + "...");
                            exportService.exportRawSql(ddl, ddlTarget);
                            appendLog("[EXPORT][OK] " + exportObj.getType() + " " + exportObj.getDisplayLabel());
                        }
                    } catch (Exception ex) {
                        appendLog("[EXPORT][FAIL] " + exportObj.getDisplayLabel() + " :: " + exportErrorMessage(ex));
                    }
                }

                return exportRoot;
            }
        };
        activeCancellableTask = task;
        bindTask(task);
        task.setOnSucceeded(e -> {
            if (task.isCancelled()) {
                unbindStatusFromTask();
                onCancellableTaskEnded(task, true, null);
                return;
            }
            setStatus("Export finished. Folder: " + task.getValue());
            onCancellableTaskEnded(task, false, null);
        });
        task.setOnFailed(e -> {
            Throwable ex = task.getException();
            appendLog("[EXPORT][FAIL] " + exportErrorMessage(ex));
            setStatus("Export failed.");
            onCancellableTaskEnded(task, false, ex);
        });
        task.setOnCancelled(e -> {
            unbindStatusFromTask();
            appendLog("[EXPORT] Stopped. Files written so far are left on disk.");
            setStatus("Export stopped. Partial result kept in folder: " + exportRoot);
            onCancellableTaskEnded(task, true, null);
        });
        disableWhileBusy(true);
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

        operationInProgress = true;
        boolean includeDml = includeDmlCheck.isSelected();
        final Integer dataMaxRows = includeDml ? resolveDataExportMaxRows() : null;
        final List<DbObject> toExport = objectsToExport;

        LocalDateTime now = LocalDateTime.now();
        String exportFolderName = connectedSchema + "_" + EXPORT_TS_FMT.format(now);
        final Path exportRoot = exportBaseDir.resolve(exportFolderName);

        clearLogNow();
        appendLog("[EXPORT ALL] Folder: " + exportRoot);
        appendLog("[EXPORT ALL] Objects: " + toExport.size());

        Task<Path> task = new Task<>() {
            @Override
            protected Path call() throws Exception {
                updateMessage("Preparing export folders...");
                updateProgress(0, toExport.size());

                int total = toExport.size();
                int ok = 0;
                int fail = 0;
                for (int i = 0; i < total; i++) {
                    if (isCancelled()) {
                        appendLog("[EXPORT ALL] Stopped after " + (i) + " object(s).");
                        break;
                    }
                    DbObject obj = toExport.get(i);
                    if (obj == null) {
                        continue;
                    }

                    updateProgress(i, total);
                    updateMessage("Exporting " + obj.getType() + " " + obj.getDisplayLabel() + " (" + (i + 1) + "/" + total + ")...");

                    Path typeRoot = exportRoot.resolve(ImportService.typeFolder(obj.getType()));
                    Path ddlDir = typeRoot.resolve("ddl");
                    Path dataDir = typeRoot.resolve("data");
                    Files.createDirectories(ddlDir);
                    Files.createDirectories(dataDir);

                    try {
                        if (obj.getType() == DbObjectType.TABLE) {
                            boolean stepOk = true;
                            if (!isCancelled()) {
                                try {
                                    TableInfo tableInfo = metadataService.loadTableInfo(connection, obj.getName());
                                    if (isCancelled()) {
                                        break;
                                    }
                                    exportService.exportSql(tableInfo, ddlDir.resolve(obj.fileBaseName() + ".sql"));
                                    appendLog("[EXPORT][OK] TABLE DDL " + obj.getDisplayLabel());
                                } catch (Exception ex) {
                                    stepOk = false;
                                    appendLog("[EXPORT][FAIL] TABLE DDL " + obj.getDisplayLabel() + " :: " + exportErrorMessage(ex));
                                }
                            }
                            if (includeDml && stepOk && !isCancelled()) {
                                try {
                                    exportService.exportDataSql(
                                            connection, obj.getName(), dataDir.resolve(obj.fileBaseName() + ".sql"), dataMaxRows, this::isCancelled);
                                    if (isCancelled()) {
                                        appendLog("[EXPORT][STOP] TABLE DATA " + obj.getDisplayLabel() + " (file may be partial)");
                                        break;
                                    }
                                    appendLog("[EXPORT][OK] TABLE DATA " + obj.getDisplayLabel());
                                } catch (CancellationException ex) {
                                    appendLog("[EXPORT][STOP] TABLE DATA " + obj.getDisplayLabel() + " (file may be partial)");
                                    break;
                                } catch (Exception ex) {
                                    stepOk = false;
                                    appendLog("[EXPORT][FAIL] TABLE DATA " + obj.getDisplayLabel() + " :: " + exportErrorMessage(ex));
                                }
                            }
                            if (isCancelled()) {
                                break;
                            }
                            if (stepOk) {
                                ok++;
                            } else {
                                fail++;
                            }
                        } else {
                            if (isCancelled()) {
                                break;
                            }
                            String ddl = metadataService.loadObjectDdl(connection, connectedSchema, obj);
                            if (isCancelled()) {
                                break;
                            }
                            if (ddl == null || ddl.isBlank()) {
                                appendLog("[EXPORT][FAIL] " + obj.getType() + " " + obj.getDisplayLabel()
                                        + " :: Could not load DDL (empty)");
                                fail++;
                                continue;
                            }
                            Path ddlTarget = ddlDir.resolve(obj.fileBaseName() + ".sql");
                            exportService.exportRawSql(ddl, ddlTarget);
                            appendLog("[EXPORT][OK] " + obj.getType() + " " + obj.getDisplayLabel());
                            ok++;
                        }
                    } catch (Exception ex) {
                        if (ex instanceof CancellationException) {
                            break;
                        }
                        fail++;
                        appendLog("[EXPORT][FAIL] " + obj.getDisplayLabel() + " :: " + exportErrorMessage(ex));
                    }
                }

                updateProgress(total, total);
                updateMessage("Done.");
                if (!isCancelled()) {
                    appendLog("[EXPORT ALL] Finished. OK: " + ok + ", failed/skipped: " + fail);
                }
                return exportRoot;
            }
        };

        activeCancellableTask = task;
        bindTask(task);
        task.setOnSucceeded(e -> {
            if (task.isCancelled()) {
                unbindStatusFromTask();
                onCancellableTaskEnded(task, true, null);
                return;
            }
            setStatus("Export finished. Folder: " + task.getValue());
            onCancellableTaskEnded(task, false, null);
        });
        task.setOnFailed(e -> {
            Throwable ex = task.getException();
            appendLog("[EXPORT ALL][FAIL] " + exportErrorMessage(ex));
            setStatus("Export failed.");
            onCancellableTaskEnded(task, false, ex);
        });
        task.setOnCancelled(e -> {
            unbindStatusFromTask();
            setStatus("Export all stopped. Partial result kept: " + exportRoot);
            appendLog("[EXPORT ALL] Stopped. Folders and files already written are kept.");
            onCancellableTaskEnded(task, true, null);
        });
        disableWhileBusy(true);
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
        final boolean dataOnly = importDataOnlyCheck != null && importDataOnlyCheck.isSelected();
        final Path importPath = importDir;
        clearLog();
        appendLog("[IMPORT] Root folder: " + importPath);
        if (dataOnly) {
            appendLog("[IMPORT] Data only: will skip table DDL, run other DDL, then data.");
        }

        Task<Path> task = new Task<>() {
            @Override
            protected Path call() throws Exception {
                updateMessage("Preparing import...");
                updateProgress(-1, 1);
                if (isCancelled()) {
                    return importPath;
                }
                if (!dataOnly) {
                    updateMessage("Checking schema is empty...");
                    metadataService.requireSchemaEmptyForImport(connection);
                }
                if (isCancelled()) {
                    return importPath;
                }
                updateMessage("Importing from folder...");
                boolean prevAutoCommit = true;
                try {
                    try {
                        prevAutoCommit = connection.getAutoCommit();
                    } catch (SQLException ignored) {
                    }
                    try {
                        connection.setAutoCommit(false);
                    } catch (SQLException ignored) {
                    }
                    importService.importFromExportFolder(
                            connection, importPath,
                            new ImportService.ImportOptions(dataOnly, this::isCancelled),
                            MainController.this::appendLog);
                } finally {
                    try {
                        connection.commit();
                    } catch (SQLException ignored) {
                    }
                    try {
                        connection.setAutoCommit(prevAutoCommit);
                    } catch (SQLException ignored) {
                    }
                }
                return importPath;
            }
        };

        activeCancellableTask = task;
        bindTask(task);
        task.setOnSucceeded(e -> {
            if (task.isCancelled()) {
                unbindStatusFromTask();
                onCancellableTaskEnded(task, true, null);
                return;
            }
            appendLog("[IMPORT] Finished.");
            setStatus("Imported from folder: " + task.getValue());
            onCancellableTaskEnded(task, false, null);
        });
        task.setOnFailed(e -> {
            Throwable ex = task.getException();
            appendLog("[IMPORT][FAIL] " + (ex == null ? "Unknown error" : exportErrorMessage(ex)));
            setStatus("Import failed.");
            onCancellableTaskEnded(task, false, ex);
        });
        task.setOnCancelled(e -> {
            unbindStatusFromTask();
            setStatus("Import stopped. Committed work so far is kept.");
            appendLog("[IMPORT] Stopped. Changes that were already applied are committed (except failed per-file rollbacks).");
            onCancellableTaskEnded(task, true, null);
        });
        disableWhileBusy(true);
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

    private void unbindStatusFromTask() {
        if (statusLabel != null) {
            try {
                statusLabel.textProperty().unbind();
            } catch (Exception ignored) {
            }
        }
        if (progressBar != null) {
            try {
                progressBar.progressProperty().unbind();
            } catch (Exception ignored) {
            }
        }
    }

    private void onCancellableTaskEnded(Task<?> task, boolean cancelled, Throwable ex) {
        if (activeCancellableTask == task) {
            activeCancellableTask = null;
        }
        operationInProgress = false;
        disableWhileBusy(false);
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
        if (stopButton != null) {
            boolean showStop = activeCancellableTask != null;
            stopButton.setDisable(!showStop);
        }
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
        if (importDataOnlyCheck != null) {
            importDataOnlyCheck.setSelected(Boolean.parseBoolean(config.getProperty(CFG_IMPORT_DATA_ONLY, "false")));
        }
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
        if (importDataOnlyCheck != null) {
            config.setProperty(CFG_IMPORT_DATA_ONLY, Boolean.toString(importDataOnlyCheck.isSelected()));
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
        if (line == null) {
            return;
        }
        Platform.runLater(() -> {
            if (logTextFlow == null) {
                return;
            }
            String l = line.endsWith("\n") ? line : (line + "\n");
            Text t = new Text(l);
            t.getStyleClass().add("log-line");
            if (l.contains("[FAIL]")) {
                t.getStyleClass().add("log-line-fail");
            } else if (l.contains("[OK]")) {
                t.getStyleClass().add("log-line-ok");
            } else {
                t.getStyleClass().add("log-line-neutral");
            }
            logTextFlow.getChildren().add(t);
            scrollLogToBottom();
        });
    }

    private void scrollLogToBottom() {
        if (logScroll == null) {
            return;
        }
        logScroll.applyCss();
        logScroll.layout();
        logScroll.setVvalue(1.0);
    }

    private void clearLog() {
        Platform.runLater(() -> {
            if (logTextFlow != null) {
                logTextFlow.getChildren().clear();
            }
        });
    }

    /** Clears log immediately when already on the JavaFX application thread. */
    private void clearLogNow() {
        if (logTextFlow != null) {
            logTextFlow.getChildren().clear();
        }
    }

    private static String exportErrorMessage(Throwable ex) {
        if (ex == null) {
            return "Unknown error";
        }
        String m = ex.getMessage();
        if (m == null || m.isBlank()) {
            return ex.getClass().getSimpleName();
        }
        return m;
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

