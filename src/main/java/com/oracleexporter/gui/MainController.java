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
    private static final String CFG_IMPORT_DIR = "import.baseDir";
    private static final String CFG_DATA_MAX_ROWS = "export.data.maxRows";
    private static final String CFG_DATA_ALL_ROWS = "export.data.allRows";

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
    @FXML private TextField importFolderField;
    @FXML private Button browseImportFolderButton;
    @FXML private TextField tableFilterField;
    @FXML private TabPane objectTypeTabs;
    @FXML private ListView<DbObject> tableList;
    @FXML private ListView<DbObject> viewList;
    @FXML private ListView<DbObject> mviewList;
    @FXML private ListView<DbObject> procedureList;

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
    private final ObservableList<DbObject> tableObjects = FXCollections.observableArrayList();
    private final ObservableList<DbObject> viewObjects = FXCollections.observableArrayList();
    private final ObservableList<DbObject> mviewObjects = FXCollections.observableArrayList();
    private final ObservableList<DbObject> procedureObjects = FXCollections.observableArrayList();

    private FilteredList<DbObject> filteredTables;
    private FilteredList<DbObject> filteredViews;
    private FilteredList<DbObject> filteredMviews;
    private FilteredList<DbObject> filteredProcedures;

    private boolean updatingSelection;
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
        if (importButton != null) importButton.getStyleClass().add("accent");

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

        filteredTables = new FilteredList<>(tableObjects, o -> true);
        filteredViews = new FilteredList<>(viewObjects, o -> true);
        filteredMviews = new FilteredList<>(mviewObjects, o -> true);
        filteredProcedures = new FilteredList<>(procedureObjects, o -> true);

        setupObjectList(tableList, filteredTables);
        setupObjectList(viewList, filteredViews);
        setupObjectList(mviewList, filteredMviews);
        setupObjectList(procedureList, filteredProcedures);

        hostField.textProperty().addListener((obs, o, n) -> saveFieldsToConfig());
        portField.textProperty().addListener((obs, o, n) -> saveFieldsToConfig());
        serviceField.textProperty().addListener((obs, o, n) -> saveFieldsToConfig());
        userField.textProperty().addListener((obs, o, n) -> saveFieldsToConfig());

        tableFilterField.textProperty().addListener((obs, oldV, newV) -> applyTableFilter(newV));

        wireSelection(tableList);
        wireSelection(viewList);
        wireSelection(mviewList);
        wireSelection(procedureList);

        setStatus("Ready.");
        progressBar.setProgress(0);

        refreshDataExportControlsState();
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
                    return;
                }
                setText(item.getName());
            }
        });
    }

    private void wireSelection(ListView<DbObject> lv) {
        if (lv == null) return;
        lv.getSelectionModel().selectedItemProperty().addListener((obs, oldV, newV) -> {
            if (updatingSelection) return;
            if (newV == null) return;

            updatingSelection = true;
            try {
                if (lv != tableList && tableList != null) tableList.getSelectionModel().clearSelection();
                if (lv != viewList && viewList != null) viewList.getSelectionModel().clearSelection();
                if (lv != mviewList && mviewList != null) mviewList.getSelectionModel().clearSelection();
                if (lv != procedureList && procedureList != null) procedureList.getSelectionModel().clearSelection();
            } finally {
                updatingSelection = false;
            }

            selectedObject = newV;
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
                String user = userField.getText();
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
            rebuildTypeLists();
            applyTableFilter(tableFilterField.getText());
            clearAllSelections();
            selectedObject = null;
            currentTable = null;
            connectedSchema = safeSchemaName(resolveCurrentSchema(connection));
            columnTable.getItems().clear();
            setStatus("Connected. Loaded " + objects.size() + " objects.");
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
        allObjects.clear();
        tableObjects.clear();
        viewObjects.clear();
        mviewObjects.clear();
        procedureObjects.clear();
        applyTableFilter(tableFilterField.getText());
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
        Path typeRoot = exportRoot.resolve(typeFolder(obj.getType()));
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
                    Path ddlTarget = ddlDir.resolve(obj.getName() + ".sql");
                    updateMessage("Exporting DDL to " + ddlTarget.getFileName() + "...");
                    exportService.exportSql(tableInfo, ddlTarget);

                    if (includeDml) {
                        Path dataTarget = dataDir.resolve(obj.getName() + ".sql");
                        updateMessage("Exporting DATA to " + dataTarget.getFileName() + "...");
                        exportService.exportDataSql(connection, obj.getName(), dataTarget, dataMaxRows);
                    }
                } else {
                    String ddl = metadataService.loadObjectDdl(connection, connectedSchema, obj);
                    if (ddl == null || ddl.isBlank()) {
                        throw new IllegalStateException("Could not load DDL for " + obj.getType() + " " + obj.getName());
                    }
                    Path ddlTarget = ddlDir.resolve(obj.getName() + ".sql");
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
        DbObjectType exportType = resolveSelectedTypeForExportAll();
        List<DbObject> objectsToExport = resolveObjectsForType(exportType);
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
        Path selectedTypeRoot = exportRoot.resolve(typeFolder(exportType));
        Path ddlDir = selectedTypeRoot.resolve("ddl");
        Path dataDir = selectedTypeRoot.resolve("data");

        operationInProgress = true;
        disableWhileBusy(true);

        Task<Path> task = new Task<>() {
            @Override
            protected Path call() throws Exception {
                updateMessage("Preparing export folders...");
                updateProgress(0, objectsToExport.size());

                Files.createDirectories(ddlDir);
                Files.createDirectories(dataDir);

                int total = objectsToExport.size();
                for (int i = 0; i < total; i++) {
                    DbObject obj = objectsToExport.get(i);
                    if (obj == null) continue;

                    updateProgress(i, total);
                    updateMessage("Exporting " + obj.getType() + " " + obj.getName() + " (" + (i + 1) + "/" + total + ")...");

                    if (obj.getType() == DbObjectType.TABLE) {
                        TableInfo tableInfo = metadataService.loadTableInfo(connection, obj.getName());
                        exportService.exportSql(tableInfo, ddlDir.resolve(obj.getName() + ".sql"));

                        if (includeDml) {
                            exportService.exportDataSql(connection, obj.getName(), dataDir.resolve(obj.getName() + ".sql"), dataMaxRows);
                        }
                    } else {
                        String ddl = metadataService.loadObjectDdl(connection, connectedSchema, obj);
                        if (ddl == null || ddl.isBlank()) {
                            throw new IllegalStateException("Could not load DDL for " + obj.getType() + " " + obj.getName());
                        }
                        Path ddlTarget = ddlDir.resolve(obj.getName() + ".sql");
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

        Task<Path> task = new Task<>() {
            @Override
            protected Path call() throws Exception {
                updateMessage("Checking schema is empty...");
                updateProgress(-1, 1);
                if (!metadataService.isSchemaEmpty(connection)) {
                    throw new IllegalStateException("Target schema is not empty. Please use a new/empty schema.");
                }

                updateMessage("Importing from folder...");
                importService.importFromExportFolder(connection, importDir);

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

    private DbObjectType resolveSelectedTypeForExportAll() {
        if (objectTypeTabs == null) return DbObjectType.TABLE;
        int idx = objectTypeTabs.getSelectionModel().getSelectedIndex();
        return switch (idx) {
            case 1 -> DbObjectType.VIEW;
            case 2 -> DbObjectType.MATERIALIZED_VIEW;
            case 3 -> DbObjectType.PROCEDURE;
            default -> DbObjectType.TABLE;
        };
    }

    private List<DbObject> resolveObjectsForType(DbObjectType type) {
        return switch (type) {
            case TABLE -> List.copyOf(tableObjects);
            case VIEW -> List.copyOf(viewObjects);
            case MATERIALIZED_VIEW -> List.copyOf(mviewObjects);
            case PROCEDURE -> List.copyOf(procedureObjects);
        };
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
        if (tableList != null) tableList.setDisable(busy || !connected);
        if (viewList != null) viewList.setDisable(busy || !connected);
        if (mviewList != null) mviewList.setDisable(busy || !connected);
        if (procedureList != null) procedureList.setDisable(busy || !connected);
        tableFilterField.setDisable(busy || !connected);
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
        java.util.function.Predicate<DbObject> pred = o -> {
            if (o == null) return false;
            if (f.isEmpty()) return true;
            return o.getName().toUpperCase(Locale.ROOT).contains(f)
                    || o.getType().name().toUpperCase(Locale.ROOT).contains(f);
        };
        filteredTables.setPredicate(pred);
        filteredViews.setPredicate(pred);
        filteredMviews.setPredicate(pred);
        filteredProcedures.setPredicate(pred);
    }

    private void rebuildTypeLists() {
        tableObjects.clear();
        viewObjects.clear();
        mviewObjects.clear();
        procedureObjects.clear();
        for (DbObject o : allObjects) {
            if (o == null) continue;
            switch (o.getType()) {
                case TABLE -> tableObjects.add(o);
                case VIEW -> viewObjects.add(o);
                case MATERIALIZED_VIEW -> mviewObjects.add(o);
                case PROCEDURE -> procedureObjects.add(o);
            }
        }
    }

    private void clearAllSelections() {
        updatingSelection = true;
        try {
            if (tableList != null) tableList.getSelectionModel().clearSelection();
            if (viewList != null) viewList.getSelectionModel().clearSelection();
            if (mviewList != null) mviewList.getSelectionModel().clearSelection();
            if (procedureList != null) procedureList.getSelectionModel().clearSelection();
        } finally {
            updatingSelection = false;
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

    private void saveFieldsToConfig() {
        config.setProperty(CFG_HOST, nv(hostField.getText()).trim());
        config.setProperty(CFG_PORT, nv(portField.getText()).trim());
        config.setProperty(CFG_SERVICE, nv(serviceField.getText()).trim());
        config.setProperty(CFG_USER, nv(userField.getText()).trim());
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

