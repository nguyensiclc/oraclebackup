## OracleSchemaExporter

JavaFX app to connect Oracle and export table schema.

### Run

```bash
mvn -q clean javafx:run
```

### Build a Windows application (jpackage)

This project targets **Java 21**. Make sure your build machine uses **JDK 21** (check with `java -version` and `mvn -v`).

- **Portable app-image** (no installer, easiest):

```bash
mvn -q clean package
```

Output folder (after build): `target/dist/OracleSchemaExporter` (app-image)

- **Windows EXE installer** (requires WiX Toolset in PATH):

```bash
mvn -q clean -Pwindows-installer-exe package
```

The `.exe` is written under `target/dist/` (exact name depends on `jpackage`, often `OracleSchemaExporter-1.0.0.exe`).

#### Build the EXE without a Windows PC

`jpackage` cannot produce a Windows installer on macOS or Linux. Use the GitHub Actions workflow **Windows EXE installer**: push to `main`/`master` (or run it manually under **Actions**), then open the completed run and download the artifact **OracleSchemaExporter-windows-installer** (contains the `.exe`).

### Notes

- Uses `USER_TAB_COLUMNS`, `USER_COL_COMMENTS`, `USER_CONSTRAINTS`, `USER_CONS_COLUMNS` for metadata (current user schema).
- Export formats: SQL (CREATE TABLE + COMMENT ON COLUMN), CSV, XLSX.

