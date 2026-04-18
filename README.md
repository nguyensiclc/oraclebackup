## OracleSchemaExporter

JavaFX app to connect Oracle and export table schema.

Requires **JDK 21**. Check with `java -version` and `mvn -v`.

### Run (development)

```bash
mvn -q clean javafx:run
```

### Build

Compile and produce the JAR (no native installer):

```bash
mvn -q clean package
```

Output: `target/OracleSchemaExporter-<version>.jar` (see `version` in `pom.xml`).

### Notes

- Uses `USER_TAB_COLUMNS`, `USER_COL_COMMENTS`, `USER_CONSTRAINTS`, `USER_CONS_COLUMNS` for metadata (current user schema).
- Export formats: SQL (CREATE TABLE + COMMENT ON COLUMN), CSV, XLSX.
