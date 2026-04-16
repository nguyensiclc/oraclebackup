## OracleSchemaExporter

JavaFX app to connect Oracle and export table schema.

### Run

```bash
mvn -q clean javafx:run
```

### Notes

- Uses `USER_TAB_COLUMNS`, `USER_COL_COMMENTS`, `USER_CONSTRAINTS`, `USER_CONS_COLUMNS` for metadata (current user schema).
- Export formats: SQL (CREATE TABLE + COMMENT ON COLUMN), CSV, XLSX.

