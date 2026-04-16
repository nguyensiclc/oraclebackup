package com.oracleexporter.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableInfo {
    private final String name;
    private final List<ColumnInfo> columns = new ArrayList<>();

    public TableInfo(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public List<ColumnInfo> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public void addColumn(ColumnInfo column) {
        columns.add(column);
    }
}

