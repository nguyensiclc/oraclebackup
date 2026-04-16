package com.oracleexporter.model;

public class ColumnInfo {
    private final String name;
    private final String dataType;
    private final Integer length;
    private final Integer precision;
    private final Integer scale;
    private final boolean nullable;
    private final boolean primaryKey;
    private final String comment;

    public ColumnInfo(
            String name,
            String dataType,
            Integer length,
            Integer precision,
            Integer scale,
            boolean nullable,
            boolean primaryKey,
            String comment
    ) {
        this.name = name;
        this.dataType = dataType;
        this.length = length;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
        this.primaryKey = primaryKey;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public String getDataType() {
        return dataType;
    }

    public Integer getLength() {
        return length;
    }

    public Integer getPrecision() {
        return precision;
    }

    public Integer getScale() {
        return scale;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public String getComment() {
        return comment;
    }
}

