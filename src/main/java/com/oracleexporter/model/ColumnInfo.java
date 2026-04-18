package com.oracleexporter.model;

public class ColumnInfo {
    private final String name;
    private final String dataType;
    private final Integer length;
    private final Integer precision;
    private final Integer scale;
    private final boolean nullable;
    private final boolean primaryKey;
    /**
     * When {@code true}, Oracle {@code VARCHAR2}/{@code CHAR} length was defined in <b>characters</b>
     * ({@code CHAR_USED = 'C'}). Export must emit {@code VARCHAR2(n CHAR)} so import DBs defaulting to BYTE
     * semantics do not shrink capacity for Japanese / multi-byte text.
     */
    private final boolean charLengthSemantics;
    private final String comment;

    public ColumnInfo(
            String name,
            String dataType,
            Integer length,
            Integer precision,
            Integer scale,
            boolean nullable,
            boolean primaryKey,
            boolean charLengthSemantics,
            String comment
    ) {
        this.name = name;
        this.dataType = dataType;
        this.length = length;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
        this.primaryKey = primaryKey;
        this.charLengthSemantics = charLengthSemantics;
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

    /** {@code true} if source column used character-length semantics ({@code CHAR} in Oracle DDL). */
    public boolean isCharLengthSemantics() {
        return charLengthSemantics;
    }

    public String getComment() {
        return comment;
    }
}

