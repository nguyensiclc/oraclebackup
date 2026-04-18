package com.oracleexporter.model;

/**
 * Mirrors Oracle {@code USER_OBJECTS.OBJECT_TYPE} values we support for export/import.
 */
public enum DbObjectType {
    TABLE,
    VIEW,
    MATERIALIZED_VIEW,
    SYNONYM,
    PROCEDURE,
    FUNCTION,
    INDEX,
    INDEX_PARTITION,
    LOB,
    PACKAGE,
    PACKAGE_BODY,
    SEQUENCE,
    TABLE_PARTITION,
    TYPE,
    DATABASE_LINK
}
