package com.oracleexporter.model;

import java.util.Locale;

public final class DbObject {
    private final DbObjectType type;
    private final String name;

    public DbObject(DbObjectType type, String name) {
        if (type == null) throw new IllegalArgumentException("type is null");
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name is blank");
        this.type = type;
        this.name = name.trim().toUpperCase(Locale.ROOT);
    }

    public DbObjectType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return type + ":" + name;
    }
}

