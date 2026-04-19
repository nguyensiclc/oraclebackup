package com.oracleexporter.model;

public final class DbObject {
    private final DbObjectType type;
    private final String name;
    /** Partition name etc.; {@code null} if not a sub-object row. */
    private final String subObjectName;

    public DbObject(DbObjectType type, String name) {
        this(type, name, null);
    }

    public DbObject(DbObjectType type, String name, String subObjectName) {
        if (type == null) throw new IllegalArgumentException("type is null");
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name is blank");
        this.type = type;
        // Preserve case: Oracle quoted identifiers (e.g. "MyTable") are case-sensitive in data dictionary.
        this.name = name.trim();
        this.subObjectName = (subObjectName == null || subObjectName.isBlank())
                ? null
                : subObjectName.trim();
    }

    public DbObjectType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getSubObjectName() {
        return subObjectName;
    }

    /** Name shown in lists (e.g. {@code IDX_PART} or {@code EMP.SALES_Q1}). */
    public String getDisplayLabel() {
        if (subObjectName == null) {
            return name;
        }
        return name + "." + subObjectName;
    }

    /**
     * Safe base name for export files (no path separators).
     */
    public String fileBaseName() {
        String raw = subObjectName == null ? name : name + "__" + subObjectName;
        return raw.replaceAll("[\\\\/:*?\"<>|]", "_");
    }

    @Override
    public String toString() {
        return type + ":" + getDisplayLabel();
    }
}
