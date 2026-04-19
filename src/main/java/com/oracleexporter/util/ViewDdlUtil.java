package com.oracleexporter.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helpers for Oracle {@code CREATE VIEW} / {@code MATERIALIZED VIEW} DDL text.
 */
public final class ViewDdlUtil {

    /**
     * First {@code CREATE ... VIEW} / {@code MATERIALIZED VIEW} header with quoted {@code "OWNER"."NAME"}
     * becomes {@code ... VIEW "NAME"} so the object is created in the current schema.
     */
    private static final Pattern VIEW_HEADER_QUOTED_OWNER = Pattern.compile(
            "(?is)\\b(EDITIONING\\s+VIEW|EDITIONABLE\\s+VIEW|MATERIALIZED\\s+VIEW|VIEW)\\s+\"([^\"]+)\"\\s*\\.\\s*\"([^\"]+)\"");

    private ViewDdlUtil() {}

    /**
     * Drops the quoted owner so {@code CREATE ... VIEW "SCHEMA"."OBJ"} becomes {@code CREATE ... VIEW "OBJ"}
     * (first header match only). Safe for {@code @dblink} text in the query body.
     */
    public static String stripQuotedOwnerFromCreateViewDdl(String ddl) {
        if (ddl == null || ddl.isBlank()) {
            return ddl;
        }
        Matcher m = VIEW_HEADER_QUOTED_OWNER.matcher(ddl);
        return m.replaceFirst(x -> x.group(1) + " \"" + x.group(3) + "\"");
    }
}
