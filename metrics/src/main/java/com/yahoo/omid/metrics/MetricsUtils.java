package com.yahoo.omid.metrics;

public class MetricsUtils {

    public static final char DEFAULT_SEPARATOR = '.';

    public static String name(String name, String... otherNames) {
        return name(name, DEFAULT_SEPARATOR, otherNames);
    }

    public static String name(String name, char separator, String... otherNames) {
        final StringBuffer builder = new StringBuffer(name);
        if (otherNames != null) {
            for (String otherName : otherNames) {
                concat(builder, otherName, separator);
            }
        }
        return builder.toString();
    }

    private static void concat(StringBuffer head, String tail, char separator) {
        if (tail != null && !tail.isEmpty()) {
            if (head.length() > 0) {
                head.append(separator);
            }
            head.append(tail);
        }
    }

}
