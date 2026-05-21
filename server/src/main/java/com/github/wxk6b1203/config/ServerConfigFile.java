package com.github.wxk6b1203.config;

import com.github.wxk6b1203.util.JsonUtil;
import com.github.wxk6b1203.util.YamlUtil;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class ServerConfigFile {
    private final Map<String, Object> values;

    private ServerConfigFile(Map<String, Object> values) {
        this.values = values;
    }

    public static ServerConfigFile empty() {
        return new ServerConfigFile(Map.of());
    }

    public static ServerConfigFile load(String configFile) throws IOException {
        if (configFile == null || configFile.isBlank()) {
            return empty();
        }
        Path path = Path.of(configFile);
        String content = Files.readString(path);
        Map<String, Object> flattened = parse(path, content);
        Map<String, Object> normalized = new HashMap<>();
        flattened.forEach((key, value) -> normalized.put(normalizeKey(key), value));
        return new ServerConfigFile(normalized);
    }

    public Object value(String... aliases) {
        for (String alias : aliases) {
            Object value = values.get(normalizeKey(alias));
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    public String stringValue(String defaultValue, String... aliases) {
        Object value = value(aliases);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Iterable<?> iterable) {
            return join(iterable);
        }
        if (value.getClass().isArray()) {
            StringBuilder builder = new StringBuilder();
            int length = Array.getLength(value);
            for (int i = 0; i < length; i++) {
                if (!builder.isEmpty()) {
                    builder.append(',');
                }
                builder.append(Objects.toString(Array.get(value, i), ""));
            }
            return builder.toString();
        }
        return value.toString();
    }

    public int intValue(int defaultValue, String... aliases) {
        Object value = value(aliases);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(value.toString().trim());
    }

    public boolean booleanValue(boolean defaultValue, String... aliases) {
        Object value = value(aliases);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        return Boolean.parseBoolean(value.toString().trim());
    }

    private static Map<String, Object> parse(Path path, String content) {
        String fileName = path.getFileName().toString().toLowerCase(Locale.ROOT);
        if (fileName.endsWith(".json")) {
            return JsonUtil.readValueAsFlattenedMap(content);
        }
        if (fileName.endsWith(".yaml") || fileName.endsWith(".yml")) {
            return YamlUtil.readValueAsFlattenedMap(content);
        }
        String trimmed = content.stripLeading();
        if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
            return JsonUtil.readValueAsFlattenedMap(content);
        }
        return YamlUtil.readValueAsFlattenedMap(content);
    }

    private static String join(Iterable<?> values) {
        StringBuilder builder = new StringBuilder();
        for (Object value : values) {
            if (!builder.isEmpty()) {
                builder.append(',');
            }
            builder.append(Objects.toString(value, ""));
        }
        return builder.toString();
    }

    private static String normalizeKey(String key) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < key.length(); i++) {
            char ch = key.charAt(i);
            if (Character.isLetterOrDigit(ch)) {
                builder.append(Character.toLowerCase(ch));
            }
        }
        return builder.toString();
    }
}
