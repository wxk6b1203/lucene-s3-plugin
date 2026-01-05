package com.github.wxk6b1203.util;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;

public class JsonUtil {
    public static final JsonMapper mapper;

    static {
        mapper = JsonMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .build();
    }

    public static byte[] writeValueAsBytes(Object value) {
        return mapper.writeValueAsBytes(value);
    }

    public static <T> T readValue(byte[] json, Class<T> valueType) {
        return mapper.readValue(json, valueType);
    }

    public static <T> T readValue(String json, Class<T> valueType) {
        return mapper.readValue(json, valueType);
    }

    public static <T> T readValue(String json, TypeReference<T> valueTypeRef) {
        return mapper.readValue(json, valueTypeRef);
    }

    public static Map<String, Object> readValueAsMap(String json) {
        return mapper.readValue(json, new TypeReference<>() {
        });
    }

    public static Map<String, Object> readValueAsFlattenedMap(String json) {
        Map<String, Object> raw = mapper.readValue(json, new TypeReference<>() {
        });

        Map<String, Object> result = new HashMap<>();
        flattenMap("", raw, result);

        return result;
    }

    static void flattenMap(String prefix, Map<?, ?> raw, Map<String, Object> result) {
        if (raw == null || raw.isEmpty()) {
            return;
        }

        for (Map.Entry<?, ?> entry : raw.entrySet()) {
            Object k = entry.getKey();
            if (!(k instanceof String)) {
                // 非 String key 无法组成扁平 key，这里转字符串
                k = String.valueOf(k);
            }

            String key = (prefix == null || prefix.isEmpty()) ? (String) k : prefix + "." + k;
            flattenAny(key, entry.getValue(), result);
        }
    }

    /**
     * Flatten a value at the given key.
     * <p>
     * Rules:
     * - Map: always recurse.
     * - Iterable/Array: expand only if it contains nested containers (Map / Iterable / Array).
     * Otherwise, treat it as a leaf value and keep as-is.
     * - Conflicts: overwrite (Map#put default behavior).
     */
    private static void flattenAny(String key, Object value, Map<String, Object> result) {
        if (value instanceof Map<?, ?> m) {
            flattenMap(key, m, result);
            return;
        }

        if (value instanceof Iterable<?> it) {
            if (containsContainer(it)) {
                int i = 0;
                for (Object elem : it) {
                    flattenAny(key + "[" + i + "]", elem, result);
                    i++;
                }
            } else {
                result.put(key, value);
            }
            return;
        }

        if (value != null && value.getClass().isArray()) {
            if (containsContainerArray(value)) {
                int len = java.lang.reflect.Array.getLength(value);
                for (int i = 0; i < len; i++) {
                    flattenAny(key + "[" + i + "]", java.lang.reflect.Array.get(value, i), result);
                }
            } else {
                result.put(key, value);
            }
            return;
        }

        result.put(key, value);
    }

    private static boolean isContainerLike(Object elem) {
        if (elem == null) {
            return false;
        }
        return elem instanceof Map<?, ?>
                || elem instanceof Iterable<?>
                || elem.getClass().isArray();
    }

    private static boolean containsContainer(Iterable<?> it) {
        for (Object elem : it) {
            if (isContainerLike(elem)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsContainerArray(Object array) {
        int len = java.lang.reflect.Array.getLength(array);
        for (int i = 0; i < len; i++) {
            if (isContainerLike(java.lang.reflect.Array.get(array, i))) {
                return true;
            }
        }
        return false;
    }
}
