package com.github.wxk6b1203.util;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

public class JsonUtil {
    public static JsonMapper mapper;

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
}
