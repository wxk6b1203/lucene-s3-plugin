package com.github.wxk6b1203.util;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.util.Map;

import static com.github.wxk6b1203.util.JsonUtil.flattenMap;

public class YamlUtil {
    private static final YAMLMapper mapper;

    static {
        mapper = YAMLMapper.builder()
                // 读取更宽容：配置文件升级/字段新增时不至于报错
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                // 读取更宽容：空串用 null 处理（YAML/配置里很常见）
                .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
                // 输出更宽容：避免某些“空对象/无 getter”类型序列化时报错
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .build();
    }

    public static <T> T readValue(String yaml, Class<T> valueType) {
        return mapper.readValue(yaml, valueType);
    }

    public static String writeValueAsString(Object value) {
        return mapper.writeValueAsString(value);
    }

    public static Map<String, Object> readValueAsMap(String yaml) {
        return mapper.readValue(yaml, new TypeReference<>() {
        });
    }

    public static Map<String, Object> readValueAsFlattenedMap(String yaml) {
        Map<String, Object> raw = mapper.readValue(yaml, new TypeReference<>() {
        });

        Map<String, Object> result = new java.util.HashMap<>();
        flattenMap("", raw, result);

        return result;
    }
}
