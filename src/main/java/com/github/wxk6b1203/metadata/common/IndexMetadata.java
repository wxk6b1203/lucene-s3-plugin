package com.github.wxk6b1203.metadata.common;

import com.github.wxk6b1203.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class IndexMetadata implements Serializable {
    private String name;
    private Long epoch;
    private IndexMappings mappings;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public byte[] json() {
        return JsonUtil.writeValueAsBytes(this);
    }

    public static IndexMetadata json(byte[] json) {
        return JsonUtil.readValue(json, IndexMetadata.class);
    }

    public static IndexMetadata json(String json) {
        return JsonUtil.readValue(json, IndexMetadata.class);
    }
}
