package com.github.wxk6b1203.metadata.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class IndexMetadata {
    private String name;
    private long epoch;
    private IndexMappings mappings;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
