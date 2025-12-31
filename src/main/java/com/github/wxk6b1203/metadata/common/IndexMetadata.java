package com.github.wxk6b1203.metadata.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IndexMetadata {
    private String indexName;
    private long epoch;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
