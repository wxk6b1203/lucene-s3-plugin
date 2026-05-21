package com.github.wxk6b1203.metadata.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class IndexCommitSnapshotPin {
    private String indexName;
    private long generation;
    private String pinId;
    private long expiresAtMillis;
}
