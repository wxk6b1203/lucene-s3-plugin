package com.github.wxk6b1203.metadata.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class IndexCommitSnapshot {
    private String indexName;
    private long generation;
    private String segmentFileName;
    private List<IndexFileMetadata> files;
    private long createdAtMillis;
}
