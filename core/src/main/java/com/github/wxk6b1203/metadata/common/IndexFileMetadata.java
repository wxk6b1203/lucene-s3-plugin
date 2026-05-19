package com.github.wxk6b1203.metadata.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class IndexFileMetadata {
    private String indexName;
    private String name;
    private String dataDirectory;
    private String objectKey;
    private long epoch;
    private long size;
    private long checksum;
    private long modifiedTime;
    private IndexFileStatus status;
}
