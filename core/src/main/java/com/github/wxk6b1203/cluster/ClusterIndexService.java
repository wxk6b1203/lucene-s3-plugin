package com.github.wxk6b1203.cluster;

import java.io.IOException;
import java.util.Map;

public interface ClusterIndexService {
    ClusterState createIndex(IndexSettings settings) throws IOException;

    ClusterState deleteIndex(String indexName) throws IOException;

    ClusterState putMapping(String indexName, Map<String, FieldMapping> mappings) throws IOException;
}
