package com.github.wxk6b1203.index;

import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.cluster.ShardId;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

final class MappingAnalyzer extends AnalyzerWrapper {
    private final ShardId shardId;
    private final ConcurrentMap<ShardId, Map<String, FieldMapping>> mappingsByShard;
    private final AnalyzerRegistry analyzerRegistry;

    MappingAnalyzer(
            ShardId shardId,
            ConcurrentMap<ShardId, Map<String, FieldMapping>> mappingsByShard,
            AnalyzerRegistry analyzerRegistry
    ) {
        super(PER_FIELD_REUSE_STRATEGY);
        this.shardId = shardId;
        this.mappingsByShard = mappingsByShard;
        this.analyzerRegistry = analyzerRegistry;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        Map<String, FieldMapping> mappings = mappingsByShard.getOrDefault(shardId, Map.of());
        FieldMapping mapping = mappings.get(fieldName);
        return analyzerRegistry.analyzer(mapping == null ? null : mapping.analyzer());
    }

    @Override
    protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        return components;
    }
}
