package com.github.wxk6b1203.metadata.common;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;

import java.io.Serializable;

public record IndexableFieldTypeSpec(
        boolean stored,
        boolean tokenized,
        boolean storeTermVectors,
        boolean storeTermVectorOffsets,
        boolean storeTermVectorPositions,
        boolean storeTermVectorPayloads,
        IndexOptions indexOptions,
        DocValuesType docValuesType,
        boolean omitNorms,
        int dimensionCount,
        int dimensionNumBytes,
        int vectorDimension) implements Serializable {

    public static IndexableFieldTypeSpec from(IndexableFieldType src) {
        if (src == null) return null;
        return new IndexableFieldTypeSpec(
                src.stored(),
                src.tokenized(),
                src.storeTermVectors(),
                src.storeTermVectorOffsets(),
                src.storeTermVectorPositions(),
                src.storeTermVectorPayloads(),
                src.indexOptions(),
                src.docValuesType(),
                src.omitNorms(),
                src.pointDimensionCount(),
                src.pointNumBytes(),
                src.vectorDimension());
    }
}