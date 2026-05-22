package com.github.wxk6b1203.index;

public record IndexDocumentOperationResult(
        IndexDocumentResponse response,
        Exception failure
) {
    public static IndexDocumentOperationResult success(IndexDocumentResponse response) {
        return new IndexDocumentOperationResult(response, null);
    }

    public static IndexDocumentOperationResult failure(Exception failure) {
        return new IndexDocumentOperationResult(null, failure);
    }

    public boolean failed() {
        return failure != null;
    }
}
