package com.github.wxk6b1203.index;

public record IndexDocumentOperation(
        String action,
        IndexDocumentRequest request
) {
    public boolean delete() {
        return "delete".equals(action);
    }
}
