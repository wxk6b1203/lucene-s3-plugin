package com.github.wxk6b1203.store.manifest;

public record DeleteTask(String indexName,
                         long epoch,
                         String name) {
}
