package com.github.wxk6b1203.metadata.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.concurrent.locks.ReentrantReadWriteLock;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class IndexFileMetadata {
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private String indexName;
    private String name;
    private long epoch;
    private long size;
    private long checksum;
    private long modifiedTime;
    private IndexFileStatus status;

    public boolean transitionTo(IndexFileStatus newStatus) {
        try {
            lock.writeLock().lock();
            boolean allowed = IndexFileStatus.validTransition(status, newStatus);
            this.status = newStatus;
            return allowed;
        } finally {
            lock.writeLock().unlock();
        }

    }
}
