package com.github.wxk6b1203.metadata.provider.mem;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MemMockProvider extends ManifestMetadataManager {
    private final ConcurrentHashMap<String, IndexFileMetadata> files = new ConcurrentHashMap<>();

    private String keyName(IndexFile indexFile) {
        return indexFile.indexName() + "/" + indexFile.name();
    }

    private String keyName(String indexName, String fileName) {
        return indexName + "/" + fileName;
    }

    @Override
    public synchronized int commitFile(IndexFile file) {
        String key = keyName(file);
        IndexFileMetadata existing = files.get(key);
        long epoch = existing == null ? 1 : existing.getEpoch() + 1;
        files.put(key, new IndexFileMetadata(
                file.indexName(),
                file.name(),
                file.dataDirectory(),
                file.objectKey(),
                epoch,
                file.size(),
                file.checksum(),
                file.modifiedTime(),
                IndexFileStatus.DIRTY
        ));
        return Math.toIntExact(epoch);
    }

    @Override
    public synchronized void updateFileStatus(String indexName, String fileName, long epoch, IndexFileStatus status) {
        IndexFileMetadata metadata = fileMetadata(indexName, fileName);
        if (metadata == null || metadata.getEpoch() != epoch) {
            return;
        }
        if (!IndexFileStatus.validTransition(metadata.getStatus(), status)) {
            log.error("Invalid status transition from {} to {}", metadata.getStatus(), status);
            return;
        }
        metadata.setStatus(status);
    }

    @Override
    public synchronized List<IndexFileMetadata> listAll(String indexName, List<IndexFileStatus> status) {
        String prefix = indexName + "/";
        return files.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(java.util.Map.Entry::getValue)
                .filter(metadata -> status.contains(metadata.getStatus()))
                .toList();
    }

    @Override
    public synchronized IndexFileMetadata fileMetadata(String indexName, String name) {
        return files.get(keyName(indexName, name));
    }

    @Override
    public synchronized void deleteAll(String indexName) {
        String prefix = indexName + "/";
        files.keySet().removeIf(key -> key.startsWith(prefix));
    }
}
