package com.github.wxk6b1203.metadata.provider.mem;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.IndexMetadata;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MemMockProvider extends ManifestMetadataManager {
    ConcurrentHashMap<String, IndexMetadata> indexes = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, List<IndexFileMetadata>> files = new ConcurrentHashMap<>();

    @Override
    public IndexMetadata get(String indexName) {
        return indexes.get(indexName);
    }

    @Override
    public synchronized long store(IndexMetadata indexMetadata) {
        IndexMetadata origin = indexes.get(indexMetadata.getName());
        if (origin != null) {
            indexMetadata.setEpoch(origin.getEpoch() + 1);
            indexes.put(indexMetadata.getName(), indexMetadata);
        } else {
            indexMetadata.setEpoch(1L);
            indexes.put(indexMetadata.getName(), indexMetadata);
        }
        return indexMetadata.getEpoch();
    }

    private String keyName(IndexFile indexFile) {
        return indexFile.indexName() + "/" + indexFile.name();
    }

    private String keyName(String indexName, String fileName) {
        return indexName + "/" + fileName;
    }

    @Override
    public synchronized int commitFile(IndexFile file) {
        String key = keyName(file);
        if (!files.containsKey(key)) {
            files.put(key, new ArrayList<>());
        }
        List<IndexFileMetadata> metadata = files.get(key);
        metadata.add(new IndexFileMetadata(
                file.indexName(),
                file.name(),
                file.dataDirectory(),
                file.objectKey(),
                metadata.size() + 1,
                file.size(),
                file.checksum(),
                file.modifiedTime(),
                IndexFileStatus.DIRTY
        ));
        return metadata.size();
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
    public List<IndexFileMetadata> listAll(List<IndexFileStatus> status) {
        return files.values().stream()
                .filter(metadata -> !metadata.isEmpty())
                .map(List::getLast)
                .filter(e -> status.contains(e.getStatus()))
                .toList();
    }

    @Override
    public synchronized IndexFileMetadata fileMetadata(String indexName, String name) {
        String key = keyName(indexName, name);
        List<IndexFileMetadata> metadata = files.get(key);
        if (metadata == null || metadata.isEmpty()) {
            return null;
        } else {
            return metadata.getLast();
        }
    }

    @Override
    public synchronized void deleteAll(String indexName) {
        String prefix = indexName + "/";
        files.keySet().removeIf(key -> key.startsWith(prefix));
    }
}
