package com.github.wxk6b1203.metadata.provider.mem;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.IndexMetadata;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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
                metadata.size() + 1,
                file.size(),
                file.checksum(),
                System.currentTimeMillis(),
                IndexFileStatus.CLEAN
        ));
        return metadata.size();
    }

    @Override
    public IndexFileMetadata get(String indexName, String fileName) {
        String key = keyName(indexName, fileName);
        List<IndexFileMetadata> metadata = files.get(key);
        if (metadata == null || metadata.isEmpty()) {
            return null;
        } else {
            return metadata.getLast();
        }
    }

    public synchronized void startRemoveFile(IndexFile file) {

    }

    @Override
    public synchronized List<IndexFileMetadata> listAllClean() {
        return files.values().stream()
                .flatMap(List::stream)
                .filter(e -> e.status() == IndexFileStatus.CLEAN || e.status() == IndexFileStatus.DIRTY)
                .toList();
    }

    @Override
    public List<IndexFileMetadata> listAll(List<IndexFileStatus> status) {
        return files.values().stream()
                .flatMap(List::stream)
                .filter(e -> status.contains(e.status()))
                .toList();
    }

    @Override
    public IndexFileMetadata fileMetadata(String indexName, String name) {
        return null;
    }
}
