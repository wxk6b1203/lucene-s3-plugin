package com.github.wxk6b1203.metadata.provider.mem;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.IndexMetadata;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MemMockProvider extends ManifestMetadataManager {
    ConcurrentHashMap<String, IndexMetadata> indexes = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, List<IndexFileMetadata>> files = new ConcurrentHashMap<>();

    @Override
    public synchronized IndexFileMetadata prepareDelete(String indexName, String name) throws IOException {
        String key = keyName(indexName, name);
        List<IndexFileMetadata> metadata = files.get(key);
        if (metadata != null && !metadata.isEmpty()) {
            IndexFileMetadata last = metadata.getLast();
            IndexFileMetadata deleteMetadata = new IndexFileMetadata(
                    last.indexName(),
                    last.name(),
                    last.epoch() + 1,
                    last.size(),
                    last.checksum(),
                    System.currentTimeMillis(),
                    IndexFileStatus.DELETING
            );
            metadata.add(deleteMetadata);
            return deleteMetadata;
        }
        throw new FileNotFoundException("File not found: " + name);
    }

    @Override
    public synchronized void cleaningUp(String indexName, String name) {
        String key = keyName(indexName, name);
        List<IndexFileMetadata> metadata = files.get(key);
        if (metadata != null && !metadata.isEmpty()) {
            IndexFileMetadata last = metadata.getLast();
            if (!IndexFileStatus.validTransition(last.status(), IndexFileStatus.CLEANING)) {
                log.error("Invalid status transition from {} to {}", last.status(), IndexFileStatus.CLEANING);
                return;
            }
        }
    }

    @Override
    public void finishDelete(String indexName, String name) {

    }

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
