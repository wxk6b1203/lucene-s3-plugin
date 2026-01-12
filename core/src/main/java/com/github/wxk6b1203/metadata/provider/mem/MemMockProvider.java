package com.github.wxk6b1203.metadata.provider.mem;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.IndexMetadata;
import com.github.wxk6b1203.metadata.provider.ManifestManager;
import lombok.Synchronized;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MemMockProvider extends ManifestManager {
    ConcurrentHashMap<String, IndexMetadata> indexes = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, IndexFileMetadata> files = new ConcurrentHashMap<>();
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

    @Override
    public synchronized int createFile(IndexFile file) {
        files.put(file.name(), new IndexFileMetadata(
                "",
                file.name(),
                file.size(),
                file.checksum(),
                System.currentTimeMillis(),
                IndexFileStatus.WRITING
        ));
        return 0;
    }

    @Synchronized
    public void startRemoveFile(IndexFile file) {

    }

    @Override
    public List<IndexFileMetadata> listAllClean() {
        return files.values().stream().filter(e -> IndexFileStatus.CLEAN.equals(e.status())).toList();
    }

    @Override
    public List<IndexFileMetadata> listAll(List<IndexFileStatus> status) {
        return files.values().stream().filter(e -> status.contains(e.status())).toList();
    }
}
