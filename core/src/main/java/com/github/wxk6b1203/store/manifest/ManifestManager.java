package com.github.wxk6b1203.store.manifest;

import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.store.common.PathUtil;
import com.github.wxk6b1203.store.directory.S3DirectoryOptions;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

public class ManifestManager {
    private final Path basePath;
    private final String bucket;
    private final S3Client s3Client;
    private final ManifestMetadataManager metadataManager;

    public ManifestManager(S3DirectoryOptions options, S3Client s3Client, ManifestMetadataManager metadataManager) {
        this.basePath = options.basePath();
        this.bucket = options.bucket();
        this.s3Client = s3Client;
        this.metadataManager = metadataManager;
    }

    public List<IndexFileMetadata> listAll(List<IndexFileStatus> statuses) {
        return metadataManager.listAll(statuses);
    }

    public void deleteFile(String indexName, String name) throws IOException {
        metadataManager.prepareDelete(indexName, name);
        Path path = PathUtil.walDataPath(basePath, indexName);
        try {
            Files.delete(path.resolve(name));
        } catch (NoSuchFileException ignored) {}

        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(PathUtil.s3ObjectKey(indexName, name))
                .build();

        s3Client.deleteObject(deleteObjectRequest);
        // TODO: 1. mark as deleted in metadataManager
        //       2. delete from local storage
        //       3. delete from remote storage
    }

    public IndexFileMetadata fileMetadata(String indexName, String name) throws NoSuchFileException {
        IndexFileMetadata fileMetadata = metadataManager.fileMetadata(indexName, name);
        if (fileMetadata == null) {
            throw new NoSuchFileException(name);
        }
        return fileMetadata;
    }

    public void commit(String indexName, Collection<String> names) {
    }
}
