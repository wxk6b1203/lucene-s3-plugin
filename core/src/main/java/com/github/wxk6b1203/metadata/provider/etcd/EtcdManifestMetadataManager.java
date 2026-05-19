package com.github.wxk6b1203.metadata.provider.etcd;

import com.github.wxk6b1203.errors.StorageException;
import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.IndexMetadata;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.processor.metadata.annotation.Provider;
import com.github.wxk6b1203.util.JsonUtil;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.options.GetOption;
import lombok.Builder;
import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Provider(value = "etcd")
public class EtcdManifestMetadataManager extends ManifestMetadataManager {
    public static final String SLASH = "/";

    private final Client client;
    private final String namespace;

    @Data
    @Builder
    public static class Options {
        private String endpoints;
        private String username;
        private String password;
        @Builder.Default
        private String namespace = "metadata/";
    }

    // TODO: support different serialization formats
    public enum SerializationFormat {
        JSON,
        PROTOBUF
    }

    public EtcdManifestMetadataManager(Options opt, Client client) {
        this.namespace = opt.namespace;
        this.client = client;
    }

    public EtcdManifestMetadataManager(Options opt) {
        this.namespace = opt.namespace;
        var cb = Client.builder().endpoints(opt.endpoints);
        if (opt.username != null && !opt.username.isEmpty()) {
            cb.user(ByteSequence.from(opt.username, StandardCharsets.UTF_8));
        }
        if (opt.password != null && !opt.password.isEmpty()) {
            cb.password(ByteSequence.from(opt.password, StandardCharsets.UTF_8));
        }
        this.client = cb.build();
    }

    @Override
    public IndexMetadata get(String indexName) {
        ByteSequence key = ByteSequence.from((root() + SLASH + Key.INDEX + SLASH + indexName)
                .getBytes(StandardCharsets.UTF_8));
        try {
            var kv = client.getKVClient();
            var getResp = kv.get(key).get();
            if (getResp.getKvs().isEmpty()) {
                return null;
            }
            var kvs = getResp.getKvs().getFirst();
            var value = kvs.getValue().toString(StandardCharsets.UTF_8);
            var result = IndexMetadata.json(value);
            result.setEpoch(kvs.getModRevision());
            return result;
        } catch (Exception e) {
            return null;
        }
    }

    // Store the index metadata, return the mod revision if success, otherwise return -1
    // format: etcd key: /metadata/index/{indexName}
    @Override
    public long store(IndexMetadata indexMetadata) {
        ByteSequence key = ByteSequence.from((root() + SLASH + Key.INDEX + SLASH + indexMetadata.getName())
                .getBytes(StandardCharsets.UTF_8));
        Long epoch = indexMetadata.getEpoch();
        try {
            indexMetadata.setEpoch(null);
            var kv = client.getKVClient();
            var ret = kv.put(key, ByteSequence.from(indexMetadata.json())).get();
            return ret.getHeader().getRevision();
        } catch (Exception e) {
            StorageException ex = new StorageException("Failed to store index metadata: " + e.getMessage());
            ex.initCause(e);
            throw ex;
        } finally {
            indexMetadata.setEpoch(epoch);
        }
    }

    @Override
    public int commitFile(IndexFile file) {
        IndexFileMetadata existing = fileMetadata(file.indexName(), file.name());
        long epoch = existing == null ? 1 : existing.getEpoch() + 1;
        IndexFileMetadata metadata = new IndexFileMetadata(
                file.indexName(),
                file.name(),
                file.dataDirectory(),
                file.objectKey(),
                epoch,
                file.size(),
                file.checksum(),
                file.modifiedTime(),
                IndexFileStatus.DIRTY
        );
        putFileMetadata(metadata);
        return Math.toIntExact(epoch);
    }

    @Override
    public void updateFileStatus(String indexName, String fileName, long epoch, IndexFileStatus status) {
        IndexFileMetadata metadata = fileMetadata(indexName, fileName);
        if (metadata == null || metadata.getEpoch() != epoch) {
            return;
        }
        if (!IndexFileStatus.validTransition(metadata.getStatus(), status)) {
            return;
        }
        metadata.setStatus(status);
        putFileMetadata(metadata);
    }

    @Override
    public List<IndexFileMetadata> listAll(List<IndexFileStatus> status) {
        try {
            var kv = client.getKVClient();
            var resp = kv.get(filePrefix(), GetOption.builder().isPrefix(true).build()).get();
            List<IndexFileMetadata> result = new ArrayList<>();
            for (var item : resp.getKvs()) {
                IndexFileMetadata metadata = decodeFileMetadata(item.getValue());
                if (status.contains(metadata.getStatus())) {
                    result.add(metadata);
                }
            }
            return result;
        } catch (Exception e) {
            throw storageException("Failed to list file metadata: " + e.getMessage(), e);
        }
    }

    @Override
    public IndexFileMetadata fileMetadata(String indexName, String name) {
        try {
            var kv = client.getKVClient();
            var resp = kv.get(fileKey(indexName, name)).get();
            if (resp.getKvs().isEmpty()) {
                return null;
            }
            return decodeFileMetadata(resp.getKvs().getFirst().getValue());
        } catch (Exception e) {
            throw storageException("Failed to get file metadata: " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteAll(String indexName) {
        try {
            client.getKVClient()
                    .delete(filePrefix(indexName), io.etcd.jetcd.options.DeleteOption.builder().isPrefix(true).build())
                    .get();
        } catch (Exception e) {
            throw storageException("Failed to delete file metadata: " + e.getMessage(), e);
        }
    }

    private void putFileMetadata(IndexFileMetadata metadata) {
        try {
            client.getKVClient().put(
                    fileKey(metadata.getIndexName(), metadata.getName()),
                    ByteSequence.from(JsonUtil.writeValueAsBytes(FileRecord.from(metadata)))
            ).get();
        } catch (Exception e) {
            throw storageException("Failed to store file metadata: " + e.getMessage(), e);
        }
    }

    private IndexFileMetadata decodeFileMetadata(ByteSequence value) {
        FileRecord record = JsonUtil.readValue(value.getBytes(), FileRecord.class);
        return new IndexFileMetadata(
                record.indexName,
                record.name,
                record.dataDirectory,
                record.objectKey,
                record.epoch,
                record.size,
                record.checksum,
                record.modifiedTime,
                record.status
        );
    }

    private ByteSequence filePrefix() {
        return ByteSequence.from((root() + SLASH + "index_file" + SLASH).getBytes(StandardCharsets.UTF_8));
    }

    private ByteSequence filePrefix(String indexName) {
        return ByteSequence.from((root() + SLASH + "index_file" + SLASH + indexName + SLASH)
                .getBytes(StandardCharsets.UTF_8));
    }

    private ByteSequence fileKey(String indexName, String fileName) {
        return ByteSequence.from((root() + SLASH + "index_file" + SLASH + indexName + SLASH + fileName)
                .getBytes(StandardCharsets.UTF_8));
    }

    private String root() {
        String normalized = namespace;
        while (normalized.startsWith(SLASH)) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith(SLASH)) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return SLASH + normalized;
    }

    private StorageException storageException(String message, Exception cause) {
        StorageException ex = new StorageException(message);
        ex.initCause(cause);
        return ex;
    }

    public static class FileRecord {
        public String indexName;
        public String name;
        public String dataDirectory;
        public String objectKey;
        public long epoch;
        public long size;
        public long checksum;
        public long modifiedTime;
        public IndexFileStatus status;

        public static FileRecord from(IndexFileMetadata metadata) {
            FileRecord record = new FileRecord();
            record.indexName = metadata.getIndexName();
            record.name = metadata.getName();
            record.dataDirectory = metadata.getDataDirectory();
            record.objectKey = metadata.getObjectKey();
            record.epoch = metadata.getEpoch();
            record.size = metadata.getSize();
            record.checksum = metadata.getChecksum();
            record.modifiedTime = metadata.getModifiedTime();
            record.status = metadata.getStatus();
            return record;
        }
    }
}
