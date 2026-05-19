package com.github.wxk6b1203.metadata.provider.etcd;

import com.github.wxk6b1203.errors.StorageException;
import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.util.JsonUtil;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import lombok.Builder;
import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class EtcdManifestMetadataManager extends ManifestMetadataManager {
    private static final int MAX_CAS_RETRIES = 32;

    private final Client client;
    private final String namespace;

    @Data
    @Builder
    public static class Options {
        @Builder.Default
        private String namespace = "lucene-s3/cluster/manifest";
    }

    public EtcdManifestMetadataManager(Options options, Client client) {
        this.client = Objects.requireNonNull(client, "client");
        this.namespace = normalize(options == null ? null : options.namespace);
    }

    @Override
    public int commitFile(IndexFile file) {
        ByteSequence key = fileKey(file.indexName(), file.name());
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            try {
                KeyValue currentKv = fileKv(key);
                IndexFileMetadata current = currentKv == null ? null : decodeFileMetadata(currentKv);
                long epoch = current == null ? 1 : current.getEpoch() + 1;
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
                if (putIfCurrent(key, currentKv, metadata)) {
                    return Math.toIntExact(epoch);
                }
            } catch (Exception e) {
                throw storageException("Failed to commit file metadata: " + file.indexName() + "/" + file.name(), e);
            }
        }
        throw new StorageException("Failed to commit file metadata after CAS retries: "
                + file.indexName() + "/" + file.name());
    }

    @Override
    public void updateFileStatus(String indexName, String fileName, long epoch, IndexFileStatus status) {
        ByteSequence key = fileKey(indexName, fileName);
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            try {
                KeyValue currentKv = fileKv(key);
                if (currentKv == null) {
                    return;
                }
                IndexFileMetadata metadata = decodeFileMetadata(currentKv);
                if (metadata.getEpoch() != epoch || !IndexFileStatus.validTransition(metadata.getStatus(), status)) {
                    return;
                }
                metadata.setStatus(status);
                if (putIfCurrent(key, currentKv, metadata)) {
                    return;
                }
            } catch (Exception e) {
                throw storageException("Failed to update file metadata status: " + indexName + "/" + fileName, e);
            }
        }
        throw new StorageException("Failed to update file metadata status after CAS retries: "
                + indexName + "/" + fileName);
    }

    @Override
    public List<IndexFileMetadata> listAll(String indexName, List<IndexFileStatus> status) {
        try {
            var response = client.getKVClient()
                    .get(filePrefix(indexName), GetOption.builder().isPrefix(true).build())
                    .get();
            List<IndexFileMetadata> result = new ArrayList<>();
            for (KeyValue item : response.getKvs()) {
                IndexFileMetadata metadata = decodeFileMetadata(item);
                if (status.contains(metadata.getStatus())) {
                    result.add(metadata);
                }
            }
            return result;
        } catch (Exception e) {
            throw storageException("Failed to list file metadata: " + indexName, e);
        }
    }

    @Override
    public IndexFileMetadata fileMetadata(String indexName, String name) {
        try {
            KeyValue kv = fileKv(fileKey(indexName, name));
            return kv == null ? null : decodeFileMetadata(kv);
        } catch (Exception e) {
            throw storageException("Failed to get file metadata: " + indexName + "/" + name, e);
        }
    }

    @Override
    public void deleteAll(String indexName) {
        try {
            client.getKVClient()
                    .delete(filePrefix(indexName), DeleteOption.builder().isPrefix(true).build())
                    .get();
        } catch (Exception e) {
            throw storageException("Failed to delete file metadata: " + indexName, e);
        }
    }

    private KeyValue fileKv(ByteSequence key) throws Exception {
        var response = client.getKVClient().get(key).get();
        return response.getKvs().isEmpty() ? null : response.getKvs().getFirst();
    }

    private boolean putIfCurrent(ByteSequence key, KeyValue currentKv, IndexFileMetadata metadata) throws Exception {
        Cmp cmp = currentKv == null
                ? new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0))
                : new Cmp(key, Cmp.Op.EQUAL, CmpTarget.modRevision(currentKv.getModRevision()));
        return client.getKVClient()
                .txn()
                .If(cmp)
                .Then(Op.put(key, ByteSequence.from(JsonUtil.writeValueAsBytes(metadata)), PutOption.DEFAULT))
                .commit()
                .get()
                .isSucceeded();
    }

    private IndexFileMetadata decodeFileMetadata(KeyValue kv) {
        return JsonUtil.readValue(kv.getValue().getBytes(), IndexFileMetadata.class);
    }

    private ByteSequence filePrefix(String indexName) {
        return key("index_file/" + indexName + "/");
    }

    private ByteSequence fileKey(String indexName, String fileName) {
        return key("index_file/" + indexName + "/" + fileName);
    }

    private ByteSequence key(String suffix) {
        return ByteSequence.from((namespace + "/" + suffix).getBytes(StandardCharsets.UTF_8));
    }

    private String normalize(String value) {
        String normalized = value == null || value.isBlank() ? "lucene-s3/cluster/manifest" : value;
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return "/" + normalized;
    }

    private StorageException storageException(String message, Exception cause) {
        StorageException exception = new StorageException(message + ": " + cause.getMessage());
        exception.initCause(cause);
        return exception;
    }
}
