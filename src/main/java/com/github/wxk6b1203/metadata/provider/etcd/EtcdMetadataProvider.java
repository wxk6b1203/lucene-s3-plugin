package com.github.wxk6b1203.metadata.provider.etcd;

import com.github.wxk6b1203.errors.StorageException;
import com.github.wxk6b1203.metadata.common.IndexMetadata;
import com.github.wxk6b1203.metadata.provider.MetadataProvider;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import lombok.Builder;
import lombok.Data;

import java.nio.charset.StandardCharsets;

public class EtcdMetadataProvider extends MetadataProvider {
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

    public EtcdMetadataProvider(Options opt, Client client) {
        this.namespace = opt.namespace;
        this.client = client;
    }

    public EtcdMetadataProvider(Options opt) {
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
        ByteSequence key = ByteSequence.from((SLASH + namespace + SLASH + Key.INDEX + SLASH + indexName)
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
        ByteSequence key = ByteSequence.from((SLASH + namespace + SLASH + Key.INDEX + SLASH + indexMetadata.getName())
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
}
