package com.github.wxk6b1203.metadata.provider;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.etcd.EtcdManifestMetadataManager;
import io.etcd.jetcd.Client;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class EtcdManifestMetadataManagerTest {
    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void storesFileManifestMetadata() {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        String namespace = "test-manifest/" + UUID.randomUUID();
        EtcdManifestMetadataManager provider = new EtcdManifestMetadataManager(
                EtcdManifestMetadataManager.Options.builder()
                        .namespace(namespace)
                        .build(),
                client
        );
        try {
            int epoch = provider.commitFile(new IndexFile("books__shard_0", "segments_1", 128, 7));
            provider.updateFileStatus("books__shard_0", "segments_1", epoch, IndexFileStatus.UPLOADING);
            provider.updateFileStatus("books__shard_0", "segments_1", epoch, IndexFileStatus.CLEAN);

            var metadata = provider.fileMetadata("books__shard_0", "segments_1");
            assertNotNull(metadata);
            assertEquals(IndexFileStatus.CLEAN, metadata.getStatus());
            assertEquals(1, provider.listAll("books__shard_0", List.of(IndexFileStatus.CLEAN)).size());

            provider.deleteAll("books__shard_0");
            assertEquals(0, provider.listAll("books__shard_0", List.of(IndexFileStatus.CLEAN)).size());
        } finally {
            client.close();
        }
    }
}
