package com.github.wxk6b1203.metadata.provider;

import com.github.wxk6b1203.metadata.common.IndexMetadata;
import com.github.wxk6b1203.metadata.provider.etcd.EtcdManifestManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetadataTest {
    @Test
    public void test() {
        EtcdManifestManager.Options opt =
                EtcdManifestManager.Options.builder()
                        .namespace("test-metadata/")
                        .endpoints("http://10.0.10.42:2379")
                        .build();
        EtcdManifestManager provider = new EtcdManifestManager(opt);
        var metadata = provider.get("test-index");
        provider.store(new IndexMetadata().setName("test-index"));
        var metadata2 = provider.get("test-index");
        assertEquals(metadata == null ? 0 : metadata.getEpoch() + 1, metadata2.getEpoch());
    }
}
