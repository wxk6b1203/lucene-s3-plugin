package com.github.wxk6b1203.metadata.provider;

import com.github.wxk6b1203.metadata.common.IndexMetadata;
import com.github.wxk6b1203.metadata.provider.etcd.EtcdMetadataProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetadataTest {
    @Test
    public void test() {
        EtcdMetadataProvider.Options opt =
                EtcdMetadataProvider.Options.builder()
                        .namespace("test-metadata/")
                        .endpoints("http://10.0.10.42:2379")
                        .build();
        EtcdMetadataProvider provider = new EtcdMetadataProvider(opt);
        var metadata = provider.get("test-index");
        provider.store(new IndexMetadata().setName("test-index"));
        var metadata2 = provider.get("test-index");
        assertEquals(metadata == null ? 0 : metadata.getEpoch() + 1, metadata2.getEpoch());
    }
}
