package com.github.wxk6b1203.store;

import com.github.wxk6b1203.store.directory.S3Directory;
import com.github.wxk6b1203.store.directory.S3LockFactory;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointProvider;
import software.amazon.awssdk.services.s3.endpoints.internal.DefaultS3EndpointProvider;

import java.io.IOException;
import java.net.URI;

public class S3DirectoryTest {
    @Test
    public void testS3ListAll() throws IOException {
        S3Client client = S3Client.builder().endpointOverride(URI.create("https://oss-cn-shanghai.aliyuncs.com")).build();
        S3Directory s3Directory = new S3Directory("bee-all", new S3LockFactory(client), client);
        var allObjects = s3Directory.listAll();
        for (var obj : allObjects) {
            System.out.println(obj);
        }
    }
}
