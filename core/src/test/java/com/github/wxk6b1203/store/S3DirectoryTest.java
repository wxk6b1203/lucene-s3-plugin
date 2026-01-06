package com.github.wxk6b1203.store;

import com.github.wxk6b1203.common.Common;
import com.github.wxk6b1203.store.directory.Hierarchy;
import com.github.wxk6b1203.store.directory.S3Directory;
import com.github.wxk6b1203.store.directory.S3LockFactory;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointProvider;
import software.amazon.awssdk.services.s3.endpoints.internal.DefaultS3EndpointProvider;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.NoSuchFileException;

public class S3DirectoryTest {
    @Test
    public void testS3ListAll() throws IOException {
        S3Client client = S3Client.builder().endpointOverride(URI.create("https://oss-cn-shanghai.aliyuncs.com")).build();
        String bucket = System.getenv("S3_BUCKET");
        String testIndexName = System.getenv("TEST_INDEX_NAME");
        // need to add:
        // AWS_ACCESS_KEY_ID
        // AWS_SECRET_ACCESS_KEY
        // AWS_REGION
        S3Directory s3Directory = new S3Directory(testIndexName, bucket, new S3LockFactory(client), client);
        var allObjects = s3Directory.listAll();
        for (var obj : allObjects) {
            System.out.println(obj);
        }
    }

    @Test
    public void testS3DeleteFile() throws IOException {
        S3Client client = S3Client.builder().endpointOverride(URI.create("https://oss-cn-shanghai.aliyuncs.com")).serviceConfiguration(S3Configuration.builder().chunkedEncodingEnabled(false).build()).build();
        String bucket = System.getenv("S3_BUCKET");
        String testIndexName = System.getenv("TEST_INDEX_NAME");
        // need to add:
        // AWS_ACCESS_KEY_ID
        // AWS_SECRET_ACCESS_KEY
        // AWS_REGION
        S3Directory s3Directory = new S3Directory(testIndexName, bucket, new S3LockFactory(client), client);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucket).key(  testIndexName + Common.SLASH + Hierarchy.DATA.path + Common.SLASH + "test_file").build();
        RequestBody body = RequestBody.fromString("value");
        var put = client.putObject(putObjectRequest, body);
        s3Directory.deleteFile("test_file");

        try {
            s3Directory.deleteFile("test_file");
        } catch (NoSuchFileException ignored) {

        }
    }
}
