package com.github.wxk6b1203.store.object;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Collectors;

public class S3RemoteObjectStore implements RemoteObjectStore {
    private final String bucket;
    private final S3Client s3Client;

    public S3RemoteObjectStore(String bucket, S3Client s3Client) {
        this.bucket = bucket;
        this.s3Client = s3Client;
    }

    @Override
    public void put(String key, Path source) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        s3Client.putObject(request, RequestBody.fromFile(source));
    }

    @Override
    public void get(String key, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        s3Client.getObject(request, target);
    }

    @Override
    public void delete(Collection<String> keys) throws IOException {
        if (keys.isEmpty()) {
            return;
        }
        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(delete -> delete.objects(keys.stream()
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .toList()))
                .build();
        DeleteObjectsResponse response = s3Client.deleteObjects(request);
        if (response.hasErrors()) {
            String errors = response.errors().stream()
                    .limit(5)
                    .map(error -> error.key() + "=" + error.code() + ":" + error.message())
                    .collect(Collectors.joining(", "));
            throw new IOException("Failed to delete S3 objects from bucket " + bucket + ": " + errors);
        }
    }
}
