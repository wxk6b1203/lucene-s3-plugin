package com.github.wxk6b1203.store.object;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class S3RemoteObjectStore implements RemoteObjectStore {
    private static final AtomicLong PUTS = new AtomicLong();
    private static final AtomicLong GETS = new AtomicLong();
    private static final AtomicLong DELETES = new AtomicLong();
    private static final AtomicLong PUT_ERRORS = new AtomicLong();
    private static final AtomicLong GET_ERRORS = new AtomicLong();
    private static final AtomicLong DELETE_ERRORS = new AtomicLong();
    private static final AtomicLong PUT_DURATION_NANOS = new AtomicLong();
    private static final AtomicLong GET_DURATION_NANOS = new AtomicLong();
    private static final AtomicLong DELETE_DURATION_NANOS = new AtomicLong();

    private final String bucket;
    private final S3Client s3Client;

    public S3RemoteObjectStore(String bucket, S3Client s3Client) {
        this.bucket = bucket;
        this.s3Client = s3Client;
    }

    @Override
    public void put(String key, Path source) {
        long started = System.nanoTime();
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            s3Client.putObject(request, RequestBody.fromFile(source));
            PUTS.incrementAndGet();
        } catch (RuntimeException e) {
            PUT_ERRORS.incrementAndGet();
            throw e;
        } finally {
            PUT_DURATION_NANOS.addAndGet(System.nanoTime() - started);
        }
    }

    @Override
    public void get(String key, Path target) throws IOException {
        long started = System.nanoTime();
        try {
            Files.createDirectories(target.getParent());
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            s3Client.getObject(request, target);
            GETS.incrementAndGet();
        } catch (IOException | RuntimeException e) {
            GET_ERRORS.incrementAndGet();
            throw e;
        } finally {
            GET_DURATION_NANOS.addAndGet(System.nanoTime() - started);
        }
    }

    @Override
    public void delete(Collection<String> keys) throws IOException {
        if (keys.isEmpty()) {
            return;
        }
        long started = System.nanoTime();
        try {
            DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                    .bucket(bucket)
                    .delete(delete -> delete.objects(keys.stream()
                            .map(key -> ObjectIdentifier.builder().key(key).build())
                            .toList()))
                    .build();
            DeleteObjectsResponse response = s3Client.deleteObjects(request);
            if (response.hasErrors()) {
                DELETE_ERRORS.incrementAndGet();
                String errors = response.errors().stream()
                        .limit(5)
                        .map(error -> error.key() + "=" + error.code() + ":" + error.message())
                        .collect(Collectors.joining(", "));
                throw new IOException("Failed to delete S3 objects from bucket " + bucket + ": " + errors);
            }
            DELETES.addAndGet(keys.size());
        } catch (IOException | RuntimeException e) {
            if (e instanceof IOException) {
                throw e;
            }
            DELETE_ERRORS.incrementAndGet();
            throw e;
        } finally {
            DELETE_DURATION_NANOS.addAndGet(System.nanoTime() - started);
        }
    }

    public static Map<String, Object> statsSnapshot() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("puts", PUTS.get());
        stats.put("gets", GETS.get());
        stats.put("deletes", DELETES.get());
        stats.put("put_errors", PUT_ERRORS.get());
        stats.put("get_errors", GET_ERRORS.get());
        stats.put("delete_errors", DELETE_ERRORS.get());
        stats.put("put_duration_seconds_sum", PUT_DURATION_NANOS.get() / 1_000_000_000.0);
        stats.put("get_duration_seconds_sum", GET_DURATION_NANOS.get() / 1_000_000_000.0);
        stats.put("delete_duration_seconds_sum", DELETE_DURATION_NANOS.get() / 1_000_000_000.0);
        return stats;
    }
}
