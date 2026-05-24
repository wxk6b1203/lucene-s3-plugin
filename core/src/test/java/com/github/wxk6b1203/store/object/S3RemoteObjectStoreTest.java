package com.github.wxk6b1203.store.object;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class S3RemoteObjectStoreTest {
    @Test
    void deleteSplitsMultiObjectDeleteIntoS3SizedBatches() throws Exception {
        List<Integer> batchSizes = new ArrayList<>();
        S3Client s3Client = s3Client(batchSizes);
        S3RemoteObjectStore store = new S3RemoteObjectStore("bucket", s3Client);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 2_501; i++) {
            keys.add("index/_data/file-" + i);
        }

        store.delete(keys);

        assertEquals(List.of(1_000, 1_000, 501), batchSizes);
    }

    private S3Client s3Client(List<Integer> batchSizes) {
        InvocationHandler handler = (Object proxy, Method method, Object[] args) -> {
            if (method.getName().equals("deleteObjects")) {
                DeleteObjectsRequest request = (DeleteObjectsRequest) args[0];
                batchSizes.add(request.delete().objects().size());
                return DeleteObjectsResponse.builder().build();
            }
            if (method.getName().equals("close")) {
                return null;
            }
            if (method.getDeclaringClass() == Object.class) {
                return method.invoke(this, args);
            }
            throw new UnsupportedOperationException("unexpected S3Client method: " + method.getName());
        };
        return (S3Client) Proxy.newProxyInstance(
                S3Client.class.getClassLoader(),
                new Class<?>[]{S3Client.class},
                handler
        );
    }
}
