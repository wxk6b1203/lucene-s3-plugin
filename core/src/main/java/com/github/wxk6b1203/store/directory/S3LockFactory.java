package com.github.wxk6b1203.store.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;

public class S3LockFactory extends LockFactory {
    private final S3Client s3Client;
    public S3LockFactory(S3Client s3Client) {
        this.s3Client = s3Client;
    }
    @Override
    public Lock obtainLock(Directory dir, String lockName) throws IOException {
        return null;
    }
}
