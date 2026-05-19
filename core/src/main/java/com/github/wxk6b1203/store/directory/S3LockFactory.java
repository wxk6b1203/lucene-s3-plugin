package com.github.wxk6b1203.store.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;

import java.io.IOException;

public class S3LockFactory extends LockFactory {
    private final SingleInstanceLockFactory delegate = new SingleInstanceLockFactory();

    public S3LockFactory() {
    }

    @Override
    public Lock obtainLock(Directory dir, String lockName) throws IOException {
        return delegate.obtainLock(dir, lockName);
    }
}
