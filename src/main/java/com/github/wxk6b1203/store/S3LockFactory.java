package com.github.wxk6b1203.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import java.io.IOException;

public class S3LockFactory extends LockFactory {
    @Override
    public Lock obtainLock(Directory dir, String lockName) throws IOException {
        return null;
    }
}
