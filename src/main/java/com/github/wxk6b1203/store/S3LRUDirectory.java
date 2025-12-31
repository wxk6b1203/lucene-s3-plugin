package com.github.wxk6b1203.store;

import org.apache.lucene.store.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public class S3LRUDirectory extends BaseDirectory {
    /**
     * Sole constructor.
     *
     * @param lockFactory
     */
    protected S3LRUDirectory(LockFactory lockFactory) {
        super(lockFactory);
    }

    @Override
    public String[] listAll() throws IOException {
        return new String[0];
    }

    @Override
    public void deleteFile(String name) throws IOException {

    }

    @Override
    public long fileLength(String name) throws IOException {
        return 0;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return null;
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return null;
    }

    @Override
    public void sync(Collection<String> names) throws IOException {

    }

    @Override
    public void syncMetaData() throws IOException {

    }

    @Override
    public void rename(String source, String dest) throws IOException {

    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return Set.of();
    }
}
