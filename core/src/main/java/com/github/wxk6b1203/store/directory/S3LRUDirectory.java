package com.github.wxk6b1203.store.directory;

import com.github.wxk6b1203.metadata.provider.MetadataProvider;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;

public class S3LRUDirectory extends BaseDirectory {
    private final MetadataProvider metadataProvider;
    private final S3Client s3Client;
    private final Path path;

    public S3LRUDirectory(
            Path path,
            long maxChunkSize,
            MetadataProvider metadataProvider,
            S3LockFactory s3LockFactory,
            S3Client s3Client) {
        super(s3LockFactory);
        this.metadataProvider = metadataProvider;
        this.s3Client = s3Client;
        this.path = path;
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
