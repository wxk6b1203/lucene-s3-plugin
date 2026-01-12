package com.github.wxk6b1203.store.directory;

import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestManager;
import org.apache.lucene.store.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class S3Directory extends BaseDirectory {
    private final S3Client s3Client;
    private final Path basePath;
    private final String bucket;
    private final String indexName;
    private final ManifestManager manifestManager;
    private final AtomicLong nextTempFileCounter = new AtomicLong();
    private static final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private final Set<String> pendingDeletes = ConcurrentHashMap.newKeySet();
    public S3Directory(
            S3DirectoryOptions options,
            S3LockFactory s3LockFactory,
            S3Client s3Client,
            ManifestManager manifestManager
    ) throws IOException {
        super(s3LockFactory);
        this.basePath = options.basePath();
        this.indexName = options.indexName();
        this.bucket = options.bucket();
        this.s3Client = s3Client;
        this.manifestManager = manifestManager;
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        try {
            return manifestManager.listAll(List.of(IndexFileStatus.CLEAN, IndexFileStatus.DIRTY))
                    .stream()
                    .map(IndexFileMetadata::name)
                    .toArray(String[]::new);
        } catch (Exception e) {
            throw new IOException("Failed to list objects from S3", e);
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        if (pendingDeletes.contains(name)) {
            throw new NoSuchFileException("file \"" + name + "\" is already pending delete");
        }
        String key = dataPath().resolve(name).toString();
        try {
            var obj = s3Client.getObject(b -> b
                    .bucket(bucket).key(key));
            if (obj != null) {
                pendingDeletes.add(name);
                executor.execute(() -> {
                    s3Client.deleteObject(b -> b
                            .bucket(bucket)
                            .key(key));
                    pendingDeletes.remove(name);
                });
            } else {
                throw new NoSuchFileException(name);
            }
        } catch (NoSuchKeyException e) {
            NoSuchFileException ex = new NoSuchFileException(name);
            ex.initCause(e);
            throw ex;
        } catch (Exception e) {
            throw new IOException("Failed to delete object from S3: " + name, e);
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        String key = dataPath().resolve(name).toString();
        try {
            var obj = s3Client.headObject(b -> b
                    .bucket(bucket).key(key));
            return obj.contentLength();
        } catch (NoSuchKeyException e) {
            NoSuchFileException ex = new NoSuchFileException(name);
            ex.initCause(e);
            throw ex;
        } catch (Exception e) {
            throw new IOException("Failed to get object length from S3: " + name, e);
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        return new S3IndexOutput("S3IndexOutput(%s/%s)", bucket, indexName, name, s3Client);
    }

    private Path dataPath() {
        // {basePath}/{indexName}/_data/
        return Path.of(basePath.toString(),
                indexName,
                Hierarchy.DATA.getPath());
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        ensureOpen();
        while (true) {
            String name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());
            if (pendingDeletes.contains(name)) {
                continue;
            }
            return new FSIndexOutput(dataPath().resolve(name));
        }
    }

    @Override
    public void sync(Collection<String> names) throws IOException {

    }

    @Override
    public void syncMetaData() throws IOException {

    }

    @Override
    public void rename(String source, String dest) throws IOException {
        ensureOpen();
        if (pendingDeletes.contains(source)) {
            throw new NoSuchFileException(
                    "file \"" + source + "\" is pending delete and cannot be moved");
        }
        if (pendingDeletes.remove(dest)) {

        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return pendingDeletes;
    }
}
