package com.github.wxk6b1203.store.directory;

import com.github.wxk6b1203.common.Common;
import com.github.wxk6b1203.metadata.provider.MetadataProvider;
import org.apache.lucene.store.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class S3CachingDirectory extends BaseDirectory {
    private final S3Client s3Client;
    private final String bucket;
    private final String indexName;
    private final MetadataProvider metadataProvider;
    private static final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private final Set<String> pendingDeletes = ConcurrentHashMap.newKeySet();

    private Directory cachingDirectory;

    public S3CachingDirectory(
            S3DirectoryOptions options,
            S3LockFactory s3LockFactory,
            S3Client s3Client,
            MetadataProvider metadataProvider
    ) throws IOException {
        super(s3LockFactory);
        this.indexName = options.indexName();
        this.bucket = options.bucket();
        this.s3Client = s3Client;
        this.metadataProvider = metadataProvider;
        if (options.enableCaching()) {
            this.cachingDirectory = new MMapDirectory(Path.of(options.basePath().toAbsolutePath().toString(), indexName, Hierarchy.DATA.getPath()));
        } else {
            this.cachingDirectory = new ByteBuffersDirectory();
        }
    }

    private String location() {
        return indexName + Common.SLASH + Hierarchy.DATA.getPath() + Common.SLASH;
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        try {
            List<String> fileNames = new ArrayList<>();
            String prefix = location();
            var paginator = s3Client.listObjectsV2Paginator(b -> b
                    .bucket(bucket)
                    .prefix(prefix)
                    .delimiter(Common.SLASH)); // 仅当前层
            paginator.stream()
                    .flatMap(r -> r.contents().stream())
                    .map(S3Object::key)
                    .filter(k -> !k.equals(prefix))
                    .filter(k -> !k.endsWith("/"))
                    .map(k -> k.substring(prefix.length()))
                    .forEach((e) -> {
                        if (!pendingDeletes.contains(e)) {
                            fileNames.add(e);
                        }
                    });
            fileNames.sort(String::compareTo);
            return fileNames.toArray(new String[0]);
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
        String key = location() + name;
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
        String key = location() + name;
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
        isOpen = false;
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return pendingDeletes;
    }
}
