package com.github.wxk6b1203.store.directory;

import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.store.common.PathUtil;
import com.github.wxk6b1203.store.manifest.ManifestManager;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Directory implementation that uses S3 as the underlying storage.
 * Notice: This directory will not deal with case when files are open and cannot be deleted or renamed on Windows.
 */
public class S3CachingDirectory extends BaseDirectory {
    private final Path basePath;
    private final String indexName;
    private final ManifestManager manifestManager;
    private final AtomicLong nextTempFileCounter = new AtomicLong();

    public S3CachingDirectory(
            S3DirectoryOptions options,
            S3LockFactory s3LockFactory,
            ManifestManager manifestManager
    ) throws IOException {
        super(s3LockFactory);
        this.basePath = options.basePath();
        this.indexName = options.indexName();
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
        manifestManager.deleteFile(indexName, name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        IndexFileMetadata metadata = manifestManager.fileMetadata(indexName, name);
        return metadata.size();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        Path file = PathUtil.walDataPath(basePath, indexName).resolve(name);
        return new FSIndexOutput(file);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        ensureOpen();
        String name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());
        Path file = PathUtil.walDataPath(basePath, indexName).resolve(name);
        return new FSIndexOutput(file);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        manifestManager.commit(indexName, names);
    }

    /**
     * @see IndexWriter#finishCommit()
     * @throws IOException
     */
    @Override
    public void syncMetaData() throws IOException {
        // TODO: sync local file status with remote manifest
        // Notice: rename / move and another file operations will be handle when committing segments.
        // this sync should ensure local metadata is consistent with remote manifest.
        // see also: org.apache.lucene.index.IndexWriter.finishCommit
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        ensureOpen();
        Path dataPath = PathUtil.walDataPath(basePath, indexName).resolve(source);
        Files.move(dataPath.resolve(source), dataPath.resolve(dest), StandardCopyOption.ATOMIC_MOVE);
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
        return Set.of();
    }
}
