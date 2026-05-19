package com.github.wxk6b1203.store.directory;

import com.github.wxk6b1203.metadata.common.CommitingIndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.store.common.PathUtil;
import com.github.wxk6b1203.store.manifest.ManifestManager;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NoLockFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Directory implementation that uses S3 as the underlying storage.
 */
public class S3CachingDirectory extends BaseDirectory {
    private final Path basePath;
    private final String indexName;
    private final ManifestManager manifestManager;
    private final AtomicLong nextTempFileCounter = new AtomicLong();
    private final Path walDataPath;
    private final Path sharedDataPath;
    private final Path sharedTempPath;
    private final NIOFSDirectory walDirectory;
    private final NIOFSDirectory sharedDirectory;
    private final List<IndexFileStatus> readableRemoteStatuses;
    private final boolean includeWalFiles;
    private final Set<String> syncedFiles = new ConcurrentSkipListSet<>();
    private final Set<String> pendingLocalDeletes = new ConcurrentSkipListSet<>();
    private final Map<String, Object> cacheLocks = new ConcurrentHashMap<>();

    public S3CachingDirectory(
            S3DirectoryOptions options,
            LockFactory lockFactory,
            ManifestManager manifestManager
    ) throws IOException {
        this(options, lockFactory, manifestManager, List.of(
                IndexFileStatus.DIRTY,
                IndexFileStatus.UPLOADING,
                IndexFileStatus.CLEAN,
                IndexFileStatus.PINNED
        ), true);
    }

    public S3CachingDirectory(
            S3DirectoryOptions options,
            LockFactory lockFactory,
            ManifestManager manifestManager,
            List<IndexFileStatus> readableRemoteStatuses
    ) throws IOException {
        this(options, lockFactory, manifestManager, readableRemoteStatuses, true);
    }

    public S3CachingDirectory(
            S3DirectoryOptions options,
            LockFactory lockFactory,
            ManifestManager manifestManager,
            List<IndexFileStatus> readableRemoteStatuses,
            boolean includeWalFiles
    ) throws IOException {
        super(lockFactory);
        this.basePath = options.basePath();
        this.indexName = options.indexName();
        this.manifestManager = manifestManager;
        this.readableRemoteStatuses = List.copyOf(readableRemoteStatuses);
        this.includeWalFiles = includeWalFiles;
        this.walDataPath = PathUtil.walDataPath(basePath, indexName);
        this.sharedDataPath = PathUtil.sharedDataPath(basePath, indexName);
        this.sharedTempPath = PathUtil.sharedTempPath(basePath, indexName);
        Files.createDirectories(walDataPath);
        Files.createDirectories(sharedDataPath);
        Files.createDirectories(sharedTempPath);
        this.walDirectory = new NIOFSDirectory(walDataPath, NoLockFactory.INSTANCE);
        this.sharedDirectory = new NIOFSDirectory(sharedDataPath, NoLockFactory.INSTANCE);
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        Set<String> names = new HashSet<>();
        if (includeWalFiles) {
            names.addAll(List.of(walDirectory.listAll()));
        }
        try {
            manifestManager.listAll(indexName, readableRemoteStatuses)
                    .stream()
                    .map(IndexFileMetadata::getName)
                    .forEach(names::add);
            return names.toArray(String[]::new);
        } catch (Exception e) {
            throw new IOException("Failed to list objects from S3", e);
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        syncedFiles.remove(name);
        deleteLocalIfExists(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        if (walFileAvailable(name)) {
            return Files.size(walDataPath.resolve(name));
        }
        Path sharedFile = sharedDataPath.resolve(name);
        if (sharedCacheAvailable(name)) {
            return Files.size(sharedFile);
        }
        IndexFileMetadata metadata = readableRemoteFileMetadata(name);
        return metadata.getSize();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        return walDirectory.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        ensureOpen();
        String name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());
        return walDirectory.createOutput(name, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        ensureOpen();
        List<String> localNames = new ArrayList<>();
        for (String name : names) {
            Path filePath = walDataPath.resolve(name);
            if (Files.exists(filePath)) {
                syncedFiles.add(name);
                localNames.add(name);
            }
        }
        walDirectory.sync(localNames);
    }

    /**
     * @see IndexWriter#finishCommit()
     * @throws IOException
     */
    @Override
    public void syncMetaData() throws IOException {
        ensureOpen();
        walDirectory.syncMetaData();
        if (syncedFiles.stream().noneMatch(this::isCommittedSegmentFile)) {
            return;
        }
        Set<String> publishCandidates = new HashSet<>(syncedFiles);
        publishCandidates.addAll(List.of(walDirectory.listAll()));
        List<CommitingIndexFile> files = new ArrayList<>();
        for (String name : publishCandidates) {
            if (shouldPublish(name) && shouldCommitToManifest(name) && Files.exists(walDataPath.resolve(name))) {
                files.add(new CommitingIndexFile(indexName, walDataPath.resolve(name)));
            }
        }
        if (!files.isEmpty()) {
            manifestManager.commit(files);
        }
        syncedFiles.clear();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        ensureOpen();
        walDirectory.rename(source, dest);
        if (syncedFiles.remove(source) || isCommittedSegmentFile(dest)) {
            syncedFiles.add(dest);
        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        if (walFileAvailable(name)) {
            return walDirectory.openInput(name, context);
        }
        if (sharedCacheAvailable(name)) {
            return sharedDirectory.openInput(name, context);
        }
        cacheRemoteFile(name);
        if (walFileAvailable(name)) {
            return walDirectory.openInput(name, context);
        }
        if (sharedCacheAvailable(name)) {
            return sharedDirectory.openInput(name, context);
        }
        throw new java.nio.file.NoSuchFileException(name);
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
        IOException failure = null;
        try {
            manifestManager.close();
        } catch (Exception e) {
            failure = e instanceof IOException ioException ? ioException : new IOException(e);
        }
        try {
            walDirectory.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            sharedDirectory.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            retryPendingLocalDeletes();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return walDirectory.getPendingDeletions();
    }

    private void cacheRemoteFile(String name) throws IOException {
        Object lock = cacheLocks.computeIfAbsent(name, ignored -> new Object());
        synchronized (lock) {
            if (localFileAvailable(name)) {
                return;
            }
            IndexFileMetadata metadata = readableRemoteFileMetadata(name);
            Path temp = sharedTempPath.resolve(name + "." + Thread.currentThread().threadId() + ".tmp");
            try {
                manifestManager.download(metadata, temp);
                if (localFileAvailable(name)) {
                    return;
                }
                moveIntoCache(temp, sharedDataPath.resolve(name));
            } finally {
                Files.deleteIfExists(temp);
            }
        }
    }

    private void moveIntoCache(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            if (Files.exists(target)) {
                return;
            }
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private IndexFileMetadata readableRemoteFileMetadata(String name) throws IOException {
        IndexFileMetadata metadata = manifestManager.fileMetadata(indexName, name);
        if (!readableRemoteStatuses.contains(metadata.getStatus())) {
            throw new java.nio.file.NoSuchFileException(name);
        }
        return metadata;
    }

    private boolean localFileAvailable(String name) throws IOException {
        return walFileAvailable(name) || sharedCacheAvailable(name);
    }

    private boolean walFileAvailable(String name) {
        return includeWalFiles && Files.exists(walDataPath.resolve(name));
    }

    private boolean sharedCacheAvailable(String name) throws IOException {
        if (!Files.exists(sharedDataPath.resolve(name))) {
            return false;
        }
        if (includeWalFiles) {
            return true;
        }
        readableRemoteFileMetadata(name);
        return true;
    }

    private void deleteLocalIfExists(String name) throws IOException {
        boolean deleted = true;
        deleted &= deleteIfExists(walDataPath.resolve(name));
        deleted &= deleteIfExists(sharedDataPath.resolve(name));
        if (deleted) {
            pendingLocalDeletes.remove(name);
        } else {
            pendingLocalDeletes.add(name);
        }
    }

    private boolean deleteIfExists(Path path) throws IOException {
        if (!Files.exists(path)) {
            return true;
        }
        try {
            Files.deleteIfExists(path);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private void retryPendingLocalDeletes() throws IOException {
        IOException failure = null;
        for (String name : List.copyOf(pendingLocalDeletes)) {
            try {
                deleteLocalIfExists(name);
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private boolean shouldPublish(String name) {
        return !name.startsWith("pending_segments_") && !name.equals("write.lock");
    }

    private boolean shouldCommitToManifest(String name) throws IOException {
        try {
            IndexFileMetadata metadata = manifestManager.fileMetadata(indexName, name);
            return metadata.getStatus() == IndexFileStatus.DIRTY
                    || metadata.getStatus() == IndexFileStatus.UPLOADING;
        } catch (java.nio.file.NoSuchFileException e) {
            return true;
        }
    }

    private boolean isCommittedSegmentFile(String name) {
        return name.startsWith("segments_") && !name.startsWith("pending_segments_");
    }
}
