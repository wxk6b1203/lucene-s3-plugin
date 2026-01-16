package com.github.wxk6b1203.store.manifest;

import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.store.common.PathUtil;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ManifestManager {
    private final Path basePath;
    private final String bucket;
    private final Duration maxBatchWait;
    private final S3Client s3Client;
    private final ManifestMetadataManager metadataManager;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final BlockingQueue<DeleteTask> deleteQueue = new LinkedBlockingQueue<>();
    private final ExecutorService deleteWorkerPool;

    public ManifestManager(ManifestOptions options, S3Client s3Client, ManifestMetadataManager metadataManager) {
        this.basePath = options.basePath();
        this.bucket = options.bucket();
        this.maxBatchWait = options.maxBatchWait();
        this.s3Client = s3Client;
        this.metadataManager = metadataManager;
        int workers = Runtime.getRuntime().availableProcessors();
        workers = Math.min(workers, options.maxDeleteWorkers());
        workers = Math.max(1, workers);
        ThreadFactory tf = Thread.ofVirtual().name("delete-worker-", 0).factory();
        this.deleteWorkerPool = new ThreadPoolExecutor(
                workers,
                workers,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                tf
        );
        for (int i = 0; i < workers; ++i) {
            deleteWorkerPool.execute(() -> deleteLoop(options.maxBatchDeleteSize()));
        }
    }

    private void deleteLoop(int maxBatchSize) {
        ArrayList<DeleteTask> batch = new ArrayList<>(maxBatchSize);
        while (running.get() || !deleteQueue.isEmpty()) {
            try {
                DeleteTask first = deleteQueue.poll(500, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);

                // 快速捞一批现成的
                deleteQueue.drainTo(batch, maxBatchSize - batch.size());

                // 在时间窗口内继续攒批
                long deadlineNanos = System.nanoTime() + maxBatchWait.toNanos();
                while (batch.size() < maxBatchSize) {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0) {
                        break;
                    }
                    DeleteTask next = deleteQueue.poll(remaining, TimeUnit.NANOSECONDS);
                    if (next == null) {
                        break;
                    }
                    batch.add(next);
                    deleteQueue.drainTo(batch, maxBatchSize - batch.size());
                }
                doDeleteBatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                batch.clear();
            }
        }
    }

    private void doDeleteBatch(List<DeleteTask> tasks) {
        if (tasks.isEmpty()) {
            return;
        }

        List<ObjectIdentifier> objects = new ArrayList<>(tasks.size());

        for (DeleteTask task : tasks) {
            objects.add(ObjectIdentifier.builder()
                    .key(PathUtil.s3ObjectKey(task.indexName(), task.name()))
                    .build());
        }

        DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(d -> d.objects(objects))
                .build();
        s3Client.deleteObjects(deleteObjectsRequest);

        for (DeleteTask task : tasks) {
            metadataManager.finishDelete(task.indexName(), task.name());
        }
    }

    public List<IndexFileMetadata> listAll(List<IndexFileStatus> statuses) {
        return metadataManager.listAll(statuses);
    }

    // 入队删除任务（非阻塞）
    private void enqueueDelete(String indexName, long epoch, String name) {
        if (!running.get()) {
            throw new IllegalStateException("ManifestManager is closed");
        }
        deleteQueue.offer(new DeleteTask(indexName, epoch, name));
    }

    public void deleteFile(String indexName, String name) throws IOException {
        IndexFileMetadata metadata = metadataManager.prepareDelete(indexName, name);
        Path path = PathUtil.walDataPath(basePath, indexName);
        try {
            Files.delete(path.resolve(name));
        } catch (NoSuchFileException ignored) {
        }

        metadataManager.cleaningUp(indexName, name);
        enqueueDelete(indexName, metadata.epoch(), name);
        // TODO: 1. mark as deleted in metadataManager
        //       2. delete from local storage
        //       3. delete from remote storage
    }

    public IndexFileMetadata fileMetadata(String indexName, String name) throws NoSuchFileException {
        IndexFileMetadata fileMetadata = metadataManager.fileMetadata(indexName, name);
        if (fileMetadata == null) {
            throw new NoSuchFileException(name);
        }
        return fileMetadata;
    }

    public void commit(String indexName, Collection<String> names) {
        for (String name : names) {

            metadataManager.commitFile(cleanMetadata);
        }
    }

    public void close() {
        this.running.set(false);
        this.deleteWorkerPool.shutdown();
    }
}
