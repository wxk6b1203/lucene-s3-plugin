package com.github.wxk6b1203.index;

import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.cluster.ShardId;
import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.mem.MemMockProvider;
import com.github.wxk6b1203.search.ByQueryRequest;
import com.github.wxk6b1203.search.SearchRequest;
import com.github.wxk6b1203.search.VectorQuery;
import com.github.wxk6b1203.store.common.PathUtil;
import com.github.wxk6b1203.store.object.LocalFileRemoteObjectStore;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LuceneLocalShardIndexServiceTest {
    @TempDir
    Path tempDir;

    @AfterEach
    void cleanupTempDirChildren() throws Exception {
        deleteChildrenWithRetry(tempDir);
    }

    @Test
    public void testIndexDocumentCommitsAndUploadsShardFiles() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Path remoteRoot = tempDir.resolve("remote");
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(remoteRoot)
        )) {
            IndexDocumentResponse response = service.index(new IndexDocumentRequest(
                    "books",
                    shardId,
                    "doc-1",
                    Map.of("title", "Lucene")
            ));

            assertEquals("indexed", response.result());
            assertEquals("doc-1", response.id());
            waitForCleanSnapshot(metadata, "books__shard_0");
            assertTrue(hasUploadedSegment(remoteRoot.resolve("books__shard_0").resolve("_data")));
        }
    }

    @Test
    public void testSearchLocalShardById() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "Lucene")));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "S3")));

            var response = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("term", Map.of("_id", "doc-1")), List.of(), null, null, 0, 10)
            );

            assertEquals(1, response.hits().size());
            assertEquals("doc-1", response.hits().getFirst().id());
            assertEquals("Lucene", response.hits().getFirst().source().get("title"));
        }
    }

    @Test
    public void testRemoteReadUsesLastCleanSnapshotWhenNewerCommitIsUploading() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        ControlledRemoteObjectStore remote = new ControlledRemoteObjectStore(
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                remote
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "clean")));
            waitForCleanSegment(metadata, "books__shard_0");

            remote.failPuts(true);
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "uploading")));
            waitForUncleanSegment(metadata, "books__shard_0");

            var weak = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("match_all", Map.of()), List.of(), null, null, 0, 10)
            );
            var remoteOnly = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            null,
                            Map.of(),
                            "remote"
                    )
            );

            List<String> weakIds = weak.hits().stream().map(hit -> hit.id()).toList();
            List<String> remoteIds = remoteOnly.hits().stream().map(hit -> hit.id()).toList();
            assertTrue(weakIds.contains("doc-1"));
            assertTrue(weakIds.contains("doc-2"));
            assertEquals(List.of("doc-1"), remoteIds);
        }
    }

    @Test
    public void testNewOwnerWritesFromLastCleanSnapshotWhenPreviousOwnerHasUnuploadedCommit() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        ControlledRemoteObjectStore remote = new ControlledRemoteObjectStore(
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        );

        try (LuceneLocalShardIndexService firstOwner = new LuceneLocalShardIndexService(
                tempDir.resolve("node-1"),
                "bucket",
                metadata,
                remote
        )) {
            firstOwner.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "clean")));
            waitForCleanSnapshot(metadata, "books__shard_0");

            remote.failPuts(true);
            firstOwner.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "lost")));
            waitForUncleanSegment(metadata, "books__shard_0");
        }

        remote.failPuts(false);
        try (LuceneLocalShardIndexService secondOwner = new LuceneLocalShardIndexService(
                tempDir.resolve("node-2"),
                "bucket",
                metadata,
                remote
        )) {
            secondOwner.index(new IndexDocumentRequest("books", shardId, "doc-3", Map.of("title", "recovered")));
            waitForCleanSnapshot(metadata, "books__shard_0");

            var response = secondOwner.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            null,
                            Map.of(),
                            "remote"
                    )
            );
            List<String> ids = response.hits().stream().map(hit -> hit.id()).toList();
            assertTrue(ids.contains("doc-1"));
            assertTrue(ids.contains("doc-3"));
            assertFalse(ids.contains("doc-2"));
        }
    }

    @Test
    public void testShardReadsAndWritesFromCleanRemoteSnapshotWhenLocalDataIsLostEverywhere() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        RemoteObjectStore remote = new LocalFileRemoteObjectStore(tempDir.resolve("remote"));

        try (LuceneLocalShardIndexService firstOwner = new LuceneLocalShardIndexService(
                tempDir.resolve("node-1"),
                "bucket",
                metadata,
                remote
        )) {
            firstOwner.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "remote-clean")));
            waitForCleanSnapshot(metadata, "books__shard_0");
        }

        try (LuceneLocalShardIndexService recoveredOwner = new LuceneLocalShardIndexService(
                tempDir.resolve("node-2-empty-local"),
                "bucket",
                metadata,
                remote
        )) {
            var remoteRead = recoveredOwner.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            null,
                            Map.of(),
                            "remote"
                    )
            );
            assertEquals(List.of("doc-1"), remoteRead.hits().stream().map(hit -> hit.id()).toList());

            recoveredOwner.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "recovered")));
            waitForCleanSnapshot(metadata, "books__shard_0");
            var afterWrite = recoveredOwner.search(
                    shardId,
                    new SearchRequest("books", Map.of("match_all", Map.of()), List.of(), null, null, 0, 10)
            );
            assertEquals(List.of("doc-1", "doc-2"), afterWrite.hits().stream().map(hit -> hit.id()).sorted().toList());
        }
    }

    @Test
    public void testRetryPendingUploadsDiscardsUnrecoverablePendingMetadataWithoutWal() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        String physicalIndexName = "books__shard_0";
        metadata.commitFile(new IndexFile(physicalIndexName, "_1.si", 1, 7));
        int segmentEpoch = metadata.commitFile(new IndexFile(physicalIndexName, "segments_2", 1, 9));
        metadata.updateFileStatus(physicalIndexName, "segments_2", segmentEpoch, IndexFileStatus.UPLOADING);

        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir.resolve("node-2"),
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.retryPendingUploads(List.of(shardId));
        }

        assertTrue(metadata.listAll(physicalIndexName, List.of(
                IndexFileStatus.DIRTY,
                IndexFileStatus.UPLOADING
        )).isEmpty());
    }

    @Test
    public void testDeleteIndexClosesWritersAndRemovesLocalShardData() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "Lucene")));
            waitForCleanSnapshot(metadata, "books__shard_0");

            service.deleteIndex("books", 1);

            assertFalse(Files.exists(PathUtil.walDataPath(tempDir, "books__shard_0")));
            assertFalse(Files.exists(PathUtil.sharedDataPath(tempDir, "books__shard_0")));
            assertFalse(Files.exists(PathUtil.sharedTempPath(tempDir, "books__shard_0")));
        }
    }

    @Test
    public void testDeleteByQueryRemovesMatchingDocs() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "Lucene")));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "S3")));

            var delete = service.deleteByQuery(
                    shardId,
                    new ByQueryRequest("books", Map.of("term", Map.of("_id", "doc-1")), Map.of(), null, false)
            );
            var search = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("match_all", Map.of()), List.of(), null, null, 0, 10)
            );

            assertEquals("deleted=1", delete.status());
            assertEquals(1, search.hits().size());
            assertEquals("doc-2", search.hits().getFirst().id());
        }
    }

    @Test
    public void testUpdateByQueryMergesSource() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "Lucene")));

            var update = service.updateByQuery(
                    shardId,
                    new ByQueryRequest(
                            "books",
                            Map.of("term", Map.of("_id", "doc-1")),
                            Map.of("title", "Lucene S3", "version", 2),
                            null,
                            false
                    )
            );
            var search = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("term", Map.of("_id", "doc-1")), List.of(), null, null, 0, 10)
            );

            assertEquals("updated=1", update.status());
            assertEquals("Lucene S3", search.hits().getFirst().source().get("title"));
            assertEquals(2, search.hits().getFirst().source().get("version"));
        }
    }

    @Test
    public void testUpdateByQueryWithNoMatchesReturnsZero() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "Lucene")));

            var update = service.updateByQuery(
                    shardId,
                    new ByQueryRequest(
                            "books",
                            Map.of("term", Map.of("_id", "missing")),
                            Map.of("title", "Changed"),
                            null,
                            false
                    )
            );
            var search = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("term", Map.of("_id", "doc-1")), List.of(), null, null, 0, 10)
            );

            assertEquals("updated=0", update.status());
            assertEquals("Lucene", search.hits().getFirst().source().get("title"));
        }
    }

    @Test
    public void testKnnSearchUsesNumericVectorSourceFields() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            Map<String, FieldMapping> mappings = Map.of(
                    "embedding", new FieldMapping("dense_vector", 2, "cosine", true, true)
            );
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "title", "Lucene",
                    "embedding", List.of(1.0, 0.0)
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of(
                    "title", "S3",
                    "embedding", List.of(0.0, 1.0)
            ), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            new VectorQuery("embedding", List.of(1.0f, 0.0f), 1, 2),
                            null,
                            0,
                            1,
                            mappings
                    )
            );

            assertEquals(1, response.hits().size());
            assertEquals("doc-1", response.hits().getFirst().id());
            assertEquals("Lucene", response.hits().getFirst().source().get("title"));
        }
    }

    @Test
    public void testKnnSearchRejectsWrongQueryDimension() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "embedding", new FieldMapping("dense_vector", 2, "cosine", true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "embedding", List.of(1.0, 0.0)
            ), mappings));

            assertThrows(IllegalArgumentException.class, () -> service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            new VectorQuery("embedding", List.of(1.0f), 1, 2),
                            null,
                            0,
                            1,
                            mappings
                    )
            ));
        }
    }

    @Test
    public void testKnnSearchCanUseScalarFilter() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "category", new FieldMapping("keyword", null, null, true, true),
                "embedding", new FieldMapping("dense_vector", 2, "cosine", true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "category", "hidden",
                    "embedding", List.of(1.0, 0.0)
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of(
                    "category", "visible",
                    "embedding", List.of(0.0, 1.0)
            ), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("term", Map.of("category", "visible")),
                            List.of(),
                            new VectorQuery("embedding", List.of(1.0f, 0.0f), 2, 10),
                            null,
                            0,
                            2,
                            mappings
                    )
            );

            assertEquals(1, response.hits().size());
            assertEquals("doc-2", response.hits().getFirst().id());
        }
    }

    @Test
    public void testKnnSearchCanApplyMinimumScore() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "embedding", new FieldMapping("dense_vector", 2, "cosine", true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "embedding", List.of(1.0, 0.0)
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of(
                    "embedding", List.of(0.0, 1.0)
            ), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            new VectorQuery("embedding", List.of(1.0f, 0.0f), 2, 10, 0.99f),
                            null,
                            0,
                            2,
                            mappings
                    )
            );

            assertEquals(1, response.hits().size());
            assertEquals("doc-1", response.hits().getFirst().id());
        }
    }

    @Test
    public void testDenseVectorRejectsInvalidSourceValue() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "embedding", new FieldMapping("dense_vector", 2, "cosine", true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            assertThrows(IllegalArgumentException.class, () -> service.index(new IndexDocumentRequest(
                    "books",
                    shardId,
                    "doc-1",
                    Map.of("embedding", List.of("bad", "vector")),
                    mappings
            )));
        }
    }

    @Test
    public void testMappedScalarFieldsAreSearchable() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "title", new FieldMapping("text", null, null, true, true),
                "category", new FieldMapping("keyword", null, null, true, true),
                "pages", new FieldMapping("long", null, null, true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "title", "Lucene in Action",
                    "category", "search",
                    "pages", 320
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of(
                    "title", "S3 Storage Notes",
                    "category", "storage",
                    "pages", 120
            ), mappings));

            var keyword = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("term", Map.of("category", "search")), List.of(), null, null, 0, 10, mappings)
            );
            var text = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("match", Map.of("title", "lucene")), List.of(), null, null, 0, 10, mappings)
            );
            var range = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("range", Map.of("pages", Map.of("gte", 300))), List.of(), null, null, 0, 10, mappings)
            );
            var bool = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("bool", Map.of("filter", List.of(
                                    Map.of("term", Map.of("category", "search")),
                                    Map.of("range", Map.of("pages", Map.of("gte", 300)))
                            ))),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );

            assertEquals("doc-1", keyword.hits().getFirst().id());
            assertEquals("doc-1", text.hits().getFirst().id());
            assertEquals("doc-1", range.hits().getFirst().id());
            assertEquals("doc-1", bool.hits().getFirst().id());
        }
    }

    @Test
    public void testSearchCanSortByMappedSourceField() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "pages", new FieldMapping("long", null, null, true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("pages", 300), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("pages", 100), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(Map.of("pages", Map.of("order", "asc"))),
                            mappings
                    )
            );

            assertEquals("doc-2", response.hits().getFirst().id());
            assertEquals(100, ((Number) response.hits().getFirst().sortValues().getFirst()).intValue());
            assertEquals("doc-1", response.hits().get(1).id());
        }
    }

    @Test
    public void testSearchAfterUsesSortCursor() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "pages", new FieldMapping("long", null, null, true, true)
        );
        List<Map<String, Object>> sort = List.of(
                Map.of("pages", Map.of("order", "asc")),
                Map.of("_id", Map.of("order", "asc"))
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("pages", 300), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("pages", 100), mappings));

            var firstPage = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            1,
                            sort,
                            mappings
                    )
            );
            var secondPage = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            1,
                            sort,
                            firstPage.hits().getFirst().sortValues(),
                            mappings
                    )
            );

            assertEquals("doc-2", firstPage.hits().getFirst().id());
            assertEquals("doc-1", secondPage.hits().getFirst().id());
        }
    }

    @Test
    public void testPointInTimeKeepsReaderSnapshot() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "Lucene")));
            String pitId = service.openPointInTime(shardId, "books", Duration.ofMinutes(1)).id();
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "S3")));

            var pitSearch = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            pitId,
                            Map.of()
                    )
            );
            var normalSearch = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("match_all", Map.of()), List.of(), null, null, 0, 10)
            );

            assertEquals(1, pitSearch.hits().size());
            assertEquals("doc-1", pitSearch.hits().getFirst().id());
            assertEquals(2, normalSearch.hits().size());
            assertTrue(service.closePointInTime(pitId));
        }
    }

    @Test
    public void testRemotePointInTimeReadsCleanSnapshotWithoutOpeningWriter() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        RemoteObjectStore remote = new LocalFileRemoteObjectStore(tempDir.resolve("remote"));
        try (LuceneLocalShardIndexService owner = new LuceneLocalShardIndexService(
                tempDir.resolve("node-1"),
                "bucket",
                metadata,
                remote
        )) {
            owner.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "clean")));
            waitForCleanSnapshot(metadata, "books__shard_0");
        }

        Path readerNode = tempDir.resolve("node-2");
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                readerNode,
                "bucket",
                metadata,
                remote
        )) {
            String pitId = service.openPointInTime(shardId, "books", Duration.ofMinutes(1), "remote").id();
            assertEquals(1, metadata.snapshotPins("books__shard_0").size());

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            pitId,
                            Map.of(),
                            "remote"
                    )
            );

            assertEquals(List.of("doc-1"), response.hits().stream().map(hit -> hit.id()).toList());
            assertFalse(hasFiles(PathUtil.walDataPath(readerNode, "books__shard_0")));
            assertTrue(service.closePointInTime(pitId));
            assertTrue(metadata.snapshotPins("books__shard_0").isEmpty());
        }
    }

    @Test
    public void testRemotePointInTimeOnEmptySnapshotDoesNotCreateWriter() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            String pitId = service.openPointInTime(shardId, "books", Duration.ofMinutes(1), "remote").id();

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            pitId,
                            Map.of(),
                            "remote"
                    )
            );

            assertTrue(response.hits().isEmpty());
            assertFalse(hasFiles(PathUtil.walDataPath(tempDir, "books__shard_0")));
            assertTrue(service.closePointInTime(pitId));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAggregationsRunOnMatchedDocuments() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "category", new FieldMapping("keyword", null, null, true, true),
                "pages", new FieldMapping("long", null, null, true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "category", "search",
                    "pages", 100
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of(
                    "category", "search",
                    "pages", 300
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-3", Map.of(
                    "category", "storage",
                    "pages", 900
            ), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("range", Map.of("pages", Map.of("lt", 500))),
                            List.of(
                                    Map.of("name", "by_category", "terms", Map.of("field", "category")),
                                    Map.of("name", "avg_pages", "avg", Map.of("field", "pages")),
                                    Map.of("name", "sum_pages", "sum", Map.of("field", "pages"))
                            ),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );

            Map<String, Object> byCategory = (Map<String, Object>) response.aggregations().get("by_category");
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) byCategory.get("buckets");
            Map<String, Object> avgPages = (Map<String, Object>) response.aggregations().get("avg_pages");
            Map<String, Object> sumPages = (Map<String, Object>) response.aggregations().get("sum_pages");

            assertEquals("search", buckets.getFirst().get("key"));
            assertEquals(2L, buckets.getFirst().get("doc_count"));
            assertEquals(200.0, avgPages.get("value"));
            assertEquals(400.0, sumPages.get("value"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAggregationCanUseDocValuesWithoutIndexingField() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "pages", new FieldMapping("long", null, null, false, true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("pages", 100), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("pages", 300), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(Map.of("name", "sum_pages", "sum", Map.of("field", "pages"))),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );

            Map<String, Object> sumPages = (Map<String, Object>) response.aggregations().get("sum_pages");
            assertEquals(400.0, sumPages.get("value"));
            assertEquals(2L, sumPages.get("count"));
        }
    }

    private void waitForCleanSegment(MemMockProvider metadata, String physicalIndexName) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            boolean cleanSegment = metadata.listAll(physicalIndexName, List.of(IndexFileStatus.CLEAN)).stream()
                    .anyMatch(file -> file.getName().startsWith("segments_"));
            if (cleanSegment) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("segments file was not marked CLEAN");
    }

    private void waitForCleanSnapshot(MemMockProvider metadata, String physicalIndexName) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            boolean cleanSegment = metadata.listAll(physicalIndexName, List.of(IndexFileStatus.CLEAN)).stream()
                    .anyMatch(file -> file.getName().startsWith("segments_"));
            boolean hasUnclean = !metadata.listAll(physicalIndexName, List.of(
                    IndexFileStatus.DIRTY,
                    IndexFileStatus.UPLOADING
            )).isEmpty();
            if (cleanSegment && !hasUnclean) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("remote snapshot was not fully marked CLEAN");
    }

    private void waitForUncleanSegment(MemMockProvider metadata, String physicalIndexName) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            boolean uncleanSegment = metadata.listAll(physicalIndexName, List.of(
                            IndexFileStatus.DIRTY,
                            IndexFileStatus.UPLOADING
                    )).stream()
                    .anyMatch(file -> file.getName().startsWith("segments_"));
            if (uncleanSegment) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("segments file was not left DIRTY/UPLOADING");
    }

    private boolean hasUploadedSegment(Path remoteDataPath) throws Exception {
        if (!Files.isDirectory(remoteDataPath)) {
            return false;
        }
        try (var files = Files.list(remoteDataPath)) {
            return files.anyMatch(path -> path.getFileName().toString().startsWith("segments_"));
        }
    }

    private boolean hasFiles(Path path) throws IOException {
        if (!Files.isDirectory(path)) {
            return false;
        }
        try (var files = Files.list(path)) {
            return files.findAny().isPresent();
        }
    }

    private void deleteChildrenWithRetry(Path root) throws Exception {
        IOException failure = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try {
                deleteChildren(root);
                return;
            } catch (IOException e) {
                failure = e;
                Thread.sleep(50L * (attempt + 1));
            }
        }
        throw failure;
    }

    private void deleteChildren(Path root) throws IOException {
        if (!Files.isDirectory(root)) {
            return;
        }
        IOException failure = null;
        try (var children = Files.list(root)) {
            for (Path child : children.toList()) {
                try {
                    deleteRecursively(child);
                } catch (IOException e) {
                    failure = addFailure(failure, e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        IOException failure = null;
        try (var stream = Files.walk(path)) {
            for (Path item : stream.sorted(Comparator.reverseOrder()).toList()) {
                try {
                    Files.deleteIfExists(item);
                } catch (IOException e) {
                    failure = addFailure(failure, e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private IOException addFailure(IOException failure, IOException e) {
        if (failure == null) {
            return e;
        }
        failure.addSuppressed(e);
        return failure;
    }

    private static class ControlledRemoteObjectStore implements RemoteObjectStore {
        private final RemoteObjectStore delegate;
        private final AtomicBoolean failPuts = new AtomicBoolean();

        private ControlledRemoteObjectStore(RemoteObjectStore delegate) {
            this.delegate = delegate;
        }

        private void failPuts(boolean fail) {
            failPuts.set(fail);
        }

        @Override
        public void put(String key, Path source) throws IOException {
            if (failPuts.get()) {
                throw new IOException("upload intentionally blocked");
            }
            delegate.put(key, source);
        }

        @Override
        public void get(String key, Path target) throws IOException {
            delegate.get(key, target);
        }

        @Override
        public void delete(Collection<String> keys) throws IOException {
            delegate.delete(keys);
        }
    }
}
