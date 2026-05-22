package com.github.wxk6b1203.index;

import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.cluster.ShardId;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.search.ByQueryRequest;
import com.github.wxk6b1203.search.ByQueryResponse;
import com.github.wxk6b1203.search.PointInTimeResponse;
import com.github.wxk6b1203.store.directory.S3CachingDirectory;
import com.github.wxk6b1203.store.directory.S3DirectoryOptions;
import com.github.wxk6b1203.store.directory.S3LockFactory;
import com.github.wxk6b1203.store.common.PathUtil;
import com.github.wxk6b1203.store.manifest.ManifestManager;
import com.github.wxk6b1203.store.manifest.ManifestOptions;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import com.github.wxk6b1203.search.SearchHit;
import com.github.wxk6b1203.search.SearchRequest;
import com.github.wxk6b1203.search.SearchResponse;
import com.github.wxk6b1203.util.JsonUtil;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class LuceneLocalShardIndexService implements LocalShardIndexService {
    private static final int UPDATE_BY_QUERY_BATCH_SIZE = 512;

    private final Path basePath;
    private final String bucket;
    private final ManifestMetadataManager metadataManager;
    private final RemoteObjectStore remoteObjectStore;
    private final Map<ShardId, ShardWriter> writers = new ConcurrentHashMap<>();
    private final Map<String, PitContext> pits = new ConcurrentHashMap<>();

    public LuceneLocalShardIndexService(
            Path basePath,
            String bucket,
            ManifestMetadataManager metadataManager,
            RemoteObjectStore remoteObjectStore
    ) {
        this.basePath = basePath;
        this.bucket = bucket;
        this.metadataManager = metadataManager;
        this.remoteObjectStore = remoteObjectStore;
    }

    @Override
    public IndexDocumentResponse index(IndexDocumentRequest request) throws IOException {
        String id = request.id() == null || request.id().isBlank() ? UUID.randomUUID().toString() : request.id();
        ShardWriter shardWriter = writers.computeIfAbsent(request.shardId(), this::openShardWriter);
        synchronized (shardWriter) {
            Document document = document(request.indexName(), request.shardId(), id, request.source(), request.mappings());
            if (request.createOnly()) {
                try (DirectoryReader reader = DirectoryReader.open(shardWriter.writer)) {
                    if (new IndexSearcher(reader).count(new TermQuery(new Term("_id", id))) > 0) {
                        throw new IllegalArgumentException("document already exists: " + id);
                    }
                }
                shardWriter.writer.addDocument(document);
            } else {
                shardWriter.writer.updateDocument(new Term("_id", id), document);
            }
            shardWriter.writer.commit();
        }
        return new IndexDocumentResponse(
                request.indexName(),
                request.shardId(),
                id,
                request.createOnly() ? "created" : "indexed",
                true
        );
    }

    @Override
    public IndexDocumentResponse delete(IndexDocumentRequest request) throws IOException {
        if (request.id() == null || request.id().isBlank()) {
            throw new IllegalArgumentException("delete requires document id");
        }
        ShardWriter shardWriter = writers.computeIfAbsent(request.shardId(), this::openShardWriter);
        synchronized (shardWriter) {
            shardWriter.writer.deleteDocuments(new Term("_id", request.id()));
            shardWriter.writer.commit();
        }
        return new IndexDocumentResponse(request.indexName(), request.shardId(), request.id(), "deleted", true);
    }

    @Override
    public SearchResponse search(ShardId shardId, SearchRequest request) throws IOException {
        long started = System.nanoTime();
        PitContext pit = pit(request.pitId(), shardId);
        if (pit != null) {
            if (pit.reader() == null) {
                return emptySearchResponse(started);
            }
            return searchReader(pit.reader(), request, started);
        }
        boolean remoteOnly = "remote".equalsIgnoreCase(request.readPreference())
                || "strong".equalsIgnoreCase(request.readPreference());
        ShardWriter shardWriter = remoteOnly ? null : writers.get(shardId);
        if (shardWriter != null) {
            synchronized (shardWriter) {
                try (DirectoryReader reader = DirectoryReader.open(shardWriter.writer)) {
                    return searchReader(reader, request, started);
                }
            }
        }
        try (S3CachingDirectory directory = openShardDirectory(
                shardId,
                remoteSnapshotStatuses(),
                false,
                request.remoteSnapshotGeneration()
        );
             DirectoryReader reader = DirectoryReader.open(directory)) {
            return searchReader(reader, request, started);
        } catch (IndexNotFoundException e) {
            return emptySearchResponse(started);
        }
    }

    private SearchResponse emptySearchResponse(long started) {
        return new SearchResponse(
                (System.nanoTime() - started) / 1_000_000,
                1,
                1,
                0,
                List.of(),
                Map.of(),
                List.of()
        );
    }

    private SearchResponse searchReader(DirectoryReader reader, SearchRequest request, long started) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        int from = Math.max(0, request.from());
        int size = Math.max(0, request.size());
        Query query = searchQuery(request);
        Sort luceneSort = luceneSort(request.sort(), request.mappings());
        if (!request.searchAfter().isEmpty() && luceneSort == null) {
            throw new IllegalArgumentException("search_after requires sort");
        }
        int topN = Math.max(1, request.searchAfter().isEmpty() ? from + size : size);
        ScoreDoc after = searchAfterDoc(request, reader.maxDoc());
        TopDocs topDocs = luceneSort == null
                ? searcher.search(query, topN)
                : searcher.searchAfter(after, query, topN, luceneSort, true);
        List<SearchHit> hits = new ArrayList<>();
        var storedFields = searcher.storedFields();
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            hits.add(hit(
                    searcher,
                    scoreDoc.doc,
                    storedFields.document(scoreDoc.doc),
                    scoreDoc.score,
                    nativeSortValues(topDocs, scoreDoc),
                    request.sort(),
                    request.mappings()
            ));
        }
        if (request.vector() != null && request.vector().minScore() != null) {
            hits = hits.stream()
                    .filter(hit -> hit.score() >= request.vector().minScore())
                    .toList();
        }
        if (luceneSort == null && !request.sort().isEmpty()) {
            hits = hits.stream()
                    .sorted((left, right) -> compareHits(left, right, request.sort()))
                    .toList();
        }
        int hitFrom = Math.min(request.searchAfter().isEmpty() ? from : 0, hits.size());
        int hitTo = Math.min(hitFrom + size, hits.size());
        List<SearchHit> page = hits.subList(hitFrom, hitTo);
        Map<String, Object> aggregations = aggregate(searcher, query, request.aggregations(), request.mappings());
        return new SearchResponse(
                (System.nanoTime() - started) / 1_000_000,
                1,
                1,
                0,
                page,
                aggregations,
                List.of()
        );
    }

    @Override
    public PointInTimeResponse openPointInTime(
            ShardId shardId,
            String indexName,
            Duration keepAlive,
            String readPreference
    ) throws IOException {
        cleanupExpiredPits();
        Duration ttl = keepAlive == null || keepAlive.isNegative() || keepAlive.isZero() ? Duration.ofMinutes(1) : keepAlive;
        if ("remote".equalsIgnoreCase(readPreference) || "strong".equalsIgnoreCase(readPreference)) {
            return openRemotePointInTime(shardId, indexName, ttl);
        }
        ShardWriter shardWriter = writers.computeIfAbsent(shardId, this::openShardWriter);
        synchronized (shardWriter) {
            DirectoryReader reader = DirectoryReader.open(shardWriter.writer);
            String id = UUID.randomUUID().toString();
            pits.put(id, new PitContext(shardId, indexName, reader, null, Instant.now().plus(ttl)));
            return new PointInTimeResponse(id);
        }
    }

    private PointInTimeResponse openRemotePointInTime(ShardId shardId, String indexName, Duration ttl) throws IOException {
        S3CachingDirectory directory = openShardDirectory(shardId);
        String id = UUID.randomUUID().toString();
        directory.pinRemoteSnapshot(id, Instant.now().plus(ttl));
        try {
            DirectoryReader reader = DirectoryReader.open(directory);
            pits.put(id, new PitContext(shardId, indexName, reader, directory, Instant.now().plus(ttl)));
        } catch (IndexNotFoundException e) {
            directory.close();
            pits.put(id, new PitContext(shardId, indexName, null, null, Instant.now().plus(ttl)));
        } catch (IOException | RuntimeException e) {
            directory.close();
            throw e;
        }
        return new PointInTimeResponse(id);
    }

    @Override
    public boolean closePointInTime(String pitId) throws IOException {
        if (pitId == null || pitId.isBlank()) {
            return false;
        }
        PitContext pit = pits.remove(pitId);
        if (pit == null) {
            return false;
        }
        closePit(pit);
        return true;
    }

    private PitContext pit(String pitId, ShardId shardId) throws IOException {
        if (pitId == null || pitId.isBlank()) {
            return null;
        }
        PitContext pit = pits.get(pitId);
        if (pit == null) {
            throw new IllegalArgumentException("point in time not found: " + pitId);
        }
        if (Instant.now().isAfter(pit.expiresAt())) {
            closePointInTime(pitId);
            throw new IllegalArgumentException("point in time expired: " + pitId);
        }
        if (!pit.shardId().equals(shardId)) {
            throw new IllegalArgumentException("point in time does not belong to shard: " + shardId.routeKey());
        }
        return pit;
    }

    private void cleanupExpiredPits() {
        Instant now = Instant.now();
        pits.entrySet().removeIf(entry -> {
            if (now.isBefore(entry.getValue().expiresAt())) {
                return false;
            }
            try {
                closePit(entry.getValue());
            } catch (IOException ignored) {
            }
            return true;
        });
    }

    private void closePit(PitContext pit) throws IOException {
        IOException failure = null;
        if (pit.reader() != null) {
            try {
                pit.reader().close();
            } catch (IOException e) {
                failure = e;
            }
        }
        if (pit.directory() != null) {
            try {
                pit.directory().close();
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

    private SearchHit hit(
            IndexSearcher searcher,
            int docId,
            Document document,
            float score,
            List<Object> nativeSortValues,
            List<Map<String, Object>> sort,
            Map<String, FieldMapping> mappings
    ) throws IOException {
        Map<String, Object> source = source(document);
        return new SearchHit(
                document.get("_index"),
                document.get("_id"),
                score,
                source,
                nativeSortValues == null
                        ? sortValues(searcher, docId, document.get("_id"), score, source, sort, mappings)
                        : nativeSortValues
        );
    }

    private List<Object> nativeSortValues(TopDocs topDocs, ScoreDoc scoreDoc) {
        if (!(topDocs instanceof TopFieldDocs) || !(scoreDoc instanceof FieldDoc fieldDoc)) {
            return null;
        }
        return Arrays.stream(fieldDoc.fields)
                .map(value -> value instanceof BytesRef bytesRef ? bytesRef.utf8ToString() : value)
                .toList();
    }

    private ScoreDoc searchAfterDoc(SearchRequest request, int maxDoc) {
        if (request.searchAfter().isEmpty()) {
            return null;
        }
        Object[] fields = coerceSearchAfterValues(request.sort(), request.mappings(), request.searchAfter());
        return new FieldDoc(Math.max(0, maxDoc - 1), Float.NaN, fields);
    }

    private Object[] coerceSearchAfterValues(
            List<Map<String, Object>> sort,
            Map<String, FieldMapping> mappings,
            List<Object> values
    ) {
        if (values.size() != sort.size()) {
            throw new IllegalArgumentException("search_after value count must match sort field count");
        }
        Object[] result = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = coerceSearchAfterValue(sortField(sort.get(i)), mappings, values.get(i));
        }
        return result;
    }

    private Object coerceSearchAfterValue(String field, Map<String, FieldMapping> mappings, Object value) {
        if (value == null) {
            return null;
        }
        if ("_score".equals(field)) {
            return value instanceof Number number ? number.floatValue() : Float.valueOf(String.valueOf(value));
        }
        if ("_id".equals(field)) {
            return new BytesRef(String.valueOf(value));
        }
        FieldMapping mapping = mappings.get(field);
        if (mapping == null) {
            throw new IllegalArgumentException("search_after sort field is not mapped: " + field);
        }
        if (mapping.longNumber()) {
            return value instanceof Number number ? number.longValue() : Long.valueOf(String.valueOf(value));
        }
        if (mapping.doubleNumber()) {
            return value instanceof Number number ? number.doubleValue() : Double.valueOf(String.valueOf(value));
        }
        return new BytesRef(String.valueOf(value));
    }

    private List<Object> sortValues(
            IndexSearcher searcher,
            int docId,
            String id,
            float score,
            Map<String, Object> source,
            List<Map<String, Object>> sort,
            Map<String, FieldMapping> mappings
    ) throws IOException {
        if (sort == null || sort.isEmpty()) {
            return List.of();
        }
        List<Object> values = new ArrayList<>(sort.size());
        for (Map<String, Object> spec : sort) {
            String field = sortField(spec);
            Object value = switch (field) {
                case "_id" -> id;
                case "_score" -> score;
                default -> sortFieldValue(searcher, docId, field, mappings.get(field), source);
            };
            values.add(value);
        }
        return values;
    }

    private Object sortFieldValue(
            IndexSearcher searcher,
            int docId,
            String field,
            FieldMapping mapping,
            Map<String, Object> source
    ) throws IOException {
        if (mapping != null && Boolean.TRUE.equals(mapping.docValues())) {
            Object value = docValue(searcher, docId, field, mapping);
            if (value != null) {
                return value;
            }
        }
        return source.get(field);
    }

    private int compareHits(SearchHit left, SearchHit right, List<Map<String, Object>> sort) {
        for (int i = 0; i < sort.size(); i++) {
            int compared = compareValues(left.sortValues().get(i), right.sortValues().get(i));
            if (compared != 0) {
                return sortDescending(sort.get(i)) ? -compared : compared;
            }
        }
        int byScore = Float.compare(right.score(), left.score());
        return byScore != 0 ? byScore : left.id().compareTo(right.id());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareValues(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            return Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
        }
        if (left instanceof Comparable comparable && left.getClass().isInstance(right)) {
            return comparable.compareTo(right);
        }
        return String.valueOf(left).compareTo(String.valueOf(right));
    }

    private String sortField(Map<String, Object> spec) {
        return spec.keySet().stream().findFirst().orElse("_score");
    }

    private boolean sortDescending(Map<String, Object> spec) {
        String field = sortField(spec);
        Object value = spec.get(field);
        if (value instanceof Map<?, ?> map) {
            Object order = map.get("order");
            return order == null ? "_score".equals(field) : "desc".equalsIgnoreCase(String.valueOf(order));
        }
        return value == null ? "_score".equals(field) : "desc".equalsIgnoreCase(String.valueOf(value));
    }

    private Sort luceneSort(List<Map<String, Object>> sort, Map<String, FieldMapping> mappings) {
        if (sort == null || sort.isEmpty()) {
            return null;
        }
        List<SortField> fields = new ArrayList<>(sort.size());
        for (Map<String, Object> spec : sort) {
            fields.add(luceneSortField(spec, mappings));
        }
        return new Sort(fields.toArray(SortField[]::new));
    }

    private SortField luceneSortField(Map<String, Object> spec, Map<String, FieldMapping> mappings) {
        String field = sortField(spec);
        boolean descending = sortDescending(spec);
        if ("_score".equals(field)) {
            return new SortField(null, SortField.Type.SCORE, descending);
        }
        if ("_id".equals(field)) {
            return new SortField("_id", SortField.Type.STRING, descending, SortField.STRING_LAST);
        }
        FieldMapping mapping = mappings.get(field);
        if (mapping == null) {
            throw new IllegalArgumentException("sort field is not mapped: " + field);
        }
        if (!Boolean.TRUE.equals(mapping.docValues())) {
            throw new IllegalArgumentException("sort field requires doc_values: " + field);
        }
        if (mapping.keyword() || mapping.bool()) {
            return new SortField(field, SortField.Type.STRING, descending, SortField.STRING_LAST);
        }
        if (mapping.longNumber()) {
            return new SortField(field, SortField.Type.LONG, descending, descending ? Long.MIN_VALUE : Long.MAX_VALUE);
        }
        if (mapping.doubleNumber()) {
            return new SortField(
                    field,
                    SortField.Type.DOUBLE,
                    descending,
                    descending ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY
            );
        }
        throw new IllegalArgumentException("sort field type is not supported: " + field);
    }

    @Override
    public ByQueryResponse deleteByQuery(ShardId shardId, ByQueryRequest request) throws IOException {
        ShardWriter shardWriter = writers.computeIfAbsent(shardId, this::openShardWriter);
        Query query = query(request.query(), request.mappings());
        long deleted;
        synchronized (shardWriter) {
            try (DirectoryReader reader = DirectoryReader.open(shardWriter.writer)) {
                deleted = new IndexSearcher(reader).count(query);
            }
            shardWriter.writer.deleteDocuments(query);
            shardWriter.writer.commit();
        }
        return new ByQueryResponse(null, "delete_by_query", "deleted=" + deleted);
    }

    @Override
    public ByQueryResponse updateByQuery(ShardId shardId, ByQueryRequest request) throws IOException {
        if (request.document() == null || request.document().isEmpty()) {
            throw new IllegalArgumentException("update_by_query requires a non-empty doc object");
        }
        ShardWriter shardWriter = writers.computeIfAbsent(shardId, this::openShardWriter);
        Query query = query(request.query(), request.mappings());
        long updated = 0;
        synchronized (shardWriter) {
            try (DirectoryReader reader = DirectoryReader.open(shardWriter.writer)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                var storedFields = searcher.storedFields();
                ScoreDoc after = null;
                while (true) {
                    TopDocs topDocs = searcher.searchAfter(after, query, UPDATE_BY_QUERY_BATCH_SIZE);
                    if (topDocs.scoreDocs.length == 0) {
                        break;
                    }
                    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                        var old = storedFields.document(scoreDoc.doc);
                        Map<String, Object> merged = new HashMap<>(source(old));
                        merged.putAll(request.document());
                        Document document = document(old.get("_index"), shardId, old.get("_id"), merged, request.mappings());
                        shardWriter.writer.updateDocument(new Term("_id", document.get("_id")), document);
                        updated++;
                    }
                    after = topDocs.scoreDocs[topDocs.scoreDocs.length - 1];
                }
            }
            shardWriter.writer.commit();
        }
        return new ByQueryResponse(null, "update_by_query", "updated=" + updated);
    }

    @Override
    public void deleteIndex(String indexName, int numberOfShards) throws IOException {
        IOException failure = null;
        for (String pitId : List.copyOf(pits.keySet())) {
            PitContext pit = pits.get(pitId);
            if (pit != null && pit.indexName().equals(indexName)) {
                try {
                    closePointInTime(pitId);
                } catch (IOException e) {
                    failure = addFailure(failure, e);
                }
            }
        }
        for (ShardId shardId : List.copyOf(writers.keySet())) {
            if (shardId.indexName().equals(indexName)) {
                ShardWriter writer = writers.remove(shardId);
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        failure = addFailure(failure, e);
                    }
                }
            }
        }
        for (int shard = 0; shard < numberOfShards; shard++) {
            String physicalIndexName = indexName + "__shard_" + shard;
            try {
                deleteRecursively(PathUtil.walDataPath(basePath, physicalIndexName));
                deleteRecursively(PathUtil.sharedDataPath(basePath, physicalIndexName));
                deleteRecursively(PathUtil.sharedTempPath(basePath, physicalIndexName));
            } catch (Exception e) {
                failure = addFailure(failure, e instanceof IOException ioException ? ioException : new IOException(e));
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void retryPendingUploads(Collection<ShardId> shardIds) throws IOException {
        IOException failure = null;
        for (ShardId shardId : shardIds) {
            try {
                ShardWriter shardWriter = writers.get(shardId);
                if (shardWriter != null) {
                    synchronized (shardWriter) {
                        shardWriter.directory.publishLocalCommit();
                    }
                } else {
                    Path walPath = PathUtil.walDataPath(basePath, physicalIndexName(shardId));
                    if (!Files.isDirectory(walPath) || !hasCommittedSegmentFile(walPath)) {
                        discardPendingUploads(shardId);
                        continue;
                    }
                    try (S3CachingDirectory directory = openShardDirectory(shardId, remoteSnapshotStatuses())) {
                        directory.publishLocalCommit();
                    }
                }
            } catch (IOException e) {
                failure = addFailure(failure, e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public int openPointInTimeCount() {
        cleanupExpiredPits();
        return pits.size();
    }

    private void discardPendingUploads(ShardId shardId) {
        try (ManifestManager manifestManager = openManifestManager()) {
            manifestManager.discardPendingUploads(physicalIndexName(shardId));
        }
    }

    private ShardWriter openShardWriter(ShardId shardId) {
        try {
            S3CachingDirectory directory = openShardDirectory(shardId, remoteSnapshotStatuses());
            IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            return new ShardWriter(directory, new IndexWriter(directory, config));
        } catch (IOException e) {
            throw new IllegalStateException("failed to open shard writer: " + shardId.routeKey(), e);
        }
    }

    private S3CachingDirectory openShardDirectory(ShardId shardId) throws IOException {
        return openShardDirectory(shardId, remoteSnapshotStatuses(), false);
    }

    private S3CachingDirectory openShardDirectory(ShardId shardId, List<IndexFileStatus> readableRemoteStatuses) throws IOException {
        return openShardDirectory(shardId, readableRemoteStatuses, true);
    }

    private S3CachingDirectory openShardDirectory(
            ShardId shardId,
            List<IndexFileStatus> readableRemoteStatuses,
            boolean includeWalFiles
    ) throws IOException {
        return openShardDirectory(shardId, readableRemoteStatuses, includeWalFiles, null);
    }

    private S3CachingDirectory openShardDirectory(
            ShardId shardId,
            List<IndexFileStatus> readableRemoteStatuses,
            boolean includeWalFiles,
            Long remoteSnapshotGeneration
    ) throws IOException {
        String physicalIndexName = physicalIndexName(shardId);
        return new S3CachingDirectory(
                new S3DirectoryOptions(basePath, physicalIndexName),
                new S3LockFactory(),
                openManifestManager(),
                readableRemoteStatuses,
                includeWalFiles,
                remoteSnapshotGeneration
        );
    }

    private ManifestManager openManifestManager() {
        return new ManifestManager(new ManifestOptions(bucket), remoteObjectStore, metadataManager);
    }

    private List<IndexFileStatus> remoteSnapshotStatuses() {
        return List.of(
                IndexFileStatus.CLEAN,
                IndexFileStatus.PINNED
        );
    }

    private void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        IOException failure = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try {
                deleteRecursivelyOnce(path);
                return;
            } catch (IOException e) {
                failure = e;
                sleepBeforeDeleteRetry(attempt, path);
            }
        }
        throw failure;
    }

    private void deleteRecursivelyOnce(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (var stream = Files.walk(path)) {
            IOException failure = null;
            for (Path item : stream.sorted(Comparator.reverseOrder()).toList()) {
                try {
                    Files.deleteIfExists(item);
                } catch (IOException e) {
                    failure = addFailure(failure, e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private void sleepBeforeDeleteRetry(int attempt, Path path) throws IOException {
        try {
            Thread.sleep(50L * (attempt + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while retrying delete: " + path, e);
        }
    }

    private IOException addFailure(IOException failure, IOException e) {
        if (failure == null) {
            return e;
        }
        failure.addSuppressed(e);
        return failure;
    }

    private Document document(
            String indexName,
            ShardId shardId,
            String id,
            Map<String, Object> source,
            Map<String, FieldMapping> mappings
    ) {
        Document document = new Document();
        document.add(new StringField("_id", id, Field.Store.YES));
        document.add(new SortedDocValuesField("_id", new BytesRef(id)));
        document.add(new StringField("_index", indexName, Field.Store.YES));
        document.add(new StringField("_shard", shardId.routeKey(), Field.Store.YES));
        document.add(new StoredField("_source", new String(JsonUtil.writeValueAsBytes(source), StandardCharsets.UTF_8)));
        mappings.forEach((field, mapping) -> {
            Object value = source.get(field);
            if (value != null) {
                addMappedField(document, field, value, mapping);
            }
        });
        return document;
    }

    private void addMappedField(Document document, String field, Object value, FieldMapping mapping) {
        if (mapping.denseVector() && Boolean.TRUE.equals(mapping.indexed())) {
            float[] vector = vectorValue(value);
            if (vector == null) {
                throw new IllegalArgumentException("dense_vector field must be a non-empty numeric array: " + field);
            }
            if (vector.length != mapping.dimension()) {
                throw new IllegalArgumentException("dense_vector field dimension mismatch: " + field);
            }
            document.add(new KnnFloatVectorField(field, vector, similarity(mapping)));
            return;
        }
        if (mapping.keyword()) {
            if (Boolean.TRUE.equals(mapping.indexed())) {
                document.add(new StringField(field, String.valueOf(value), store(mapping)));
            }
            if (Boolean.TRUE.equals(mapping.docValues())) {
                document.add(new SortedDocValuesField(field, new BytesRef(String.valueOf(value))));
            }
            return;
        }
        if (mapping.text()) {
            if (Boolean.TRUE.equals(mapping.indexed())) {
                document.add(new TextField(field, String.valueOf(value), store(mapping)));
            }
            return;
        }
        if (mapping.longNumber()) {
            Long number = longValue(value);
            if (number != null) {
                if (Boolean.TRUE.equals(mapping.indexed())) {
                    document.add(new LongPoint(field, number));
                }
                if (Boolean.TRUE.equals(mapping.docValues())) {
                    document.add(new NumericDocValuesField(field, number));
                }
            }
            return;
        }
        if (mapping.doubleNumber()) {
            Double number = doubleValue(value);
            if (number != null) {
                if (Boolean.TRUE.equals(mapping.indexed())) {
                    document.add(new DoublePoint(field, number));
                }
                if (Boolean.TRUE.equals(mapping.docValues())) {
                    document.add(new DoubleDocValuesField(field, number));
                }
            }
            return;
        }
        if (mapping.bool()) {
            if (Boolean.TRUE.equals(mapping.indexed())) {
                document.add(new StringField(field, String.valueOf(value), store(mapping)));
            }
            if (Boolean.TRUE.equals(mapping.docValues())) {
                document.add(new SortedDocValuesField(field, new BytesRef(String.valueOf(value))));
            }
        }
    }

    private Map<String, Object> source(Document document) {
        String source = document.get("_source");
        return source == null || source.isBlank() ? Map.of() : JsonUtil.readValueAsMap(source);
    }

    private Map<String, Object> aggregate(
            IndexSearcher searcher,
            Query query,
            List<Map<String, Object>> aggregations,
            Map<String, FieldMapping> mappings
    ) throws IOException {
        if (aggregations == null || aggregations.isEmpty()) {
            return Map.of();
        }
        int count = searcher.count(query);
        List<Integer> docIds = new ArrayList<>(count);
        List<Map<String, Object>> sources = new ArrayList<>(count);
        if (count > 0) {
            TopDocs topDocs = searcher.search(query, count);
            var storedFields = searcher.storedFields();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                docIds.add(scoreDoc.doc);
                sources.add(source(storedFields.document(scoreDoc.doc)));
            }
        }
        Map<String, Object> results = new LinkedHashMap<>();
        for (Map<String, Object> aggregation : aggregations) {
            String name = stringValue(aggregation.get("name"));
            if (name == null || name.isBlank()) {
                continue;
            }
            if (aggregation.get("terms") instanceof Map<?, ?> terms) {
                results.put(name, termsAggregation(searcher, docIds, sources, mapValue(terms), mappings));
            } else if (aggregation.get("min") instanceof Map<?, ?> min) {
                results.put(name, metricAggregation(searcher, docIds, sources, mapValue(min), "min", mappings));
            } else if (aggregation.get("max") instanceof Map<?, ?> max) {
                results.put(name, metricAggregation(searcher, docIds, sources, mapValue(max), "max", mappings));
            } else if (aggregation.get("sum") instanceof Map<?, ?> sum) {
                results.put(name, metricAggregation(searcher, docIds, sources, mapValue(sum), "sum", mappings));
            } else if (aggregation.get("avg") instanceof Map<?, ?> avg) {
                results.put(name, metricAggregation(searcher, docIds, sources, mapValue(avg), "avg", mappings));
            } else if (aggregation.get("value_count") instanceof Map<?, ?> valueCount) {
                results.put(name, metricAggregation(searcher, docIds, sources, mapValue(valueCount), "value_count", mappings));
            }
        }
        return results;
    }

    private Map<String, Object> termsAggregation(
            IndexSearcher searcher,
            List<Integer> docIds,
            List<Map<String, Object>> sources,
            Map<String, Object> spec,
            Map<String, FieldMapping> mappings
    ) throws IOException {
        String field = stringValue(spec.get("field"));
        int size = Math.max(1, intValue(spec.get("size"), 10));
        Map<String, Long> counts = new HashMap<>();
        FieldMapping mapping = mappings.get(field);
        for (int i = 0; i < docIds.size(); i++) {
            Object value = aggregationValue(searcher, docIds.get(i), field, mapping, sources.get(i));
            if (value != null) {
                counts.merge(String.valueOf(value), 1L, Long::sum);
            }
        }
        List<Map<String, Object>> buckets = counts.entrySet().stream()
                .sorted(Comparator
                        .comparing((Map.Entry<String, Long> entry) -> entry.getValue()).reversed()
                        .thenComparing(Map.Entry::getKey))
                .limit(size)
                .map(entry -> Map.<String, Object>of(
                        "key", entry.getKey(),
                        "doc_count", entry.getValue()
                ))
                .toList();
        return Map.of(
                "type", "terms",
                "field", field,
                "buckets", buckets
        );
    }

    private Map<String, Object> metricAggregation(
            IndexSearcher searcher,
            List<Integer> docIds,
            List<Map<String, Object>> sources,
            Map<String, Object> spec,
            String type,
            Map<String, FieldMapping> mappings
    ) throws IOException {
        String field = stringValue(spec.get("field"));
        FieldMapping mapping = mappings.get(field);
        double sum = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        long count = 0;
        for (int i = 0; i < docIds.size(); i++) {
            Double value = doubleValue(aggregationValue(searcher, docIds.get(i), field, mapping, sources.get(i)));
            if (value == null) {
                continue;
            }
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
            count++;
        }
        Object value = switch (type) {
            case "min" -> count == 0 ? null : min;
            case "max" -> count == 0 ? null : max;
            case "sum" -> sum;
            case "avg" -> count == 0 ? null : sum / count;
            case "value_count" -> count;
            default -> null;
        };
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("type", type);
        result.put("field", field);
        result.put("value", value);
        result.put("count", count);
        if ("avg".equals(type)) {
            result.put("sum", sum);
        }
        return result;
    }

    private Object aggregationValue(
            IndexSearcher searcher,
            int docId,
            String field,
            FieldMapping mapping,
            Map<String, Object> source
    ) throws IOException {
        if (mapping == null || !Boolean.TRUE.equals(mapping.docValues())) {
            return source.get(field);
        }
        Object docValue = docValue(searcher, docId, field, mapping);
        return docValue == null ? source.get(field) : docValue;
    }

    private Object docValue(IndexSearcher searcher, int docId, String field, FieldMapping mapping) throws IOException {
        LeafReaderContext leaf = leaf(searcher, docId);
        if (leaf == null) {
            return null;
        }
        int leafDocId = docId - leaf.docBase;
        if (mapping.keyword() || mapping.bool()) {
            SortedDocValues values = DocValues.getSorted(leaf.reader(), field);
            if (!values.advanceExact(leafDocId)) {
                return null;
            }
            return values.lookupOrd(values.ordValue()).utf8ToString();
        }
        if (mapping.longNumber()) {
            NumericDocValues values = DocValues.getNumeric(leaf.reader(), field);
            return values.advanceExact(leafDocId) ? values.longValue() : null;
        }
        if (mapping.doubleNumber()) {
            NumericDocValues values = DocValues.getNumeric(leaf.reader(), field);
            return values.advanceExact(leafDocId) ? Double.longBitsToDouble(values.longValue()) : null;
        }
        return null;
    }

    private LeafReaderContext leaf(IndexSearcher searcher, int docId) {
        for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
            if (docId >= leaf.docBase && docId < leaf.docBase + leaf.reader().maxDoc()) {
                return leaf;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mapValue(Object value) {
        return value instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
    }

    private int intValue(Object value, int defaultValue) {
        return value instanceof Number number ? number.intValue() : defaultValue;
    }

    private String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private Query searchQuery(SearchRequest request) {
        if (request.vector() == null) {
            return query(request.query(), request.mappings());
        }
        if (request.vector().field() == null || request.vector().field().isBlank()) {
            throw new IllegalArgumentException("knn field is required");
        }
        FieldMapping mapping = request.mappings().get(request.vector().field());
        if (mapping == null) {
            throw new IllegalArgumentException("knn field is not mapped: " + request.vector().field());
        }
        if (!mapping.denseVector()) {
            throw new IllegalArgumentException("knn field is not a dense_vector: " + request.vector().field());
        }
        float[] target = vectorValue(request.vector().vector());
        if (target == null) {
            throw new IllegalArgumentException("knn query_vector must be a non-empty numeric array");
        }
        if (target.length != mapping.dimension()) {
            throw new IllegalArgumentException("knn query_vector dimension mismatch: " + request.vector().field());
        }
        int k = Math.max(1, request.vector().k());
        Query filter = filterQuery(request.query(), request.mappings());
        return filter == null
                ? new KnnFloatVectorQuery(request.vector().field(), target, k)
                : new KnnFloatVectorQuery(request.vector().field(), target, k, filter);
    }

    private Query filterQuery(Map<String, Object> query, Map<String, FieldMapping> mappings) {
        if (query == null || query.isEmpty() || query.containsKey("match_all")) {
            return null;
        }
        return query(query, mappings);
    }

    private VectorSimilarityFunction similarity(FieldMapping mapping) {
        return switch (mapping.similarity()) {
            case "dot_product" -> VectorSimilarityFunction.DOT_PRODUCT;
            case "euclidean" -> VectorSimilarityFunction.EUCLIDEAN;
            case "maximum_inner_product" -> VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
            default -> VectorSimilarityFunction.COSINE;
        };
    }

    private float[] vectorValue(Object value) {
        if (!(value instanceof List<?> list) || list.isEmpty()) {
            return null;
        }
        float[] vector = new float[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object item = list.get(i);
            if (!(item instanceof Number number)) {
                return null;
            }
            vector[i] = number.floatValue();
        }
        return vector;
    }

    private Query query(Map<String, Object> query, Map<String, FieldMapping> mappings) {
        if (query == null || query.isEmpty() || query.containsKey("match_all")) {
            return MatchAllDocsQuery.INSTANCE;
        }
        Object ids = query.get("ids");
        if (ids instanceof Map<?, ?> idsMap && idsMap.get("values") instanceof Iterable<?> values) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (Object value : values) {
                builder.add(new TermQuery(new Term("_id", String.valueOf(value))), BooleanClause.Occur.SHOULD);
            }
            return builder.build();
        }
        Object term = query.get("term");
        if (term instanceof Map<?, ?> termMap) {
            var entry = termMap.entrySet().stream().findFirst();
            if (entry.isPresent()) {
                return exactQuery(String.valueOf(entry.get().getKey()), entry.get().getValue(), mappings);
            }
        }
        Object match = query.get("match");
        if (match instanceof Map<?, ?> matchMap) {
            var entry = matchMap.entrySet().stream().findFirst();
            if (entry.isPresent()) {
                return matchQuery(String.valueOf(entry.get().getKey()), entry.get().getValue(), mappings);
            }
        }
        Object bool = query.get("bool");
        if (bool instanceof Map<?, ?> boolMap) {
            return boolQuery(boolMap, mappings);
        }
        Object range = query.get("range");
        if (range instanceof Map<?, ?> rangeMap) {
            var entry = rangeMap.entrySet().stream().findFirst();
            if (entry.isPresent() && entry.get().getValue() instanceof Map<?, ?> bounds) {
                return rangeQuery(String.valueOf(entry.get().getKey()), bounds, mappings);
            }
        }
        throw new IllegalArgumentException("unsupported query; supported: match_all, ids.values, term, match, range");
    }

    private Query boolQuery(Map<?, ?> boolMap, Map<String, FieldMapping> mappings) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        addBoolClauses(builder, boolMap.get("filter"), BooleanClause.Occur.FILTER, mappings);
        addBoolClauses(builder, boolMap.get("must"), BooleanClause.Occur.MUST, mappings);
        addBoolClauses(builder, boolMap.get("should"), BooleanClause.Occur.SHOULD, mappings);
        addBoolClauses(builder, boolMap.get("must_not"), BooleanClause.Occur.MUST_NOT, mappings);
        BooleanQuery built = builder.build();
        return built.clauses().isEmpty() ? MatchAllDocsQuery.INSTANCE : built;
    }

    private void addBoolClauses(
            BooleanQuery.Builder builder,
            Object value,
            BooleanClause.Occur occur,
            Map<String, FieldMapping> mappings
    ) {
        if (value == null) {
            return;
        }
        if (value instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> map) {
                    builder.add(query(mapValue(map), mappings), occur);
                }
            }
            return;
        }
        if (value instanceof Map<?, ?> map) {
            builder.add(query(mapValue(map), mappings), occur);
        }
    }

    private Query exactQuery(String field, Object value, Map<String, FieldMapping> mappings) {
        if ("_id".equals(field)) {
            return new TermQuery(new Term("_id", String.valueOf(value)));
        }
        FieldMapping mapping = mappings.get(field);
        if (mapping == null || mapping.keyword() || mapping.text() || mapping.bool()) {
            return new TermQuery(new Term(field, String.valueOf(value)));
        }
        if (mapping.longNumber()) {
            Long number = longValue(value);
            if (number == null) {
                throw new IllegalArgumentException("term value must be numeric for field: " + field);
            }
            return LongPoint.newExactQuery(field, number);
        }
        if (mapping.doubleNumber()) {
            Double number = doubleValue(value);
            if (number == null) {
                throw new IllegalArgumentException("term value must be numeric for field: " + field);
            }
            return DoublePoint.newExactQuery(field, number);
        }
        throw new IllegalArgumentException("term query is not supported for field: " + field);
    }

    private Query matchQuery(String field, Object value, Map<String, FieldMapping> mappings) {
        FieldMapping mapping = mappings.get(field);
        if (mapping == null || !mapping.text()) {
            return exactQuery(field, value, mappings);
        }
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        Arrays.stream(String.valueOf(value).toLowerCase().split("\\W+"))
                .filter(token -> !token.isBlank())
                .forEach(token -> builder.add(new TermQuery(new Term(field, token)), BooleanClause.Occur.SHOULD));
        BooleanQuery built = builder.build();
        return built.clauses().isEmpty() ? MatchAllDocsQuery.INSTANCE : built;
    }

    private Query rangeQuery(String field, Map<?, ?> bounds, Map<String, FieldMapping> mappings) {
        FieldMapping mapping = mappings.get(field);
        if (mapping == null) {
            throw new IllegalArgumentException("range field is not mapped: " + field);
        }
        Object gte = bounds.containsKey("gte") ? bounds.get("gte") : bounds.get("gt");
        Object lte = bounds.containsKey("lte") ? bounds.get("lte") : bounds.get("lt");
        if (mapping.longNumber()) {
            long min = bounds.containsKey("gt") ? requiredLong(gte, field) + 1 : longOrDefault(gte, Long.MIN_VALUE);
            long max = bounds.containsKey("lt") ? requiredLong(lte, field) - 1 : longOrDefault(lte, Long.MAX_VALUE);
            return LongPoint.newRangeQuery(field, min, max);
        }
        if (mapping.doubleNumber()) {
            double min = bounds.containsKey("gt") ? DoublePoint.nextUp(requiredDouble(gte, field)) : doubleOrDefault(gte, Double.NEGATIVE_INFINITY);
            double max = bounds.containsKey("lt") ? DoublePoint.nextDown(requiredDouble(lte, field)) : doubleOrDefault(lte, Double.POSITIVE_INFINITY);
            return DoublePoint.newRangeQuery(field, min, max);
        }
        throw new IllegalArgumentException("range query is only supported for numeric fields: " + field);
    }

    private Field.Store store(FieldMapping mapping) {
        return Boolean.TRUE.equals(mapping.stored()) ? Field.Store.YES : Field.Store.NO;
    }

    private Long longValue(Object value) {
        return value instanceof Number number ? number.longValue() : null;
    }

    private long longOrDefault(Object value, long defaultValue) {
        Long number = longValue(value);
        return number == null ? defaultValue : number;
    }

    private long requiredLong(Object value, String field) {
        Long number = longValue(value);
        if (number == null) {
            throw new IllegalArgumentException("range bound must be numeric for field: " + field);
        }
        return number;
    }

    private Double doubleValue(Object value) {
        return value instanceof Number number ? number.doubleValue() : null;
    }

    private double doubleOrDefault(Object value, double defaultValue) {
        Double number = doubleValue(value);
        return number == null ? defaultValue : number;
    }

    private double requiredDouble(Object value, String field) {
        Double number = doubleValue(value);
        if (number == null) {
            throw new IllegalArgumentException("range bound must be numeric for field: " + field);
        }
        return number;
    }

    private String physicalIndexName(ShardId shardId) {
        return shardId.indexName() + "__shard_" + shardId.shardNumber();
    }

    private boolean hasCommittedSegmentFile(Path walPath) throws IOException {
        try (var paths = Files.list(walPath)) {
            return paths
                    .map(path -> path.getFileName().toString())
                    .anyMatch(this::isCommittedSegmentFile);
        }
    }

    private boolean isCommittedSegmentFile(String name) {
        return name.startsWith("segments_") && !name.startsWith("pending_segments_");
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (String pitId : List.copyOf(pits.keySet())) {
            try {
                closePointInTime(pitId);
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        for (ShardWriter writer : writers.values()) {
            try {
                writer.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        writers.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private record ShardWriter(S3CachingDirectory directory, IndexWriter writer) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            try {
                writer.close();
            } finally {
                directory.close();
            }
        }
    }

    private record PitContext(
            ShardId shardId,
            String indexName,
            DirectoryReader reader,
            S3CachingDirectory directory,
            Instant expiresAt
    ) {
    }
}
