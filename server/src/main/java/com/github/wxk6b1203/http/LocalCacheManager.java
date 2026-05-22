package com.github.wxk6b1203.http;

import com.github.wxk6b1203.store.directory.Hierarchy;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

@Slf4j
final class LocalCacheManager {
    private final Path sharedRoot;
    private final long maxBytes;
    private final Duration tempMaxAge;

    LocalCacheManager(Path basePath, long maxBytes, Duration tempMaxAge) {
        this.sharedRoot = basePath.resolve(Hierarchy.SHARED.path);
        this.maxBytes = Math.max(0, maxBytes);
        this.tempMaxAge = tempMaxAge == null || tempMaxAge.isNegative() ? Duration.ofHours(1) : tempMaxAge;
    }

    boolean enabled() {
        return maxBytes > 0;
    }

    long maxBytes() {
        return maxBytes;
    }

    CleanupStats cleanup() {
        if (!Files.isDirectory(sharedRoot)) {
            return new CleanupStats(0, 0, 0, 0);
        }
        try {
            cleanupTemps();
            List<CacheFile> files = cacheFiles();
            long totalBytes = files.stream().mapToLong(CacheFile::size).sum();
            long deletedFiles = 0;
            long deletedBytes = 0;
            if (totalBytes > maxBytes) {
                for (CacheFile file : files.stream().sorted(Comparator.comparingLong(CacheFile::lastModifiedMillis)).toList()) {
                    if (totalBytes <= maxBytes) {
                        break;
                    }
                    try {
                        Files.deleteIfExists(file.path());
                        totalBytes -= file.size();
                        deletedFiles++;
                        deletedBytes += file.size();
                    } catch (IOException e) {
                        log.debug("failed to evict cache file {}", file.path(), e);
                    }
                }
            }
            return new CleanupStats(totalBytes, maxBytes, deletedFiles, deletedBytes);
        } catch (IOException e) {
            log.warn("failed to clean local remote cache under {}", sharedRoot, e);
            return new CleanupStats(0, maxBytes, 0, 0);
        }
    }

    private List<CacheFile> cacheFiles() throws IOException {
        try (var stream = Files.walk(sharedRoot)) {
            return stream.filter(Files::isRegularFile)
                    .filter(path -> Hierarchy.DATA.path.equals(parentName(path)))
                    .map(this::cacheFile)
                    .filter(file -> file.size() > 0)
                    .toList();
        }
    }

    private CacheFile cacheFile(Path path) {
        try {
            return new CacheFile(
                    path,
                    Files.size(path),
                    Files.getLastModifiedTime(path).toMillis()
            );
        } catch (IOException e) {
            return new CacheFile(path, 0, Long.MAX_VALUE);
        }
    }

    private void cleanupTemps() throws IOException {
        Instant cutoff = Instant.now().minus(tempMaxAge);
        try (var stream = Files.walk(sharedRoot)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> Hierarchy.TEMP.path.equals(parentName(path)))
                    .forEach(path -> deleteOldTemp(path, cutoff));
        }
    }

    private void deleteOldTemp(Path path, Instant cutoff) {
        try {
            if (Files.getLastModifiedTime(path).toInstant().isBefore(cutoff)) {
                Files.deleteIfExists(path);
            }
        } catch (IOException e) {
            log.debug("failed to delete cache temp file {}", path, e);
        }
    }

    private String parentName(Path path) {
        Path parent = path.getParent();
        return parent == null ? "" : parent.getFileName().toString();
    }

    record CleanupStats(long totalBytes, long maxBytes, long deletedFiles, long deletedBytes) {
    }

    private record CacheFile(Path path, long size, long lastModifiedMillis) {
    }
}
