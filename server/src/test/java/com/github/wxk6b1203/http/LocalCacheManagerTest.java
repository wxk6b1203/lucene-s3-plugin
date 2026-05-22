package com.github.wxk6b1203.http;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalCacheManagerTest {
    @TempDir
    Path tempDir;

    @Test
    void cleanupEvictsLeastRecentlyUsedCacheFilesAndOldTemps() throws Exception {
        Path data = tempDir.resolve("_shared").resolve("books__shard_0").resolve("_data");
        Path temp = tempDir.resolve("_shared").resolve("books__shard_0").resolve("_temp");
        Files.createDirectories(data);
        Files.createDirectories(temp);
        Path oldCache = data.resolve("old.fdt");
        Path newCache = data.resolve("new.fdt");
        Path oldTemp = temp.resolve("download.tmp");
        Files.writeString(oldCache, "1234");
        Files.writeString(newCache, "5678");
        Files.writeString(oldTemp, "temp");
        Files.setLastModifiedTime(oldCache, FileTime.from(Instant.now().minus(Duration.ofHours(2))));
        Files.setLastModifiedTime(newCache, FileTime.from(Instant.now()));
        Files.setLastModifiedTime(oldTemp, FileTime.from(Instant.now().minus(Duration.ofHours(2))));

        LocalCacheManager manager = new LocalCacheManager(tempDir, 4, Duration.ofMinutes(30));
        LocalCacheManager.CleanupStats stats = manager.cleanup();

        assertFalse(Files.exists(oldCache));
        assertTrue(Files.exists(newCache));
        assertFalse(Files.exists(oldTemp));
        assertTrue(stats.deletedFiles() >= 1);
    }
}
