package com.github.wxk6b1203.store.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;

public final class FileChecksums {
    private FileChecksums() {
    }

    public static long crc32(Path path) throws IOException {
        CRC32 crc32 = new CRC32();
        byte[] buffer = new byte[8192];
        try (InputStream input = Files.newInputStream(path)) {
            int read;
            while ((read = input.read(buffer)) >= 0) {
                crc32.update(buffer, 0, read);
            }
        }
        return crc32.getValue();
    }
}
