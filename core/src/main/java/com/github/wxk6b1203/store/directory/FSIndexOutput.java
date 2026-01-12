package com.github.wxk6b1203.store.directory;

import org.apache.lucene.store.OutputStreamIndexOutput;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FSIndexOutput extends OutputStreamIndexOutput {
    /**
     * The maximum chunk size is 8192 bytes, because file channel mallocs a native buffer outside of
     * stack if the write buffer size is larger.
     */
    static final int CHUNK_SIZE = 8192;

    public FSIndexOutput(Path absoluteFileName) throws IOException {
        this(absoluteFileName, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    FSIndexOutput(Path absoluteFileName, OpenOption... options) throws IOException {
        super(
                "FSIndexOutput(path=\"" + absoluteFileName  + "\")",
                absoluteFileName.getFileName().toString(),
                new FilterOutputStream(Files.newOutputStream(absoluteFileName, options)) {
                    // This implementation ensures, that we never write more than CHUNK_SIZE bytes:
                    @Override
                    public void write(byte[] b, int offset, int length) throws IOException {
                        while (length > 0) {
                            final int chunk = Math.min(length, CHUNK_SIZE);
                            out.write(b, offset, chunk);
                            length -= chunk;
                            offset += chunk;
                        }
                    }
                },
                CHUNK_SIZE);
    }
}