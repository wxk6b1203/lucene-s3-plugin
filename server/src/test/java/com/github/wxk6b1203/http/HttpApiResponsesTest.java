package com.github.wxk6b1203.http;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HttpApiResponsesTest {
    @Test
    void luceneSnapshotUnavailableIsServiceUnavailable() {
        assertEquals(503, HttpApiResponses.status(new IOException(
                "no segments* file found in S3CachingDirectory: files: []"
        )));
    }

    @Test
    void ordinaryMissingIndexStaysNotFound() {
        assertEquals(404, HttpApiResponses.status(new IllegalArgumentException("index not found: books")));
    }

    @Test
    void localResourceExhaustionIsServiceUnavailable() {
        assertEquals(503, HttpApiResponses.status(new IOException("Too many open files")));
    }
}
