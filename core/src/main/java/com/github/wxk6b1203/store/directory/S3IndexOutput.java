package com.github.wxk6b1203.store.directory;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class S3IndexOutput extends IndexOutput {
    private final S3Client s3Client;
    private final XBufferedOutputStream bufferOutputStream;
    private final CRC32 crc = new CRC32();
    private final CheckedOutputStream checkedOutputStream;

    /**
     * Sole constructor. resourceDescription should be non-null, opaque string describing this
     * resource; it's returned from {@link #toString}.
     *
     * @param resourceDescription
     * @param name
     */
    protected S3IndexOutput(String resourceDescription, String name, S3Client s3Client) {
        super(resourceDescription, name);
        this.s3Client = s3Client;
        this.bufferOutputStream = new XBufferedOutputStream();
        this.checkedOutputStream = new CheckedOutputStream(this.bufferOutputStream, crc);
    }


    @Override
    public void close() throws IOException {

    }

    @Override
    public long getFilePointer() {
        return 0;
    }

    @Override
    public long getChecksum() throws IOException {
        return 0;
    }

    @Override
    public void writeByte(byte b) throws IOException {

    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {

    }
}
