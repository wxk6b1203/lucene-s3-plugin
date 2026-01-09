package com.github.wxk6b1203.store.directory;

import com.github.wxk6b1203.common.Common;
import org.apache.lucene.store.IndexOutput;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.OutputStream;

public class S3IndexOutput extends IndexOutput {
    private final XBufferedOutputStream bufferedOutputStream;
    private boolean flushedOnClose = false;

    /**
     * Sole constructor. resourceDescription should be non-null, opaque string describing this
     * resource; it's returned from {@link #toString}.
     *
     * @param resourceDescription description
     * @param bucket              S3 bucket name
     * @param index               index name
     * @param name                file name
     * @param s3Client            S3 client
     *
     */
    protected S3IndexOutput(String resourceDescription, String bucket, String index, String name, S3Client s3Client) {
        super(resourceDescription, name);
        this.bufferedOutputStream = new XBufferedOutputStream(bucket, index, name, s3Client) {
            @Override
            public void close() {
                this.flush();
                super.close();
            }
        };
    }


    @Override
    public void close() throws IOException {
        try (final OutputStream out = bufferedOutputStream) {
            if (!flushedOnClose) {
                flushedOnClose = true;
                out.flush();
            }
        }
    }

    @Override
    public long getFilePointer() {
        return bufferedOutputStream.size();
    }

    @Override
    public long getChecksum() {
        return bufferedOutputStream.checksum();
    }

    @Override
    public void writeByte(byte b) {
        bufferedOutputStream.write(b);
    }

    @Override
    public void writeInt(int i) {
        bufferedOutputStream.writeInt(i);
    }

    @Override
    public void writeShort(short i) {
        bufferedOutputStream.writeShort(i);
    }

    @Override
    public void writeLong(long i) {
        bufferedOutputStream.writeLong(i);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        bufferedOutputStream.write(b, offset, length);
    }
}
