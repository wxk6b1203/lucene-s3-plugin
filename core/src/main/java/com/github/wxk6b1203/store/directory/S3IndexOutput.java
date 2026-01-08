package com.github.wxk6b1203.store.directory;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.util.BitUtil;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class S3IndexOutput extends IndexOutput {
    private final S3Client s3Client;
    private final XBufferedOutputStream bufferedOutputStream;

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
        this.bufferedOutputStream = new XBufferedOutputStream();
    }


    @Override
    public void close() throws IOException {

    }

    @Override
    public long getFilePointer() {
        return bufferedOutputStream.size();
    }

    @Override
    public long getChecksum() throws IOException {
        return bufferedOutputStream.checksum();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        bufferedOutputStream.write(b);
    }

    @Override
    public void writeInt(int i) throws IOException {
        bufferedOutputStream.writeInt(i);
    }

    @Override
    public void writeShort(short i) throws IOException {
        bufferedOutputStream.writeShort(i);
    }

    @Override
    public void writeLong(long i) throws IOException {
        bufferedOutputStream.writeLong(i);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {

    }
}
