package com.github.wxk6b1203.store.directory;

import org.apache.lucene.util.BitUtil;
import org.jspecify.annotations.NonNull;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.CRC32;

public class XBufferedOutputStream extends OutputStream {
    /**
     * buffer where data is stored.
     */
    protected byte[] buf;
    /**
     * The number of valid bytes in the buffer.
     */
    protected int count;

    CRC32 crc = new CRC32();

    public XBufferedOutputStream() {
        this(128);
    }

    public XBufferedOutputStream(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: "
                    + size);
        }
        buf = new byte[size];
    }

    public void writeShort(short i) {
        ensureCapacity(count + Short.BYTES);
        BitUtil.VH_LE_SHORT.set(buf, count, i);
        crc.update(buf, count, Short.BYTES);
        count += Short.BYTES;
    }

    public void writeInt(int i) {
        ensureCapacity(count + Integer.BYTES);
        BitUtil.VH_LE_INT.set(buf, count, i);
        crc.update(buf, count, Integer.BYTES);
        count += Integer.BYTES;
    }

    public void writeLong(long l) {
        ensureCapacity(count + Long.BYTES);
        BitUtil.VH_LE_LONG.set(buf, count, l);
        crc.update(buf, count, Long.BYTES);
        count += Long.BYTES;
    }

    private void ensureCapacity(int minCapacity) {
        minCapacity = minCapacity < 0 ? 1 : minCapacity;
        int oldCapacity = buf.length;
        if (minCapacity <= oldCapacity) {
            return;
        }

        // 1.5x 增长，尽量减少扩容次数（也可改为 2x）
        int newCapacity = oldCapacity + (oldCapacity >> 1);

        // 保证至少满足 minCapacity
        if (newCapacity < minCapacity) {
            newCapacity = minCapacity;
        }

        // 溢出保护：数组最大长度保守取 Integer.MAX_VALUE - 8（与 JDK 常见实现一致）
        final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
        if (newCapacity > MAX_ARRAY_SIZE) {
            newCapacity = minCapacity;
        }

        buf = Arrays.copyOf(buf, newCapacity);
    }

    @Override
    public void write(int b) {
        ensureCapacity(count + 1);
        buf[count] = (byte) b;
        count += 1;
    }

    @Override
    public void write(byte @NonNull [] b, int off, int len) {
        ensureCapacity(count + len);
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }

    public int size() {
        return count;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buf, count);
    }

    @Override
    public void close() {
        buf = new byte[32];
        count = 0;
    }

    public void writeBytes(byte[] b) {
        write(b, 0, b.length);
    }

    public long checksum() {
        return crc.getValue();
    }
}
