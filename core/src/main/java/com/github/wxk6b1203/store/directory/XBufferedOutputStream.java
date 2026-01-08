package com.github.wxk6b1203.store.directory;

import org.apache.lucene.util.BitUtil;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class XBufferedOutputStream extends ByteArrayOutputStream {
    public void writeShort(short i) {
        ensureCapacity(count + 1);
        BitUtil.VH_LE_SHORT.set(buf, count, i);
        count += Short.BYTES;
    }
    public void writeInt(int i) {
        ensureCapacity(count + 1);
        BitUtil.VH_LE_INT.set(buf, count, i);
        count += Integer.BYTES;
    }
    public void writeLong(long l) {
        ensureCapacity(count + 1);
        BitUtil.VH_LE_LONG.set(buf, count, l);
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
}
