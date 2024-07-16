/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.memory;

import org.apache.paimon.annotation.Public;

import javax.annotation.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.Objects;

import static org.apache.paimon.memory.MemoryUtils.getByteBufferAddress;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class represents a piece of memory.
 *
 * @since 0.4.0
 */
@Public
public final class MemorySegment {

    public static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

    // 表示 Java 堆中 byte 数组的实际数据部分相对于数组对象头的偏移量，堆中数组 buffer 的数据部分真实地址可以视为:
    // BASE_ARRAY_ADDRESS(buffer) + BYTE_ARRAY_BASE_OFFSET + index
    // index 表示数组下标
    public static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    public static final boolean LITTLE_ENDIAN =
            (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

    @Nullable private byte[] heapMemory;

    @Nullable private ByteBuffer offHeapBuffer;

    // 对于堆内存和非堆内存，address 具有不同含义
    // 堆内存：address 表示 Java 堆中 byte 数组的实际数据部分相对于数组对象头的偏移量
    // 非堆内存：address 表示数组的真实内存地址
    private long address;

    private final int size;

    private MemorySegment(
            @Nullable byte[] heapMemory,
            @Nullable ByteBuffer offHeapBuffer,
            long address,
            int size) {
        this.heapMemory = heapMemory;
        this.offHeapBuffer = offHeapBuffer;
        this.address = address;
        this.size = size;
    }

    public static MemorySegment wrap(byte[] buffer) {
        return new MemorySegment(buffer, null, BYTE_ARRAY_BASE_OFFSET, buffer.length);
    }

    public static MemorySegment wrapOffHeapMemory(ByteBuffer buffer) {
        return new MemorySegment(null, buffer, getByteBufferAddress(buffer), buffer.capacity());
    }

    /**
     * 分配对上内存.
     *
     * @param size The size of the memory segment to allocate.
     * @return A new memory segment of the given size.
     */
    public static MemorySegment allocateHeapMemory(int size) {
        return wrap(new byte[size]);
    }

    /**
     * 分配对外内存.
     *
     * @param size The size of the memory segment to allocate.
     * @return A new memory segment of the given size.
     */
    public static MemorySegment allocateOffHeapMemory(int size) {
        return wrapOffHeapMemory(ByteBuffer.allocateDirect(size));
    }

    // 数据大小，多少个 byte.
    public int size() {
        return size;
    }

    public boolean isOffHeap() {
        return heapMemory == null;
    }

    /**
     * 获取堆上内存数组.
     *
     * @return The byte array that backs this memory segment.
     */
    public byte[] getArray() {
        if (heapMemory != null) {
            return heapMemory;
        } else {
            throw new IllegalStateException("Memory segment does not represent heap memory");
        }
    }

    /**
     * 截取部分数据，作为 ByteBuffer 对象返回.
     *
     * @param offset 偏移量
     * @param length 长度
     * @return ByteBuffer
     */
    public ByteBuffer wrap(int offset, int length) {
        return wrapInternal(offset, length);
    }

    private ByteBuffer wrapInternal(int offset, int length) {
        if (heapMemory != null) {
            return ByteBuffer.wrap(heapMemory, offset, length);
        } else {
            try {
                ByteBuffer wrapper = Objects.requireNonNull(offHeapBuffer).duplicate();
                wrapper.limit(offset + length);
                wrapper.position(offset);
                return wrapper;
            } catch (IllegalArgumentException e) {
                throw new IndexOutOfBoundsException();
            }
        }
    }

    public byte get(int index) {
        // address + index 就是第 index 字段数据的真实地址偏移量
        // 加上 heapMemory 的起始地址，就是字段真实地址
        return UNSAFE.getByte(heapMemory, address + index);
    }

    public void put(int index, byte b) {
        UNSAFE.putByte(heapMemory, address + index, b);
    }

    /**
     * 从内存中读取数据到 dst 数组中.
     *
     * @param index 读取数据的起始位置
     * @param dst 目标数组
     */
    public void get(int index, byte[] dst) {
        get(index, dst, 0, dst.length);
    }

    /**
     * 拷贝数据到内存中.
     *
     * @param index 拷贝数据的起始位置
     * @param src 要拷贝的数组
     */
    public void put(int index, byte[] src) {
        put(index, src, 0, src.length);
    }

    /**
     * 从内存中读取数据到 dst 数组中.
     *
     * @param index 读取数据的起始位置
     * @param dst 目标数组
     * @param offset 目标数组的偏移量
     * @param length 读取数据的长度
     */
    public void get(int index, byte[] dst, int offset, int length) {
        // check the byte array offset and length and the status
        // offset 和 length 不能为负数
        // offset + length 不能大于整数的最大值
        // dst.length - offset 的长度要大于要复制到长度 length
        if ((offset | length | (offset + length) | (dst.length - (offset + length))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        UNSAFE.copyMemory(
                heapMemory, address + index, dst, BYTE_ARRAY_BASE_OFFSET + offset, length);
    }

    /**
     * 拷贝数据到内存中.
     *
     * @param index 拷贝数据的起始位置
     * @param src 要拷贝的数组
     * @param offset 要拷贝的数组的偏移量
     * @param length 拷贝数据的长度
     */
    public void put(int index, byte[] src, int offset, int length) {
        // check the byte array offset and length
        if ((offset | length | (offset + length) | (src.length - (offset + length))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        UNSAFE.copyMemory(
                src, BYTE_ARRAY_BASE_OFFSET + offset, heapMemory, address + index, length);
    }

    public boolean getBoolean(int index) {
        return get(index) != 0;
    }

    public void putBoolean(int index, boolean value) {
        put(index, (byte) (value ? 1 : 0));
    }

    public char getChar(int index) {
        return UNSAFE.getChar(heapMemory, address + index);
    }

    public char getCharLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getChar(index);
        } else {
            return Character.reverseBytes(getChar(index));
        }
    }

    public char getCharBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Character.reverseBytes(getChar(index));
        } else {
            return getChar(index);
        }
    }

    public void putChar(int index, char value) {
        UNSAFE.putChar(heapMemory, address + index, value);
    }

    public void putCharLittleEndian(int index, char value) {
        if (LITTLE_ENDIAN) {
            putChar(index, value);
        } else {
            putChar(index, Character.reverseBytes(value));
        }
    }

    public void putCharBigEndian(int index, char value) {
        if (LITTLE_ENDIAN) {
            putChar(index, Character.reverseBytes(value));
        } else {
            putChar(index, value);
        }
    }

    public short getShort(int index) {
        return UNSAFE.getShort(heapMemory, address + index);
    }

    public short getShortLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getShort(index);
        } else {
            return Short.reverseBytes(getShort(index));
        }
    }

    public short getShortBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Short.reverseBytes(getShort(index));
        } else {
            return getShort(index);
        }
    }

    public void putShort(int index, short value) {
        UNSAFE.putShort(heapMemory, address + index, value);
    }

    public void putShortLittleEndian(int index, short value) {
        if (LITTLE_ENDIAN) {
            putShort(index, value);
        } else {
            putShort(index, Short.reverseBytes(value));
        }
    }

    public void putShortBigEndian(int index, short value) {
        if (LITTLE_ENDIAN) {
            putShort(index, Short.reverseBytes(value));
        } else {
            putShort(index, value);
        }
    }

    public int getInt(int index) {
        return UNSAFE.getInt(heapMemory, address + index);
    }

    public int getIntLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getInt(index);
        } else {
            return Integer.reverseBytes(getInt(index));
        }
    }

    public int getIntBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Integer.reverseBytes(getInt(index));
        } else {
            return getInt(index);
        }
    }

    public void putInt(int index, int value) {
        UNSAFE.putInt(heapMemory, address + index, value);
    }

    public void putIntLittleEndian(int index, int value) {
        if (LITTLE_ENDIAN) {
            putInt(index, value);
        } else {
            putInt(index, Integer.reverseBytes(value));
        }
    }

    public void putIntBigEndian(int index, int value) {
        if (LITTLE_ENDIAN) {
            putInt(index, Integer.reverseBytes(value));
        } else {
            putInt(index, value);
        }
    }

    public long getLong(int index) {
        return UNSAFE.getLong(heapMemory, address + index);
    }

    public long getLongLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getLong(index);
        } else {
            return Long.reverseBytes(getLong(index));
        }
    }

    public long getLongBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Long.reverseBytes(getLong(index));
        } else {
            return getLong(index);
        }
    }

    public void putLong(int index, long value) {
        UNSAFE.putLong(heapMemory, address + index, value);
    }

    public void putLongLittleEndian(int index, long value) {
        if (LITTLE_ENDIAN) {
            putLong(index, value);
        } else {
            putLong(index, Long.reverseBytes(value));
        }
    }

    public void putLongBigEndian(int index, long value) {
        if (LITTLE_ENDIAN) {
            putLong(index, Long.reverseBytes(value));
        } else {
            putLong(index, value);
        }
    }

    public float getFloat(int index) {
        return Float.intBitsToFloat(getInt(index));
    }

    public float getFloatLittleEndian(int index) {
        return Float.intBitsToFloat(getIntLittleEndian(index));
    }

    public float getFloatBigEndian(int index) {
        return Float.intBitsToFloat(getIntBigEndian(index));
    }

    public void putFloat(int index, float value) {
        putInt(index, Float.floatToRawIntBits(value));
    }

    public void putFloatLittleEndian(int index, float value) {
        putIntLittleEndian(index, Float.floatToRawIntBits(value));
    }

    public void putFloatBigEndian(int index, float value) {
        putIntBigEndian(index, Float.floatToRawIntBits(value));
    }

    public double getDouble(int index) {
        return Double.longBitsToDouble(getLong(index));
    }

    public double getDoubleLittleEndian(int index) {
        return Double.longBitsToDouble(getLongLittleEndian(index));
    }

    public double getDoubleBigEndian(int index) {
        return Double.longBitsToDouble(getLongBigEndian(index));
    }

    public void putDouble(int index, double value) {
        putLong(index, Double.doubleToRawLongBits(value));
    }

    public void putDoubleLittleEndian(int index, double value) {
        putLongLittleEndian(index, Double.doubleToRawLongBits(value));
    }

    public void putDoubleBigEndian(int index, double value) {
        putLongBigEndian(index, Double.doubleToRawLongBits(value));
    }

    // -------------------------------------------------------------------------
    //                     Bulk Read and Write Methods
    // -------------------------------------------------------------------------

    public void get(DataOutput out, int offset, int length) throws IOException {
        // 读取当前 MemorySegment 中的内容，通过 out 写出
        if (heapMemory != null) {
            out.write(heapMemory, offset, length);
        } else {
            // 一次写出 8 个字节
            while (length >= 8) {
                out.writeLong(getLongBigEndian(offset));
                offset += 8;
                length -= 8;
            }

            // 剩余部分，一次写出 1 个字节
            while (length > 0) {
                out.writeByte(get(offset));
                offset++;
                length--;
            }
        }
    }

    public void put(DataInput in, int offset, int length) throws IOException {
        // 读取 in，写入 MemorySegment
        if (heapMemory != null) {
            // 从 offset 开始写入，写入长度为 length
            in.readFully(heapMemory, offset, length);
        } else {
            // 一次写入 8 个字节
            while (length >= 8) {
                putLongBigEndian(offset, in.readLong());
                offset += 8;
                length -= 8;
            }

            // 一次写入 1 个字节
            while (length > 0) {
                put(offset, in.readByte());
                offset++;
                length--;
            }
        }
    }

    public void get(int offset, ByteBuffer target, int numBytes) {
        // 拷贝 numBytes 个字节数据到 target
        // check the byte array offset and length
        if ((offset | numBytes | (offset + numBytes)) < 0) {
            throw new IndexOutOfBoundsException();
        }
        if (target.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }

        final int targetOffset = target.position();
        final int remaining = target.remaining();

        if (remaining < numBytes) {
            throw new BufferOverflowException();
        }

        if (target.isDirect()) {
            // 直接拷贝内存到 target
            // copy to the target memory directly
            final long targetPointer = getByteBufferAddress(target) + targetOffset;
            final long sourcePointer = address + offset;

            UNSAFE.copyMemory(heapMemory, sourcePointer, null, targetPointer, numBytes);
            target.position(targetOffset + numBytes);
        } else if (target.hasArray()) {
            // move directly into the byte array
            // 当 buffer 的底层是 array 时，buffer 的第一个元素并不一定是数组的第一个元素
            // buffer 的第一个元素对应 array 的下标为 target.arrayOffset()
            // 所以 buffer position 对应的 array 下标为 targetOffset + target.arrayOffset()
            get(offset, target.array(), targetOffset + target.arrayOffset(), numBytes);

            // this must be after the get() call to ensue that the byte buffer is not
            // modified in case the call fails
            target.position(targetOffset + numBytes);
        } else {
            // other types of byte buffers
            throw new IllegalArgumentException(
                    "The target buffer is not direct, and has no array.");
        }
    }

    public void put(int offset, ByteBuffer source, int numBytes) {
        // 拷贝 numBytes 个字节数据到 MemorySegment
        // check the byte array offset and length
        if ((offset | numBytes | (offset + numBytes)) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final int sourceOffset = source.position();
        final int remaining = source.remaining();

        if (remaining < numBytes) {
            throw new BufferUnderflowException();
        }

        if (source.isDirect()) {
            // copy to the target memory directly
            final long sourcePointer = getByteBufferAddress(source) + sourceOffset;
            final long targetPointer = address + offset;

            UNSAFE.copyMemory(null, sourcePointer, heapMemory, targetPointer, numBytes);
            source.position(sourceOffset + numBytes);
        } else if (source.hasArray()) {
            // move directly into the byte array
            put(offset, source.array(), sourceOffset + source.arrayOffset(), numBytes);

            // this must be after the get() call to ensue that the byte buffer is not
            // modified in case the call fails
            source.position(sourceOffset + numBytes);
        } else {
            // other types of byte buffers
            for (int i = 0; i < numBytes; i++) {
                put(offset++, source.get());
            }
        }
    }

    public void copyTo(int offset, MemorySegment target, int targetOffset, int numBytes) {
        // 拷贝数据到一个 MemorySegment
        final byte[] thisHeapRef = this.heapMemory;
        final byte[] otherHeapRef = target.heapMemory;
        final long thisPointer = this.address + offset;
        final long otherPointer = target.address + targetOffset;

        UNSAFE.copyMemory(thisHeapRef, thisPointer, otherHeapRef, otherPointer, numBytes);
    }

    public void copyToUnsafe(int offset, Object target, int targetPointer, int numBytes) {
        // 拷贝到一块 Unsafe 内存
        UNSAFE.copyMemory(this.heapMemory, this.address + offset, target, targetPointer, numBytes);
    }

    public void copyFromUnsafe(int offset, Object source, int sourcePointer, int numBytes) {
        // 从一块 Unsafe 内存拷贝
        UNSAFE.copyMemory(source, sourcePointer, this.heapMemory, this.address + offset, numBytes);
    }

    public int compare(MemorySegment seg2, int offset1, int offset2, int len) {
        // 比较一段长度
        while (len >= 8) {
            long l1 = this.getLongBigEndian(offset1);
            long l2 = seg2.getLongBigEndian(offset2);

            if (l1 != l2) {
                return (l1 < l2) ^ (l1 < 0) ^ (l2 < 0) ? -1 : 1;
            }

            offset1 += 8;
            offset2 += 8;
            len -= 8;
        }
        while (len > 0) {
            int b1 = this.get(offset1) & 0xff;
            int b2 = seg2.get(offset2) & 0xff;
            int cmp = b1 - b2;
            if (cmp != 0) {
                return cmp;
            }
            offset1++;
            offset2++;
            len--;
        }
        return 0;
    }

    public int compare(MemorySegment seg2, int offset1, int offset2, int len1, int len2) {
        // 比较一段长度
        final int minLength = Math.min(len1, len2);
        int c = compare(seg2, offset1, offset2, minLength);
        return c == 0 ? (len1 - len2) : c;
    }

    public void swapBytes(
            byte[] tempBuffer, MemorySegment seg2, int offset1, int offset2, int len) {
        // 和 seg2 交换一段内存
        if ((offset1 | offset2 | len | (tempBuffer.length - len)) >= 0) {
            final long thisPos = this.address + offset1;
            final long otherPos = seg2.address + offset2;

            // this -> temp buffer
            UNSAFE.copyMemory(this.heapMemory, thisPos, tempBuffer, BYTE_ARRAY_BASE_OFFSET, len);

            // other -> this
            UNSAFE.copyMemory(seg2.heapMemory, otherPos, this.heapMemory, thisPos, len);

            // temp buffer -> other
            UNSAFE.copyMemory(tempBuffer, BYTE_ARRAY_BASE_OFFSET, seg2.heapMemory, otherPos, len);
            return;
        }

        // index is in fact invalid
        throw new IndexOutOfBoundsException(
                String.format(
                        "offset1=%d, offset2=%d, len=%d, bufferSize=%d, address1=%d, address2=%d",
                        offset1, offset2, len, tempBuffer.length, this.address, seg2.address));
    }

    /**
     * Equals two memory segment regions.
     *
     * @param seg2 Segment to equal this segment with
     * @param offset1 Offset of this segment to start equaling
     * @param offset2 Offset of seg2 to start equaling
     * @param length Length of the equaled memory region
     * @return true if equal, false otherwise
     */
    public boolean equalTo(MemorySegment seg2, int offset1, int offset2, int length) {
        // 比较一段内存是否相等
        int i = 0;

        // we assume unaligned accesses are supported.
        // Compare 8 bytes at a time.
        while (i <= length - 8) {
            if (getLong(offset1 + i) != seg2.getLong(offset2 + i)) {
                return false;
            }
            i += 8;
        }

        // cover the last (length % 8) elements.
        while (i < length) {
            if (get(offset1 + i) != seg2.get(offset2 + i)) {
                return false;
            }
            i += 1;
        }

        return true;
    }

    /**
     * Get the heap byte array object.
     *
     * @return Return non-null if the memory is on the heap, and return null if the memory if off
     *     the heap.
     */
    public byte[] getHeapMemory() {
        return heapMemory;
    }
}
