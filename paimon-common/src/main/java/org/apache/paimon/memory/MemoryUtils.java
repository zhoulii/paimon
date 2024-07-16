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

import org.apache.paimon.utils.Preconditions;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Utility class for memory operations. */
public class MemoryUtils {

    /** The "unsafe", which can be used to perform native memory accesses. */
    @SuppressWarnings({"restriction", "UseOfSunClasses"})
    public static final sun.misc.Unsafe UNSAFE = getUnsafe();

    /** The native byte order of the platform on which the system currently runs. */
    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

    // 获取 Buffer 的 address 字段的偏移量
    private static final long BUFFER_ADDRESS_FIELD_OFFSET =
            getClassFieldOffset(Buffer.class, "address");

    @SuppressWarnings("restriction")
    private static sun.misc.Unsafe getUnsafe() {
        try {
            Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            return (sun.misc.Unsafe) unsafeField.get(null);
        } catch (SecurityException e) {
            throw new Error(
                    "Could not access the sun.misc.Unsafe handle, permission denied by security manager.",
                    e);
        } catch (NoSuchFieldException e) {
            throw new Error("The static handle field in sun.misc.Unsafe was not found.", e);
        } catch (IllegalArgumentException e) {
            throw new Error("Bug: Illegal argument reflection access for static field.", e);
        } catch (IllegalAccessException e) {
            throw new Error("Access to sun.misc.Unsafe is forbidden by the runtime.", e);
        } catch (Throwable t) {
            throw new Error(
                    "Unclassified error while trying to access the sun.misc.Unsafe handle.", t);
        }
    }

    private static long getClassFieldOffset(
            @SuppressWarnings("SameParameterValue") Class<?> cl, String fieldName) {
        // 获取指定字段的内存便宜地址
        // 也就是存储这个对象时，这个字段在内存中的位置
        try {
            return UNSAFE.objectFieldOffset(cl.getDeclaredField(fieldName));
        } catch (SecurityException e) {
            throw new Error(
                    getClassFieldOffsetErrorMessage(cl, fieldName)
                            + ", permission denied by security manager.",
                    e);
        } catch (NoSuchFieldException e) {
            throw new Error(getClassFieldOffsetErrorMessage(cl, fieldName), e);
        } catch (Throwable t) {
            throw new Error(
                    getClassFieldOffsetErrorMessage(cl, fieldName) + ", unclassified error", t);
        }
    }

    private static String getClassFieldOffsetErrorMessage(Class<?> cl, String fieldName) {
        return "Could not get field '"
                + fieldName
                + "' offset in class '"
                + cl
                + "' for unsafe operations";
    }

    private static Class<?> getClassByName(
            @SuppressWarnings("SameParameterValue") String className) {
        // 如果方法被调用时传递的参数总是相同的值，编译器可能会触发警告，因为动态传递参数的行为似乎没有必要
        // 但是方法为了动态处理不同值而设计的，只是目前还没提现出来
        // 可以使用 @SuppressWarnings("SameParameterValue") 来抑制告警
        // public static void main(String[] args) {
        //     getClassByName("java.lang.String"); // 第一处调用，总是传递同一个值 "java.lang.String"
        //     getClassByName("java.lang.String"); // 第二处调用，总是传递同一个值 "java.lang.String"
        // }

        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new Error("Could not find class '" + className + "' for unsafe operations.", e);
        }
    }

    /**
     * 获取 ByteBuffer 的堆外内存地址.
     *
     * <p>Get native memory address wrapped by the given {@link ByteBuffer}.
     *
     * @param buffer {@link ByteBuffer} which wraps the native memory address to get
     * @return native memory address wrapped by the given {@link ByteBuffer}
     */
    static long getByteBufferAddress(ByteBuffer buffer) {
        Preconditions.checkNotNull(buffer, "buffer is null");
        Preconditions.checkArgument(
                buffer.isDirect(), "Can't get address of a non-direct ByteBuffer.");

        long offHeapAddress;
        try {
            offHeapAddress = UNSAFE.getLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET);
        } catch (Throwable t) {
            throw new Error("Could not access direct byte buffer address field.", t);
        }

        Preconditions.checkState(offHeapAddress > 0, "negative pointer or size");
        Preconditions.checkState(
                offHeapAddress < Long.MAX_VALUE - Integer.MAX_VALUE,
                "Segment initialized with too large address: "
                        + offHeapAddress
                        + " ; Max allowed address is "
                        + (Long.MAX_VALUE - Integer.MAX_VALUE - 1));

        return offHeapAddress;
    }

    /** Should not be instantiated. */
    private MemoryUtils() {}
}
