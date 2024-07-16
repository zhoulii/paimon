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

package org.apache.paimon.io;

import org.apache.paimon.memory.MemorySegment;

import java.io.DataOutput;
import java.io.IOException;

/**
 * DataOutput: 向输出流写出各种类型的对象. DataOutputView: DataOutput 的增强，能输出 DataInputView 到输出流，这里用于输出到
 * MemorySegment.
 *
 * <p>This interface defines a view over some memory that can be used to sequentially write contents
 * to the memory. The view is typically backed by one or more {@link MemorySegment}.
 */
public interface DataOutputView extends DataOutput {

    /**
     * DataOutputView 也可以理解为一块输出内存视图，后端可以由 MemorySegment 支撑，skipBytesToWrite 表示向 MemorySegment 写入时，
     * 跳过一段范围的内存。
     *
     * <p>Skips {@code numBytes} bytes memory. If some program reads the memory that was skipped
     * over, the results are undefined.
     *
     * @param numBytes The number of bytes to skip.
     * @throws IOException Thrown, if any I/O related problem occurred such that the view could not
     *     be advanced to the desired position.
     */
    void skipBytesToWrite(int numBytes) throws IOException;

    /**
     * 写出输入内存视图中的部分内容.
     *
     * <p>Copies {@code numBytes} bytes from the source to this view.
     *
     * @param source The source to copy the bytes from.
     * @param numBytes The number of bytes to copy.
     * @throws IOException Thrown, if any I/O related problem occurred, such that either the input
     *     view could not be read, or the output could not be written.
     */
    void write(DataInputView source, int numBytes) throws IOException;
}
