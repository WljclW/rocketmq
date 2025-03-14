/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import org.apache.rocketmq.store.logfile.MappedFile;

/**
 * 【总述】一种抽象，用于 存储和管理 内存映射文件缓冲区的结果
 * 类的作用：
 *      用于封装从内存映射文件中读取的数据缓冲区。。。。包含了对内存映射文件缓冲区的操作方法
 * */
public class SelectMappedBufferResult {
    /*读取数据的起始偏移量，是全局偏移量*/
    private final long startOffset;
    /*表示实际读取到的数据缓冲区，通常是通过内存映射文件（Memory-Mapped File）技术创建的 MappedByteBuffer;
    * 【作用】提供对读取到数据的访问接口
    * 【内容】比如全局偏移量startOffset对应的mappedFile是mappedFile1，则byteBuffer就是mappedFile1中从
    *       startOffset(准确的说应该是startOffset % mappedFileSize————这个才是全局偏移在mappedFile的
    *       相对偏移)到这个mappedFile结束的缓冲区片段*/
    private final ByteBuffer byteBuffer;
    /*读取到数据的大小，单位字节。同时也暗示了byteBuffer的有效长度*/
    private int size;
    /*表示与当前读取结果关联的内存映射文件对象。(this.byteBuffer中的数据就是this.mappedFile的一部分数据)
    用途 : 用于管理底层的内存映射资源，确保在使用完缓冲区后能够正确释放资源。*/
    protected MappedFile mappedFile;
    /*标识当前读取到的结果是不是已经在缓存*/
    private boolean isInCache = true;

    public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getSize() {
        return size;
    }

    public void setSize(final int s) {
        this.size = s;
        this.byteBuffer.limit(this.size);
    }

    public MappedFile getMappedFile() {
        return mappedFile;
    }

    public synchronized void release() {
        if (this.mappedFile != null) {
            this.mappedFile.release();
            this.mappedFile = null;
        }
    }
    public synchronized boolean hasReleased() {
        return this.mappedFile == null;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public boolean isInMem() {
        if (mappedFile == null) {
            return true;
        }
        long pos = startOffset - mappedFile.getFileFromOffset();
        return mappedFile.isLoaded(pos, size);
    }

    public boolean isInCache() {
        return isInCache;
    }

    public void setInCache(boolean inCache) {
        isInCache = inCache;
    }
}
