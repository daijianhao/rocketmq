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
package org.apache.rocketmq.store.index;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

/**
 * 表示Index File
 * <p>
 * <p>
 * 负责处理与IndexFile相关操作的类
 * <p>
 * IndexFile：IndexFile（索引文件）提供了一种可以通过key或时间区间来查询消息的方法。Index文件的存储位置是：$HOME \store\index${fileName}，
 * 文件名fileName是以创建时的时间戳命名的，固定的单个IndexFile文件大小约为400M，一个IndexFile可以保存 2000W个索引，
 * IndexFile的底层存储设计为在文件系统中实现HashMap结构，故rocketmq的索引文件其底层实现为hash索引。
 * <p>
 * <p>
 * todo https://blog.csdn.net/wb_snail/article/details/106236898  index文件结构
 * <p>
 * 由以下几部分组成：
 * <p>
 * 1.IndexHeader ,共40个字节
 * <p>
 * beginTimestamp：该indexFile对应的第一条消息存储的时间
 * endTimestamp：该indexFile对应的最后一条消息存储的时间
 * beginPhyOffset：第一条消息在CommitLog中的偏移量
 * endPhyOffset：最后一条消息在CommitLog总的偏移量
 * hashSlotCount： 已填充的hash slot数
 * indexCount：该indexFile包含的索引个数
 * -------------------------------------------------------------------------------------------------------------------------------------------------------
 * |                        |                        |                        |                        |                        |                        |
 * | beginTimestamp(8byte)  |   endTimestamp(8byte)  | beginPhyOffset(8byte)  |   endPhyOffset(8byte)  |  hashSlotCount(4byte)  |   indexCount(4byte)    |
 * |                        |                        |                        |                        |                        |                        |
 * -------------------------------------------------------------------------------------------------------------------------------------------------------
 * 2. slot槽位
 * <p>
 * 默认500万个，每个4byte
 * 每个slot中存一个int表示当前slot下最新的index的序号，可以算出该index的文件位置
 * ------------------------------------------------------------------------------------------------------------------------------
 * |                        |                        |                        |                        |                        |
 * |          slot1         |           slot2        |          slot3         |             ...        |        slot500w        |
 * |                        |                        |                        |                        |                        |
 * ------------------------------------------------------------------------------------------------------------------------------
 * <p>
 * 3.每个index的结构
 * <p>
 * 每个固定大小20byte，紧邻着500wslot之后，最大500w*4个，也就是每个槽平均4个
 * <p>
 * 1.hashKey:索引key的hash值
 * 2.索引消息在CommitLog中的偏移量
 * 3.该索引对应消息的存储时间与当前索引文件的第一条消息的存储时间的时间差
 * 4.当前slot下，当前index的前一个index的序号
 * <p>
 * 索引查找时用hashKey和timeDiff比较该索引与传入的key和时间是否匹配
 * -----------------------------------------------------------------------------------------------------
 * |                        |                        |                        |                        |
 * |    hashKey(4byte)      |    phyOffset(8byte)    |   timeDiff(4byte)      |    preindexNo(4byte)   |
 * |                        |                        |                        |                        |
 * -----------------------------------------------------------------------------------------------------
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    /**
     * hash槽数，默认500w
     */
    private final int hashSlotNum;
    private final int indexNum;

    /**
     * 文件映射
     */
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
                     final long endPhyOffset, final long endTimestamp) throws IOException {
        //文件总大小 =  索引头 + hash槽数 *slot大小  + 索引数 * 索引大
        int fileTotalSize =
                IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        //进行文件映射
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        //获取文件channel
        this.fileChannel = this.mappedFile.getFileChannel();
        //获取mappedBuffer
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            //设置第一条消息在CommitLog中的偏移量
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            //设置最后一条消息在CommitLog中的偏移量
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            //设置indexFile对应的第一条消息存储的时间
            this.indexHeader.setBeginTimestamp(endTimestamp);
            //设置indexFile对应的最后一条消息存储的时间
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        //判断header中记录的索引数量是否 >= indexNum,true满了
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 将消息记录在索引文件中
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {//如果还能放下
            int keyHash = indexKeyHashMethod(key);
            //除余法，计算slot
            int slotPos = keyHash % this.hashSlotNum;
            //计算被选中的slot偏移地址
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                //取出该slot下最新的index序号
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    //置为无效
                    slotValue = invalidIndex;
                }
                //计算时间差
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                //下一个开始放置索引的位置
                int absIndexPos =
                        IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + this.indexHeader.getIndexCount() * indexSize;

                //将索引数据放入

                //放入hashKey
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                //放入在CommitLog中的物理偏移量
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                //放入和第一条消息的时间差
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                //放入上一个index的位置
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                //todo 这里虽然放入的是索引的个数，但因为index是一次放入的，所以也就相当于是当前slot最新index的位置。因为位置可以推断出来
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    //如果是第一个index,则设置初始偏移量和开始时间
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                if (invalidIndex == slotValue) {
                    //如果slot中保存的index序号是0，则已填充的slot数+1
                    this.indexHeader.incHashSlotCount();
                }
                //index数+1
                this.indexHeader.incIndexCount();
                //更新最后一条消息的偏移量
                this.indexHeader.setEndPhyOffset(phyOffset);
                //更新最后一条消息的保存时间
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                    + "; index max num = " + this.indexNum);
        }

        return false;
    }

    /**
     * 计算HashKey
     *
     * @param key
     * @return
     */
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                        || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos =
                                IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                        + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                                || prevIndexRead > this.indexHeader.getIndexCount()
                                || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
