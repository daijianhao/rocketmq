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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;

import java.util.List;

/**
 * 消息消费服务
 */
public interface ConsumeMessageService {
    /**
     * 启动
     */
    void start();

    void shutdown(long awaitTerminateMillis);

    /**
     * 更新核心线程数
     *
     * @param corePoolSize
     */
    void updateCorePoolSize(int corePoolSize);

    /**
     * 增加核心线程数
     */
    void incCorePoolSize();

    /**
     * 减少核心线程数
     */
    void decCorePoolSize();

    /**
     * 获取核心线程数
     *
     * @return
     */
    int getCorePoolSize();

    /**
     * 直接消费消息
     *
     * @param msg
     * @param brokerName
     * @return
     */
    ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String brokerName);

    /**
     * 提交消费请求
     *
     * @param msgs
     * @param processQueue
     * @param messageQueue
     * @param dispathToConsume
     */
    void submitConsumeRequest(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final boolean dispathToConsume);
}
