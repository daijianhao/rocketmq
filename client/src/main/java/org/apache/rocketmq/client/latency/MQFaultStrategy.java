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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    /**
     * 以下两个列表是一一对应的，当发送时长低于100ms时，设置broker不可用时长为0，之后依次增加，
     * 如果超过15秒，则有10分钟不可用。可以看到如果上次发送失败的话，也是10分钟不可用，如果重试肯定不会选择相同的broker。
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 选择Queue
     * Queue的选取是发送端实现负责均衡的核心，根据client是否开启了延时容错，实现轮询和加可用性轮询的选取策略。
     * <p>
     * Producer为每个topic缓存了一个全局index，每次发送之后+1,然后从所有queue列表中选择index位置上的queue，这样就实现了轮询的效果。
     * 如果开启了延时容错，则会考虑broker的可用性：
     * 第1) 2)步：根据全局index找到queue
     * 第3)步：如果根据延时容错判断queue所在的broker当前可用，并且是第一次发送，或者是重试并且和上次用的broker是同一个，
     * 则使用这个queue。这里面有两个逻辑，一个是broker的可用性是如何判断的，这个我们下面说；
     * 第二个是为什么重试的时候要选上次的broker，下面说下我的理解。
     * <p>
     * 由前面的发送逻辑中的第6和8步知道，有两种情况会重试，一种是broker返回处理成功但是store失败，一种是broker返回失败。
     * 对于返回失败的情况，其实会直接更新broker为短时不可用状态,这个在第一个if条件就已经通不过了；而对于store失败的情况，说明broker当前是正常的，重发还是发给同一个broker有利于防止消息重复。
     * <p>
     * 第4)步：如果将所有queue按照第3)步的情况过一遍，发现都不符合条件，则从所有broker中选择一个相对好的。
     * 第5)步：如果第4不中的broker不支持写入，则跟未开启延时容错一样的逻辑，直接选下一个queue
     * Broker延时控制逻辑
     * 由上面的queue的选择策略可以知道，queue的选择除了轮询以外，就是根据Broker的可用性。回看下消息发送的第6步和第8步，
     * 在消息发送后会更新时间和发送状态到MQFaultStrategy中
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        //开启了延时容错
        if (this.sendLatencyFaultEnable) {
            try {
                //1、首先获取上次使用的Queue index+1
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    //取模
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    //2、找到index对应的queue
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    //3、如果queue对应的broker可用，则使用该broker
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        /**
                         * 由前面的发送逻辑中的第6和8步知道，有两种情况会重试，一种是broker返回处理成功但是store失败，一种是broker返回失败。
                         * 对于返回失败的情况，其实会直接更新broker为短时不可用状态,这个在第一个if条件就已经通不过了；
                         * 而对于store失败的情况，说明broker当前是正常的，重发还是发给同一个broker有利于防止消息重复。
                         *  这便是延时容错
                         */
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }
                //4、如果上一步没找个合适的broker，则从所有的broker中选择一个相对合适的，并且broker是可写的
                //如果将所有queue按照第3)步的情况过一遍，发现都不符合条件，则从所有broker中选择一个相对好的
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            //5、如果以上都没找到，则直接按顺序选择下一个
            //如果第4不中的broker不支持写入，则跟未开启延时容错一样的逻辑，直接选下一个queue
            return tpInfo.selectOneMessageQueue();
        }
        //6、未开启延时容错，直接按顺序选下一个与lastBrokerName不一样的
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            //1、根据发送结果，计算broker不可用时长
            /**
             * 根据上次消息发送时长和结果，计算Broker应该多长时间不可用，如果上次发送失败的话，发送时长按30秒计算。
             */
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            //2、更新Broker不可用时长
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
