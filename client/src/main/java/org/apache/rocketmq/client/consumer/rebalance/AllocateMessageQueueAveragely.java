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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 *
 * 平均分配队列算法实现类
 *
 * 系统默认使用AVG策略（AllocateMessageQueueAveragely），就是将该topic所有Queue按照broker和queueId从小到大做排列，
 * 按照consumer的数量平均分成几份。然后每个consumer分到一份，按照consumer排序后的顺序来领取
 *
 * RocketMQ提供其它的queue分配策略：
 *
 * AVG_BY_CIRCLE， 跟AVG类似，只是分到的queue不是连续的。比如一共12个Queue，3个consumer，则第一个consumer接收queue1，4，7，9的消息。
 * CONSISTENT_HASH，使用一致性hash算法来分配Queue，用户需自定义虚拟节点的数量
 * MACHINE_ROOM，将queue先按照broker划分几个computer room，不同的consumer只消费某几个broker上的消息
 * CONFIG,用户启动时指定消费哪些Queue的消息
 *
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        //先做一些参数检查
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            //出现bug，正常情况下是不存在的
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }
        //获取当前consumer的索引
        int index = cidAll.indexOf(currentCID);
        //取模
        int mod = mqAll.size() % cidAll.size();
        /*
         *   //AVG size计算方法，mq数量<=consumer数量，size=1，这种情况是很少的
         *   //否则size=mq数量/consumer数量，余数是几则前几个consumer的size+1,这样所有的queue都会有consumer消费
         */
        //todo 没太懂
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        //从第一个consumer开始分配，每个分avgSize个连续的Queue
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
