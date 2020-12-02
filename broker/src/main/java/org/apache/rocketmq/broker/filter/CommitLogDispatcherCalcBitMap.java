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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.store.CommitLogDispatcher;
import org.apache.rocketmq.store.DispatchRequest;

import java.util.Collection;
import java.util.Iterator;

/**
 * Calculate bit map of filter.
 *
 * 将消息分发至bit map的分发器实现类
 *
 * 本类主要的作用就是将message与 预设的过滤条件进行匹配，通过bloom过滤器计算bitmap，然后设置到DispatchRequest的 bitMap 中，
 * 供后续ConsumerQueue和 MessageArrivingListener 中过滤时使用
 */
public class CommitLogDispatcherCalcBitMap implements CommitLogDispatcher {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    protected final BrokerConfig brokerConfig;
    protected final ConsumerFilterManager consumerFilterManager;

    public CommitLogDispatcherCalcBitMap(BrokerConfig brokerConfig, ConsumerFilterManager consumerFilterManager) {
        this.brokerConfig = brokerConfig;
        this.consumerFilterManager = consumerFilterManager;
    }

    @Override
    public void dispatch(DispatchRequest request) {
        if (!this.brokerConfig.isEnableCalcFilterBitMap()) {
            return;
        }

        try {
            //获取当前主题的过滤数据
            Collection<ConsumerFilterData> filterDatas = consumerFilterManager.get(request.getTopic());

            if (filterDatas == null || filterDatas.isEmpty()) {
                return;
            }

            Iterator<ConsumerFilterData> iterator = filterDatas.iterator();
            BitsArray filterBitMap = BitsArray.create(
                this.consumerFilterManager.getBloomFilter().getM()
            );

            long startTime = System.currentTimeMillis();
            while (iterator.hasNext()) {
                ConsumerFilterData filterData = iterator.next();

                if (filterData.getCompiledExpression() == null) {
                    log.error("[BUG] Consumer in filter manager has no compiled expression! {}", filterData);
                    continue;
                }

                if (filterData.getBloomFilterData() == null) {
                    log.error("[BUG] Consumer in filter manager has no bloom data! {}", filterData);
                    continue;
                }

                Object ret = null;
                try {
                    //消息属性评估的上下文
                    MessageEvaluationContext context = new MessageEvaluationContext(request.getPropertiesMap());
                    //进行评估
                    ret = filterData.getCompiledExpression().evaluate(context);
                } catch (Throwable e) {
                    log.error("Calc filter bit map error!commitLogOffset={}, consumer={}, {}", request.getCommitLogOffset(), filterData, e);
                }

                log.debug("Result of Calc bit map:ret={}, data={}, props={}, offset={}", ret, filterData, request.getPropertiesMap(), request.getCommitLogOffset());

                // eval true
                if (ret != null && ret instanceof Boolean && (Boolean) ret) {
                    //将当前主题的bloom过滤器计算结果设置到filterBitMap中
                    consumerFilterManager.getBloomFilter().hashTo(
                        filterData.getBloomFilterData(),//当前主题的过滤器数据
                        filterBitMap
                    );
                }
            }
            //设置消息的bitmap
            request.setBitMap(filterBitMap.bytes());

            long elapsedTime = UtilAll.computeElapsedTimeMilliseconds(startTime);
            // 1ms
            if (elapsedTime >= 1) {
                log.warn("Spend {} ms to calc bit map, consumerNum={}, topic={}", elapsedTime, filterDatas.size(), request.getTopic());
            }
        } catch (Throwable e) {
            log.error("Calc bit map error! topic={}, offset={}, queueId={}, {}", request.getTopic(), request.getCommitLogOffset(), request.getQueueId(), e);
        }
    }
}
