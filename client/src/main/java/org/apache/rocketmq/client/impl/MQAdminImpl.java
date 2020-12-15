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
package org.apache.rocketmq.client.impl;

import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryMessageResponseHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * MQ管理实现类，负责 创建topic、获取队列信息、查询消息等
 * <p>
 * Consumer与Producer的客户端也是委托此类实现以上功能的
 */
public class MQAdminImpl {

    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private long timeoutMillis = 6000;

    public MQAdminImpl(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    /**
     * 创建topic
     * @param key
     * @param newTopic topic名称
     * @param queueNum topic对应的queue数量
     * @param topicSysFlag topic的标记位设置，没有特殊要求就填0就可以了。可选值在TopicSysFlag中定义
     * @throws MQClientException
     */
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        try {
            //检查是否为空
            Validators.checkTopic(newTopic);
            //检查是否是系统自带的topic，即不能与系统自带的重名
            Validators.isSystemTopic(newTopic);
            //从Nameserver获取路由信息，根据提供的key代表的topic去获取broker的路由，如果想在所有broker创建，
            // 一般使用DefaultTopic，因为这个topic是在所有broker上都存在的。
            TopicRouteData topicRouteData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(key, timeoutMillis);
            //获取到指定的broker信息
            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
            if (brokerDataList != null && !brokerDataList.isEmpty()) {
                //先按broker名称排序
                Collections.sort(brokerDataList);

                //至少成功创建一次
                boolean createOKAtLeastOnce = false;
                //异常
                MQClientException exception = null;

                StringBuilder orderTopicString = new StringBuilder();

                for (BrokerData brokerData : brokerDataList) {
                    //取得这个broker的master地址
                    String addr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (addr != null) {//地址不等于null才创建
                        //新建一个TopicConfig
                        TopicConfig topicConfig = new TopicConfig(newTopic);
                        topicConfig.setReadQueueNums(queueNum);
                        topicConfig.setWriteQueueNums(queueNum);
                        topicConfig.setTopicSysFlag(topicSysFlag);

                        boolean createOK = false;
                        for (int i = 0; i < 5; i++) {
                            try {
                                //在指定addr上循环5次，
                                this.mQClientFactory.getMQClientAPIImpl().createTopic(addr, key, topicConfig, timeoutMillis);
                                createOK = true;
                                createOKAtLeastOnce = true;
                                break;
                            } catch (Exception e) {
                                if (4 == i) {
                                    exception = new MQClientException("create topic to broker exception", e);
                                }
                            }
                        }

                        if (createOK) {
                            //如果创建成功
                            orderTopicString.append(brokerData.getBrokerName());
                            orderTopicString.append(":");
                            orderTopicString.append(queueNum);
                            orderTopicString.append(";");
                        }
                    }
                }

                if (exception != null && !createOKAtLeastOnce) {
                    //一次未创建成功 抛出异常
                    throw exception;
                }
            } else {
                //如果broker不存在，则抛出异常
                throw new MQClientException("Not found broker, maybe key is wrong", null);
            }
        } catch (Exception e) {
            //创建失败
            throw new MQClientException("create new topic failed", e);
        }
    }

    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        try {
            TopicRouteData topicRouteData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, timeoutMillis);
            if (topicRouteData != null) {
                TopicPublishInfo topicPublishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);
                if (topicPublishInfo != null && topicPublishInfo.ok()) {
                    return parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
                }
            }
        } catch (Exception e) {
            throw new MQClientException("Can not find Message Queue for this topic, " + topic, e);
        }

        throw new MQClientException("Unknow why, Can not find Message Queue for this topic, " + topic, null);
    }

    public List<MessageQueue> parsePublishMessageQueues(List<MessageQueue> messageQueueList) {
        List<MessageQueue> resultQueues = new ArrayList<MessageQueue>();
        for (MessageQueue queue : messageQueueList) {
            String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), this.mQClientFactory.getClientConfig().getNamespace());
            resultQueues.add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
        }

        return resultQueues;
    }

    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        try {
            TopicRouteData topicRouteData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, timeoutMillis);
            if (topicRouteData != null) {
                Set<MessageQueue> mqList = MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                if (!mqList.isEmpty()) {
                    return mqList;
                } else {
                    throw new MQClientException("Can not find Message Queue for this topic, " + topic + " Namesrv return empty", null);
                }
            }
        } catch (Exception e) {
            throw new MQClientException(
                    "Can not find Message Queue for this topic, " + topic + FAQUrl.suggestTodo(FAQUrl.MQLIST_NOT_EXIST),
                    e);
        }

        throw new MQClientException("Unknow why, Can not find Message Queue for this topic, " + topic, null);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        if (brokerAddr != null) {
            try {
                return this.mQClientFactory.getMQClientAPIImpl().searchOffset(brokerAddr, mq.getTopic(), mq.getQueueId(), timestamp,
                        timeoutMillis);
            } catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        if (brokerAddr != null) {
            try {
                return this.mQClientFactory.getMQClientAPIImpl().getMaxOffset(brokerAddr, mq.getTopic(), mq.getQueueId(), timeoutMillis);
            } catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        if (brokerAddr != null) {
            try {
                return this.mQClientFactory.getMQClientAPIImpl().getMinOffset(brokerAddr, mq.getTopic(), mq.getQueueId(), timeoutMillis);
            } catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        if (brokerAddr != null) {
            try {
                return this.mQClientFactory.getMQClientAPIImpl().getEarliestMsgStoretime(brokerAddr, mq.getTopic(), mq.getQueueId(),
                        timeoutMillis);
            } catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public MessageExt viewMessage(
            String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

        MessageId messageId = null;
        try {
            messageId = MessageDecoder.decodeMessageId(msgId);
        } catch (Exception e) {
            throw new MQClientException(ResponseCode.NO_MESSAGE, "query message by id finished, but no message.");
        }
        return this.mQClientFactory.getMQClientAPIImpl().viewMessage(RemotingUtil.socketAddress2String(messageId.getAddress()),
                messageId.getOffset(), timeoutMillis);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin,
                                    long end) throws MQClientException,
            InterruptedException {
        return queryMessage(topic, key, maxNum, begin, end, false);
    }

    public MessageExt queryMessageByUniqKey(String topic,
                                            String uniqKey) throws InterruptedException, MQClientException {

        QueryResult qr = this.queryMessage(topic, uniqKey, 32,
                MessageClientIDSetter.getNearlyTimeFromID(uniqKey).getTime() - 1000, Long.MAX_VALUE, true);
        if (qr != null && qr.getMessageList() != null && qr.getMessageList().size() > 0) {
            return qr.getMessageList().get(0);
        } else {
            return null;
        }
    }

    /**
     * 查询消息
     * @param topic topic
     * @param key todo MessageKey
     * @param maxNum 最多返回的消息数，因为key是由用户设置的，并不保证唯一，所以可能取到多个消息；同时index中只存储了hash，所以hash相同的消息也会取出来
     * @param begin 开始时间
     * @param end 结束时间
     * @param isUniqKey 是否唯一
     * @return
     * @throws MQClientException
     * @throws InterruptedException
     */
    protected QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end,
                                       boolean isUniqKey) throws MQClientException,
            InterruptedException {
        //获取topic
        TopicRouteData topicRouteData = this.mQClientFactory.getAnExistTopicRouteData(topic);
        if (null == topicRouteData) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            topicRouteData = this.mQClientFactory.getAnExistTopicRouteData(topic);
        }

        if (topicRouteData != null) {
            List<String> brokerAddrs = new LinkedList<String>();
            for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                //获取broker地址
                String addr = brokerData.selectBrokerAddr();
                if (addr != null) {
                    brokerAddrs.add(addr);
                }
            }

            if (!brokerAddrs.isEmpty()) {
                final CountDownLatch countDownLatch = new CountDownLatch(brokerAddrs.size());
                final List<QueryResult> queryResultList = new LinkedList<QueryResult>();
                final ReadWriteLock lock = new ReentrantReadWriteLock(false);

                for (String addr : brokerAddrs) {
                    try {
                        //构建查询请求头
                        QueryMessageRequestHeader requestHeader = new QueryMessageRequestHeader();
                        requestHeader.setTopic(topic);
                        requestHeader.setKey(key);
                        requestHeader.setMaxNum(maxNum);
                        requestHeader.setBeginTimestamp(begin);
                        requestHeader.setEndTimestamp(end);
                        //发送查询请求
                        this.mQClientFactory.getMQClientAPIImpl().queryMessage(addr, requestHeader, timeoutMillis * 3,
                                new InvokeCallback() {
                                    @Override
                                    public void operationComplete(ResponseFuture responseFuture) {
                                        try {
                                            RemotingCommand response = responseFuture.getResponseCommand();
                                            if (response != null) {
                                                switch (response.getCode()) {
                                                    case ResponseCode.SUCCESS: {
                                                        QueryMessageResponseHeader responseHeader = null;
                                                        try {
                                                            responseHeader =
                                                                    (QueryMessageResponseHeader) response
                                                                            .decodeCommandCustomHeader(QueryMessageResponseHeader.class);
                                                        } catch (RemotingCommandException e) {
                                                            log.error("decodeCommandCustomHeader exception", e);
                                                            return;
                                                        }

                                                        List<MessageExt> wrappers =
                                                                MessageDecoder.decodes(ByteBuffer.wrap(response.getBody()), true);

                                                        QueryResult qr = new QueryResult(responseHeader.getIndexLastUpdateTimestamp(), wrappers);
                                                        try {
                                                            //封装查询结果
                                                            lock.writeLock().lock();
                                                            queryResultList.add(qr);
                                                        } finally {
                                                            lock.writeLock().unlock();
                                                        }
                                                        break;
                                                    }
                                                    default:
                                                        log.warn("getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
                                                        break;
                                                }
                                            } else {
                                                log.warn("getResponseCommand return null");
                                            }
                                        } finally {
                                            countDownLatch.countDown();
                                        }
                                    }
                                }, isUniqKey);
                    } catch (Exception e) {
                        log.warn("queryMessage exception", e);
                    }

                }

                boolean ok = countDownLatch.await(timeoutMillis * 4, TimeUnit.MILLISECONDS);
                if (!ok) {
                    log.warn("queryMessage, maybe some broker failed");
                }

                long indexLastUpdateTimestamp = 0;
                List<MessageExt> messageList = new LinkedList<MessageExt>();
                for (QueryResult qr : queryResultList) {
                    if (qr.getIndexLastUpdateTimestamp() > indexLastUpdateTimestamp) {
                        indexLastUpdateTimestamp = qr.getIndexLastUpdateTimestamp();
                    }

                    for (MessageExt msgExt : qr.getMessageList()) {
                        if (isUniqKey) {
                            if (msgExt.getMsgId().equals(key)) {

                                if (messageList.size() > 0) {

                                    if (messageList.get(0).getStoreTimestamp() > msgExt.getStoreTimestamp()) {

                                        messageList.clear();
                                        messageList.add(msgExt);
                                    }

                                } else {

                                    messageList.add(msgExt);
                                }
                            } else {
                                log.warn("queryMessage by uniqKey, find message key not matched, maybe hash duplicate {}", msgExt.toString());
                            }
                        } else {
                            String keys = msgExt.getKeys();
                            if (keys != null) {
                                boolean matched = false;
                                String[] keyArray = keys.split(MessageConst.KEY_SEPARATOR);
                                if (keyArray != null) {
                                    for (String k : keyArray) {
                                        if (key.equals(k)) {
                                            matched = true;
                                            break;
                                        }
                                    }
                                }

                                if (matched) {
                                    messageList.add(msgExt);
                                } else {
                                    log.warn("queryMessage, find message key not matched, maybe hash duplicate {}", msgExt.toString());
                                }
                            }
                        }
                    }
                }

                //If namespace not null , reset Topic without namespace.
                for (MessageExt messageExt : messageList) {
                    if (null != this.mQClientFactory.getClientConfig().getNamespace()) {
                        messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.mQClientFactory.getClientConfig().getNamespace()));
                    }
                }

                if (!messageList.isEmpty()) {
                    //返回查询结果
                    return new QueryResult(indexLastUpdateTimestamp, messageList);
                } else {
                    throw new MQClientException(ResponseCode.NO_MESSAGE, "query message by key finished, but no message.");
                }
            }
        }

        throw new MQClientException(ResponseCode.TOPIC_NOT_EXIST, "The topic[" + topic + "] not matched route info");
    }
}
