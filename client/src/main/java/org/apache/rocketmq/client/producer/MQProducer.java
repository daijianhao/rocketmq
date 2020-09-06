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
package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Collection;
import java.util.List;

/**
 * producer发送消息支持3种方式，同步、异步和Oneway。
 * <p>
 * 同步发送：客户端提交消息到broker后会等待返回结果，相对来说是最常用的方式。
 * 异步发送：调用发送接口时会注册一个callback类，发送线程继续其它业务逻辑，producer在收到broker结果后回调。
 * 比较适合不想发送结果影响正常业务逻辑的情况。
 * Oneway：Producer提交消息后，无论broker是否正常接收消息都不关心。适合于追求高吞吐、能容忍消息丢失的场景，比如日志收集。
 */
public interface MQProducer extends MQAdmin {
    /**
     * 启动
     */
    void start() throws MQClientException;

    /**
     * 关闭
     */
    void shutdown();

    /**
     * 获取指定topic下可以发送消息的队列信息
     *
     * @param topic
     */
    List<MessageQueue> fetchPublishMessageQueues(final String topic) throws MQClientException;

    /**
     * Send message in synchronous mode. This method returns only when the sending procedure totally completes
     */
    SendResult send(final Message msg) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    /**
     * 带超时的同步发送,不设置超时，默认超时3000ms
     */
    SendResult send(final Message msg, final long timeout) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    /**
     * 异步发送，不设置超时，默认超时3000ms
     */
    void send(final Message msg, final SendCallback sendCallback) throws MQClientException,
            RemotingException, InterruptedException;

    /**
     * 带超时的异步发送
     */
    void send(final Message msg, final SendCallback sendCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException;

    /**
     * one-way方式
     */
    void sendOneway(final Message msg) throws MQClientException, RemotingException,
            InterruptedException;

    /**
     * 同步发送，指定队列
     *
     * @param msg 消息
     * @param mq  队列
     */
    SendResult send(final Message msg, final MessageQueue mq) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    /**
     * 同步发送，指定队列，有超时
     *
     * @param msg 消息
     * @param mq  队列
     */
    SendResult send(final Message msg, final MessageQueue mq, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 异步发送，指定队列
     */
    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException;

    /**
     * 异步发送，指定队列，有超时
     */
    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException;

    /**
     * one-way方式，指定队列
     */
    void sendOneway(final Message msg, final MessageQueue mq) throws MQClientException,
            RemotingException, InterruptedException;

    //和指定MessageQueue类似，只是使用MessageQueueSelector指定队列

    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg,
                    final long timeout) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    void send(final Message msg, final MessageQueueSelector selector, final Object arg,
              final SendCallback sendCallback) throws MQClientException, RemotingException,
            InterruptedException;

    void send(final Message msg, final MessageQueueSelector selector, final Object arg,
              final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException,
            InterruptedException;

    void sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg)
            throws MQClientException, RemotingException, InterruptedException;

    /**
     * 发送事务消息
     */
    TransactionSendResult sendMessageInTransaction(final Message msg,
                                                   final LocalTransactionExecuter tranExecuter, final Object arg) throws MQClientException;

    TransactionSendResult sendMessageInTransaction(final Message msg,
                                                   final Object arg) throws MQClientException;

    //for batch 批量发送

    SendResult send(final Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    SendResult send(final Collection<Message> msgs, final long timeout) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Collection<Message> msgs, final MessageQueue mq) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Collection<Message> msgs, final MessageQueue mq, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    //for rpc，相当于rpc调用，可看作是producer直接调用consumer，在收到consumer回复消息后才返回或执行回调
    Message request(final Message msg, final long timeout) throws RequestTimeoutException, MQClientException,
            RemotingException, MQBrokerException, InterruptedException;

    void request(final Message msg, final RequestCallback requestCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException, MQBrokerException;

    Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
                    final long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException,
            InterruptedException;

    void request(final Message msg, final MessageQueueSelector selector, final Object arg,
                 final RequestCallback requestCallback,
                 final long timeout) throws MQClientException, RemotingException,
            InterruptedException, MQBrokerException;

    Message request(final Message msg, final MessageQueue mq, final long timeout)
            throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
}
