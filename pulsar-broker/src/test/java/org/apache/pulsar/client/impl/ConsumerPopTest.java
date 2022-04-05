/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import lombok.Cleanup;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.fail;

@Test(groups = "broker-impl")
public class ConsumerPopTest extends ProducerConsumerBase {

    private static final TransactionImpl transaction = mock(TransactionImpl.class);

    @BeforeClass
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        doReturn(1L).when(transaction).getTxnIdLeastBits();
        doReturn(1L).when(transaction).getTxnIdMostBits();
        doReturn(TransactionImpl.State.OPEN).when(transaction).getState();
        CompletableFuture<Void> completableFuture = CompletableFuture.completedFuture(null);
        doNothing().when(transaction).registerAckOp(any());
        doReturn(true).when(transaction).checkIfOpen(any());
        doReturn(completableFuture).when(transaction).registerAckedTopic(any(), any());

        Thread.sleep(1000 * 3);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPop() throws PulsarClientException, InterruptedException {
        String topic = "testAckResponse";
        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .enableBatching(false)
                .create();
        @Cleanup
        ConsumerImpl<Integer> consumer = (ConsumerImpl<Integer>) pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .consumeMode(ConsumeMode.Pop)
                .subscribe();

        /*
        MessageId messageId = producer.send(1);
        System.out.println("send message" + messageId);
        MessageId messageId2=producer.send(2);
        System.out.println("send message" + messageId2);
        */


        Message<Integer> message = consumer.pop();

        System.out.println(message.getMessageId());
        consumer.acknowledge(message);

        Message<Integer> message2 = consumer.pop();
        System.out.println(message2.getMessageId());
        consumer.acknowledge(message2);

        TimeUnit.SECONDS.sleep(10000);

    }
}
